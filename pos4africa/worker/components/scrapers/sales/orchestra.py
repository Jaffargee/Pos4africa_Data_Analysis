"""
Parallel runner: N workers, each with its OWN logged-in POS session, pull sale codes from a queue
and rewrite the comment on each one.

    python run_comment_fix.py --csv worklist.csv                      # DRY RUN, 5 workers
    python run_comment_fix.py --csv worklist.csv --limit 3 --commit   # pilot: really edit 3 sales
    python run_comment_fix.py --csv worklist.csv --workers 6 --commit # full run

CSV columns: pos_sale_id, old_comment
Resumable: finished sales are appended to results_commit.jsonl and skipped on the next run.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import httpx

from pos4africa.worker.components.scrapers.sales.edit_sale_comment import CommitUnverifiedError, EditSaleComment
from pos4africa.infra.pos_connector import PosConnector
from pos4africa.worker.components.scrapers.sales.edit_sale import NotInEditModeError, SaleMismatchError


async def make_connector() -> PosConnector:
      """
      ADAPT THIS to however you create a logged-in PosConnector today.
      Each call must return a NEW, independently logged-in session.
      If POS logs out older sessions when the same user logs in again, give each worker its own POS user.
      """
      connector = PosConnector()
      await connector.init()  # <-- replace with your real login call
      return connector


@dataclass
class Job:
      sale_code: int
      old_comment: str | None
      total: str | None = None


class Breaker:
      def __init__(self, limit: int) -> None:
            self.limit, self.streak, self.tripped = limit, 0, asyncio.Event()

      def ok(self) -> None:
            self.streak = 0

      def fail(self) -> None:
            self.streak += 1
            if self.streak >= self.limit:
                  self.tripped.set()


def load_jobs(path: Path, done: set[int], limit: int | None) -> list[Job]:
      jobs: list[Job] = []
      with path.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                  code = int(row["pos_sale_id"])
                  if code in done:
                        continue
                  jobs.append(Job(code, row.get("old_comment") or None, row.get("invoice_total") or None))
      return jobs[:limit] if limit else jobs


def load_done(path: Path) -> set[int]:
      if not path.exists():
            return set()
      done = set()
      for line in path.read_text(encoding="utf-8").splitlines():
            rec = json.loads(line)
            if rec["status"] in ("ok", "already_done"):
                  done.add(rec["sale"])
      return done


async def process(comp: EditSaleComment, job: Job, args) -> tuple[str, dict]:
      last: Exception | None = None
      for attempt in range(1, args.attempts + 1):
            try:
                  res = await comp.rewrite(
                        job.sale_code,
                        args.new_comment,
                        expected_old_comment=None if args.no_old_guard else job.old_comment,
                        expected_total=None if args.no_total_guard else job.total,
                        id_kind=args.id_kind,
                        commit=args.commit,
                        verify=not args.no_verify,
                  )
                  status = "already_done" if res.get("note") == "already_done" else ("ok" if args.commit else "dry_ok")
                  return status, res
            except SaleMismatchError as exc:        # wrong sale / unexpected state: never retry
                  return "skipped_mismatch", {"error": str(exc)}
            except CommitUnverifiedError as exc:    # may already be written: never retry blindly
                  return "unverified", {"error": str(exc)}
            except (NotInEditModeError, httpx.HTTPError) as exc:
                  last = exc
                  await asyncio.sleep(min(30, 2 ** attempt) + random.random())
      return "failed", {"error": repr(last)}


async def worker(n: int, queue: asyncio.Queue, args, out, out_lock, breaker: Breaker, stats: Counter) -> None:
      try:
            comp = EditSaleComment(await make_connector())
      except Exception as exc:
            print(f"[w{n}] login failed: {exc!r}")
            breaker.fail()
            return
      while not breaker.tripped.is_set():
            try:
                  job: Job = queue.get_nowait()
            except asyncio.QueueEmpty:
                  return
            status, detail = await process(comp, job, args)
            (breaker.ok if status in ("ok", "dry_ok", "already_done", "skipped_mismatch") else breaker.fail)()
            stats[status] += 1
            async with out_lock:
                  out.write(json.dumps({"sale": job.sale_code, "status": status, "worker": n, **detail}) + "\n")
                  out.flush()
            total = sum(stats.values())
            if total % 25 == 0 or status not in ("ok", "dry_ok"):
                  print(f"[w{n}] sale {job.sale_code}: {status}  (done {total})")
            await asyncio.sleep(args.delay + random.random() * args.delay)


async def main() -> None:
      ap = argparse.ArgumentParser()
      ap.add_argument("--csv", type=Path, required=True)
      ap.add_argument("--new-comment", default="CUSTOMER")
      ap.add_argument("--workers", type=int, default=5)
      ap.add_argument("--commit", action="store_true", help="really write to POS (default is a dry run)")
      ap.add_argument("--limit", type=int)
      ap.add_argument("--delay", type=float, default=0.5, help="seconds between sales per worker")
      ap.add_argument("--attempts", type=int, default=3)
      ap.add_argument("--max-consecutive-failures", type=int, default=8)
      ap.add_argument("--no-verify", action="store_true")
      ap.add_argument("--no-old-guard", action="store_true", help="skip 'POS comment must equal old_comment'")
      ap.add_argument("--no-total-guard", action="store_true", help="skip 'POS total must equal invoice_total'")
      ap.add_argument("--id-kind", choices=["code", "internal"], default="code",
                        help="code = number in 'Editing Sale S7441'; internal = id in the URL / delete link (7470)")
      args = ap.parse_args()

      out_path = Path("results_commit.jsonl" if args.commit else "results_dry.jsonl")
      done = load_done(out_path) if args.commit else set()
      jobs = load_jobs(args.
      csv, done, args.limit)
      print(f"{'COMMIT' if args.commit else 'DRY RUN'}: {len(jobs)} sales, {args.workers} workers "
            f"({len(done)} already done)")

      queue: asyncio.Queue = asyncio.Queue()
      for j in jobs:
            queue.put_nowait(j)

      breaker, stats, out_lock = Breaker(args.max_consecutive_failures), Counter(), asyncio.Lock()
      started = time.monotonic()
      with out_path.open("a", encoding="utf-8") as out:
            await asyncio.gather(*(
                  worker(i + 1, queue, args, out, out_lock, breaker, stats)
                  for i in range(min(args.workers, max(1, len(jobs))))
            ))
      print(f"\nfinished in {time.monotonic() - started:.0f}s: {dict(stats)}; "
            f"{queue.qsize()} left in queue" + ("  <-- STOPPED by circuit breaker" if breaker.tripped.is_set() else ""))


if __name__ == "__main__":
      asyncio.run(main())