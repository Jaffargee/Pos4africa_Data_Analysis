import json
import logging
import asyncio
import dataclasses
import pandas as pd
from asyncio import Queue, Event, log
from typing import Counter, Iterable

from pos4africa.worker.components.base import BaseComponent
from pos4africa.worker.pipeline.base import PipelineContext
from pos4africa.infra.repository import Repository
from pos4africa.infra.pos_connector import PosConnector
from pos4africa.worker.components.syncer import Syncer, MatchedResults
from pos4africa.worker.components.scanner import Scanner
from pos4africa.worker.components.scrapers.sales.edit_sale import EditSale, SaleMismatchError, CommitUnverifiedError, NotInEditModeError

log = logging.getLogger("reassign")

@dataclasses.dataclass
class Job:
      comment: str
      pos_sale_id: int
      pos_customer_id: int
      old_pos_customer_id: int
      total: int

@dataclasses.dataclass(slots=True)
class Result:
      sale_id: int
      status: str
      details: str = ""

class Breaker:
      def __init__(self, limit: int) -> None:
            self.limit, self._streak, self.tripped = limit, 0, Event()
            self.reason: str | None = None

      def ok(self) -> None:
            self._streak = 0

      def fail(self, reason: str) -> None:
            self._streak += 1
            if self._streak >= self.limit:
                  self.trip(f"{self._consecutive} consecutive failures, last: {reason}")

      def trip(self, reason: str) -> None:
            if not self.tripped.is_set():
                  self.reason = reason or "Unknown reason"
                  self.tripped.set()
                  print(f"Breaker tripped due to: {reason}")


class Worker:
      def __init__(
                  self,
                  queue: Queue, 
                  breaker: Breaker, 
                  results: list[Result],
                  *,
                  commit: bool = False
            ):

            if not isinstance(queue, Queue):
                  raise TypeError("Expected 'queue' to be an instance of 'asyncio.Queue'.")

            if not isinstance(breaker, Breaker):
                  raise TypeError("Expected 'breaker' to be an instance of 'Breaker'.")

            self._editor: EditSale | None = None
            self._queue: Queue = queue
            self._breaker: Breaker = breaker
            self._results: list[Result] = results
            self._commit: bool = commit

      async def run(self) -> None:
            async with PosConnector() as connector:
                  self._editor = EditSale(connector=connector)
                  while not self._breaker.tripped.is_set():
                        try:
                              job: Job = self._queue.get_nowait()
                        except asyncio.QueueEmpty:
                              return

                        try:
                              self._results.append(await self._process(job=job))
                        finally:
                              self._queue.task_done()

      async def _process(self, job: Job) -> Result:

            if not isinstance(job, Job):
                  raise TypeError("Expected 'job' to be type of 'Job'.")

            try:
                  res = await  self._editor.reassign(
                        sale_id=job.pos_sale_id,
                        new_person_id=job.pos_customer_id,
                        expected_total=job.total,
                        expected_current_person_id=job.old_pos_customer_id,
                        commit=self._commit
                  )
            except SaleMismatchError as exc:
                  self._breaker.fail(reason=str(exc))
                  log.warning("sale %s mismatch: %s", job.pos_sale_id, exc)
                  return Result(sale_id=job.pos_sale_id, status="mismatch", details=str(exc))
            except CommitUnverifiedError as exc:
                  self._breaker.fail(reason=str(exc))
                  log.warning("sale %s unverified: %s", job.pos_sale_id, exc)
                  return Result(sale_id=job.pos_sale_id, status="unverified", details=str(exc))
            except NotInEditModeError as exc:
                  log.warning("sale %s not in edit mode: %s", job.pos_sale_id, exc)
                  self._breaker.trip(reason=str(exc))
                  return Result(sale_id=job.pos_sale_id, status="not_in_edit_mode", details=str(exc))
            except Exception as exc:
                  log.error("sale %s error: %s", job.pos_sale_id, exc)
                  return Result(sale_id=job.pos_sale_id, status="error", details=str(exc))

            self._breaker.ok()
            if not res["changed"]: status = "already_assigned"
            elif res["committed"]: status = "committed"
            else: status = "dry_run_ok"

            return Result(sale_id=job.pos_sale_id, status=status)

class Orchestrator:

      def __init__(self, n_workers: int = 5, commit: bool = False, breaker_limit: int = 5):
            self._queue = Queue()
            self._breaker = Breaker(limit=breaker_limit)
            self._results: list[Result] = []
            self._n_workers = n_workers
            self._commit = commit

      def _push_jobs_in_queue(self, job: Job) -> None:
            self._queue.put_nowait(job)

      def push_jobs(self, jobs: list[Job]) -> None:
            for job in jobs:
                  self._push_jobs_in_queue(job)

      async def _start(self):
            workers = [Worker(queue=self._queue, breaker=self._breaker, results=self._results, commit=self._commit) for _ in range(self._n_workers)]
            await asyncio.gather(*(worker.run() for worker in workers))

            while not self._queue.empty():
                  job = self._queue.get_nowait()
                  self._results.append(Result(sale_id=job.pos_sale_id, status="skipped", details="Queue not empty after workers finished."))
                  self._queue.task_done()

            return self._results

      async def run(self, jobs: Iterable[Job], report_path: str | None = "reassign_results.jsonl") -> list[Result]:
            self.push_jobs(jobs)
            results = await self._start()
      
            counts = Counter(r.status for r in results)
            log.info("done (commit=%s): %s", self._commit, dict(counts))
            if self._breaker.tripped.is_set():
                  log.error("stopped early: %s", self._breaker.reason)
      
            if report_path:
                  with open(report_path, "w", encoding="utf-8") as f:
                        for r in results:
                              f.write(json.dumps(dataclasses.asdict(r)) + "\n")
            return results

def load_jobs(df: pd.DataFrame, matched_data: list[MatchedResults]) -> list[Job]:

      if not isinstance(df, pd.DataFrame):
            raise TypeError("Expected 'df' to be a pandas DataFrame.")
      if not isinstance(matched_data, list):
            raise TypeError("Expected matched_data arguments to be list of 'MatchedResults'.")

      if df.empty or not matched_data:
            return []

      required_cols = {"pos_sale_id", "comment", "pos_customer_id", "invoice_total"}
      missing_cols = required_cols - set(df.columns)

      if missing_cols:
            raise ValueError(f"DataFrame is missing required columns: {missing_cols}")

      jobs: list[Job] = []

      sales_by_comment = {
            str(comment): group.to_dict(orient="records")
            for comment, group in df.groupby("comment")
      }

      for data in matched_data:

            if not data.customer_found: continue

            ctm_sales = sales_by_comment.get(data.comment, [])
            for sale in ctm_sales:
                  jobs.append(
                        Job(
                              comment=data.comment, 
                              pos_sale_id=sale["pos_sale_id"],
                              pos_customer_id=data.pos_customer_id,
                              old_pos_customer_id=sale["pos_customer_id"],
                              total=sale["invoice_total"]
                        )
                  )
      return jobs

class S2CMapper(BaseComponent):

      async def run(self, ctx: PipelineContext):
            repo = Repository()
            syncer = Syncer()
            scanner = Scanner()
            orchestrator = Orchestrator(n_workers=10, commit=False, breaker_limit=5)

            sales = repo.get_sales()

            df = scanner.init(sales)
            comments = scanner.get_comment_frequencies(df)
            customers = syncer.load_json_object(syncer.customers_local_db_file_path)

            results = syncer.match_comments_to_customers(comments, customers)

            jobs: list[Job] = load_jobs(df, results) 

            orchestrator_results = await orchestrator.run(jobs, report_path="reassign_results.jsonl")

            for result in orchestrator_results:
                  log.info("sale %s: %s", result.sale_id, result.status)
