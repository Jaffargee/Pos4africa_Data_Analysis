import asyncio
import httpx
import logging
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# WORKER NODE
# ─────────────────────────────────────────

@dataclass
class WorkerResult:
    worker_name: str
    start_id: int
    end_id: int
    success: int
    failed: list[int]
    duration: float


class Worker:
    def __init__(self, name: str, start_id: int, end_id: int):
        self.name = name
        self.start_id = start_id
        self.end_id = end_id
        self.client = httpx.AsyncClient(timeout=30)
        self.success = 0
        self.failed = []

    async def login(self):
        await self.client.post(
            f"{POS_BASE}/index.php/login",
            data={"username": POS_USER, "password": POS_PASS}
        )
        logger.info(f"[{self.name}] Logged in")

    async def scrape_one(self, sale_id: int) -> dict | None:
        try:
            resp = await self.client.get(
                f"{POS_BASE}/index.php/sales/receipt/{sale_id}",
                timeout=15
            )
            if resp.status_code == 404:
                return None
            return parse_receipt(resp.text, sale_id)
        except Exception as e:
            logger.warning(f"[{self.name}] Sale {sale_id} failed: {e}")
            return None

    async def work(self, batch_size: int = 10, delay: float = 0.5) -> WorkerResult:
        await self.login()
        
        start_time = asyncio.get_event_loop().time()
        total = self.end_id - self.start_id + 1

        logger.info(
            f"[{self.name}] Starting — "
            f"IDs {self.start_id} → {self.end_id} "
            f"({total} sales)"
        )

        for batch_start in range(self.start_id, self.end_id + 1, batch_size):
            batch_end = min(batch_start + batch_size, self.end_id + 1)
            batch_ids = range(batch_start, batch_end)

            # Scrape batch concurrently
            tasks = [self.scrape_one(sid) for sid in batch_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Save results
            for sale_id, result in zip(batch_ids, results):
                if isinstance(result, Exception) or result is None:
                    self.failed.append(sale_id)
                else:
                    await save_sale(result)
                    self.success += 1

            logger.info(
                f"[{self.name}] "
                f"Batch {batch_start}-{batch_end} done | "
                f"Success: {self.success} | "
                f"Failed: {len(self.failed)}"
            )

            await asyncio.sleep(delay)

        # Retry failed
        if self.failed:
            logger.info(f"[{self.name}] Retrying {len(self.failed)} failed...")
            retry_list = self.failed.copy()
            self.failed.clear()

            for sale_id in retry_list:
                result = await self.scrape_one(sale_id)
                if result:
                    await save_sale(result)
                    self.success += 1
                else:
                    self.failed.append(sale_id)
                await asyncio.sleep(1)

        duration = asyncio.get_event_loop().time() - start_time

        logger.info(
            f"[{self.name}] Done in {duration:.1f}s | "
            f"Success: {self.success} | "
            f"Failed: {len(self.failed)}"
        )

        await self.client.aclose()

        return WorkerResult(
            worker_name=self.name,
            start_id=self.start_id,
            end_id=self.end_id,
            success=self.success,
            failed=self.failed,
            duration=duration
        )


# ─────────────────────────────────────────
# WORKER MANAGER
# ─────────────────────────────────────────

class WorkerManager:
    def __init__(self, total_sales: int, num_workers: int = 8):
        self.total_sales = total_sales
        self.num_workers = num_workers
        self.workers = self._create_workers()

    def _create_workers(self) -> list[Worker]:
        chunk = self.total_sales // self.num_workers
        workers = []

        for i in range(self.num_workers):
            start = (i * chunk) + 1
            # Last worker takes remainder
            end = self.total_sales if i == self.num_workers - 1 else (i + 1) * chunk

            workers.append(Worker(
                name=f"worker-{i + 1}",
                start_id=start,
                end_id=end
            ))

        return workers

    def preview(self):
        print("\n── Worker Allocation ──────────────────")
        for w in self.workers:
            count = w.end_id - w.start_id + 1
            print(f"  {w.name:<12} IDs {w.start_id:>6} → {w.end_id:>6}  ({count} sales)")
        print("───────────────────────────────────────\n")

    async def run(self) -> list[WorkerResult]:
        self.preview()

        # All workers run concurrently
        tasks = [w.work() for w in self.workers]
        results = await asyncio.gather(*tasks)

        # Final summary
        total_success = sum(r.success for r in results)
        total_failed  = sum(len(r.failed) for r in results)
        all_failed    = [sid for r in results for sid in r.failed]
        max_duration  = max(r.duration for r in results)

        print("\n── Final Summary ──────────────────────")
        for r in results:
            print(
                f"  {r.worker_name:<12} "
                f"✓ {r.success:<6} "
                f"✗ {len(r.failed):<6} "
                f"⏱ {r.duration:.1f}s"
            )
        print(f"───────────────────────────────────────")
        print(f"  Total Success : {total_success}")
        print(f"  Total Failed  : {total_failed}")
        print(f"  Total Time    : {max_duration:.1f}s")
        if all_failed:
            print(f"  Failed IDs    : {all_failed}")
        print("───────────────────────────────────────\n")

        return results


# ─────────────────────────────────────────
# RUN
# ─────────────────────────────────────────

async def main():
    manager = WorkerManager(total_sales=15601, num_workers=8)
    results = await manager.run()

asyncio.run(main())
# ```

# ---

# ## What You Get
# ```
# ── Worker Allocation ──────────────────
#   worker-1     IDs      1 →   1950  (1950 sales)
#   worker-2     IDs   1951 →   3900  (1950 sales)
#   worker-3     IDs   3901 →   5850  (1950 sales)
#   worker-4     IDs   5851 →   7800  (1950 sales)
#   worker-5     IDs   7801 →   9750  (1950 sales)
#   worker-6     IDs   9751 →  11700  (1950 sales)
#   worker-7     IDs  11701 →  13650  (1950 sales)
#   worker-8     IDs  13651 →  15601  (1951 sales)
# ───────────────────────────────────────

# ── Final Summary ──────────────────────
#   worker-1     ✓ 1948   ✗ 2      ⏱ 98.3s
#   worker-2     ✓ 1950   ✗ 0      ⏱ 102.1s
#   ...
# ───────────────────────────────────────
#   Total Success : 15598
#   Total Failed  : 3
#   Total Time    : 102.1s        ← ~1.7 minutes