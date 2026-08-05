from __future__ import annotations

"""

This module contains the auto_sync function, which is responsible for automatically synchronizing data between different systems or databases. The function ensures that the data remains consistent and up-to-date across all platforms, reducing the risk of discrepancies and improving overall data integrity.

How the auto_sync function works:
1. It checks for the availability of the TODAY_REPORT URL to fetch the latest data.
2. If the TODAY_REPORT URL is not available, it falls back to the ALL_REPORT URL to fetch the data.
3. The function downloads the Excel file and saves it to a specified path.
4. It then initiates the data transfer process from the downloaded Excel file to the Supabase database.
5. The synchronization process is scheduled to run every day at 6 PM, ensuring that the data is updated regularly.

TODAY_REPORT_URL = 'https://fahadtahir.pos4africa.com/index.php/reports/generate/detailed_sales?report_type=simple&report_date_range_simple=TODAY&sale_type=all&with_time=1&excel_export=0&export_excel=1'
ALL_REPORT_URL = 'https://fahadtahir.pos4africa.com/index.php/reports/generate/detailed_sales?report_type=simple&report_date_range_simple=ALL_TIME&sale_type=all&with_time=1&excel_export=0&export_excel=1'

XLSX_FILE_PATH = os.getcwd() + '/Excels/DSR.xlsx'

"""

"""pos4africa.auto_sync

Provides an AutoSync helper that downloads the latest Excel report and writes it
into the repository-owned Excel source path.

The module resolves relative Excel paths against the project root, supports
`pos4africa.config.settings.excel_source_path`, and returns the final path that
was written. This makes path handling explicit and avoids hardcoded absolute
paths in the codebase.
"""


from pathlib import Path
import asyncio

from pos4africa.worker.components.network.connector import PosConnector
from pos4africa.config.settings import settings
from pos4africa.manager.host import HostManager
from pos4africa.shared.utils.logger import configure_logging

import logging
import sys

ALL_REPORT_URL = (
      'https://fahadtahir.pos4africa.com/index.php/reports/generate/detailed_sales'
      '?report_type=simple&report_date_range_simple=ALL_TIME&sale_type=all&with_time=1&excel_export=0&export_excel=1'
)
TODAY_REPORT_URL = (
      'https://fahadtahir.pos4africa.com/index.php/reports/generate/detailed_sales'
      '?report_type=simple&report_date_range_simple=TODAY&sale_type=all&with_time=1&excel_export=0&export_excel=1'
)

CUSTOMERS_REPORT_URL = (
      'https://fahadtahir.pos4africa.com/index.php/customers/excel_export'
)

ITEMS_REPORT_URL = (
      'https://fahadtahir.pos4africa.com/index.php/items/excel_export/'
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_EXCEL_PATH = PROJECT_ROOT / "Excels" / "DSR.xlsx"


class FileSystem:

      def __init__(self):
            self.project_root = PROJECT_ROOT

      def resolve_excel_path(self, source: str | Path) -> Path:
            path = Path(source).expanduser()
            if not path.is_absolute():
                  self.ensure_excel_directory(path)
                  path = self.project_root / path
            return path.resolve()

      def ensure_excel_directory(self, path: Path) -> None:
            if not path.parent.exists():
                  path.parent.mkdir(parents=True, exist_ok=True)

      def ensure_excel_file(self, path: Path) -> None:
            if not path.exists():
                  self.ensure_excel_directory(path)
                  path.touch()


class DownloadManager:
      
      async def download_excel(self, connector: PosConnector, url: str, destination: Path) -> None:
            try:
                  response = await connector.session.get(url)
                  if len(response.content) == 0:
                        raise ValueError(f"Received empty response from {url}")
                  # Check the byte and ensure it's a valid Excel file (basic check)
                  if not response.content.startswith(b'PK'):
                        raise ValueError(f"Downloaded file from {url} does not appear to be a valid Excel file.")

                  self.save_excel(response.content, destination)
                  logging.info(f"Successfully downloaded and saved Excel file to {destination}")
                  
            except Exception as e:
                  logging.error(f"Error downloading Excel file from {url}: {e}")
                  raise


      def save_excel(self, data: bytes, destination: Path) -> None:
            try:
                  destination.parent.mkdir(parents=True, exist_ok=True)
                  with open(destination, 'wb') as f:
                        f.write(data)
            except Exception as e:
                  logging.error(f"Error saving Excel file to {destination}: {e}")
                  raise

class AutoSync:

      def __init__(self, report: str, download_manager: DownloadManager, file_system: FileSystem):
            configure_logging()
            self.download_manager = download_manager
            self.file_system = file_system
            self.report = report
            self.ran_all = False
            self.manager = HostManager()

      async def run_manager(self) -> None:
            await self.manager.run()


      async def auto_sync(self) -> None:
            async with PosConnector(None, None) as connector:
                  while True:
                        logging.info("Starting auto-sync process...")
                        await self._sync_once(connector=connector)
                        logging.info("Auto-sync process completed. Sleeping for 24 hours...")
                        if self.report == ALL_REPORT_URL:
                              self.report = TODAY_REPORT_URL
                              self.ran_all = True
                        await asyncio.sleep(60)  # Sleep for 1 minute for testing; change to 86400 for 24 hours in production

      async def _sync_once(self, connector: PosConnector) -> None:
            excel_path = self.file_system.resolve_excel_path(settings.excel_source_path)
            self.file_system.ensure_excel_file(excel_path)

            try:
                  await self.download_manager.download_excel(connector, CUSTOMERS_REPORT_URL, self.file_system.resolve_excel_path(settings.customer_excel_path))
                  await self.download_manager.download_excel(connector, ITEMS_REPORT_URL, self.file_system.resolve_excel_path(settings.catalog_excel_path))
                  await self.download_manager.download_excel(connector, self.report, excel_path)
                  await self.run_manager()
            except Exception as e:
                  logging.error(f"Failed to download today's report: {e}. Attempting to download all-time report.")
                  try:
                        await self.download_manager.download_excel(connector, ALL_REPORT_URL, excel_path)
                        await self.run_manager()
                        logging.info(f"Successfully downloaded and saved all-time Excel report to {excel_path}")
                  except Exception as e:
                        logging.error(f"Failed to download all-time report: {e}. No data was synchronized.")


if __name__ == "__main__":
      try:
            logging.basicConfig(level=logging.INFO)
            argv = sys.argv[1:]

            if argv:
                  if argv[0] == "today":
                        report_url = TODAY_REPORT_URL
                  elif argv[0] == "all":
                        report_url = ALL_REPORT_URL
                  else:
                        logging.error("Invalid argument. Use 'today' or 'all'.")
                        sys.exit(1)
            else:
                  report_url = TODAY_REPORT_URL

            file_system = FileSystem()
            download_manager = DownloadManager()
            auto_sync = AutoSync(report=report_url, download_manager=download_manager, file_system=file_system)
            asyncio.run(auto_sync.auto_sync())
      except KeyboardInterrupt:
            logging.info("Auto-sync process interrupted by user. Exiting...")
            sys.exit(0)