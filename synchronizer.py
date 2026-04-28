"""

This module contains the Syncrhonizer class, which is responsible for synchronizing data between different sources. 
It provides methods to fetch and download and Excel file an initiates data transfer from execl to supabase database.

Time to Fetch and download Excel file: Everyday 6pm

Access the TODAY_REPORT url and download the file imediately when is 6pm, 
The ALL_REPORT url is for contingency when the TODAY_REPORT url is not available, 
in that case the ALL_REPORT url will be used to fetch the data, but it will be used only when the TODAY_REPORT url is not available.



"""



from __future__ import annotations
from datetime import datetime
import asyncio
import requests
import subprocess
import sys
import json
import os

from pos4africa.worker.components.connector import PosConnector

from hashlib import sha256
import sys, logging

EXCEL_FILE_BASE_URL = 'https://fahadtahir.pos4africa.com/index.php/reports/generate/detailed_sales'
ALL_TIME_REPORT = ''
TODAY_REPORT = f'{EXCEL_FILE_BASE_URL}?report_type=simple&report_date_range_simple=TODAY&sale_type=all&with_time=1&excel_export=0&export_excel=1'
DSR_ABS_PATH = '/home/ubuntu/Pos4africa_Data_Analysis/Excels/DSR.xlsx' if sys.platform == 'linux' else 'C://Users/Tahir General/Documents/Projects/Data Analytics/Excels/DSR.xlsx'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

TRANSFER_CMD = ['python3', '-m', 'pos4africa.main'] if sys.platform == 'linux' else ['python', '-m', 'pos4africa.main']

class Syncrhonizer:
      async def init(self) -> None:
            logger.info('Starting the synchronizer...')
            while True:
                  logger.info('Fetching and downloading excel file...')
                  report = await self._fetch_excel_file()
                  if not report:
                        logger.warning('Failed to download the Excel file.')
                        continue
                  
                  self._overide_excel_file(DSR_ABS_PATH, report)
                  logger.info('Excel file downloaded successfully.')
                  logger.info('Initiating data transfer from Excel to Supabase...')
                  self.run_ingestion()

                  logger.info('Sleeping for 250s aproximately 4.7m ....')
                  await asyncio.sleep(250)

      async def _fetch_excel_file(self):
            async with PosConnector(None, None) as connector:
                  resp = await connector.session.get(TODAY_REPORT)
                  return resp.content
      
      def _overide_excel_file(self, file_path: str, file_data: str) -> None:
            with open(file_path, 'wb') as xlsx_f:
                  xlsx_f.write(file_data)
      
      def run_ingestion(self):
            """
                  Run the external data transfer script.
                  Returns True if the subprocess succeeded, False otherwise.
            """
            try:
                  subprocess.run(TRANSFER_CMD, shell=True)
                  logger.info("Ingestion completed successfully")
                  return True
            except subprocess.CalledProcessError as e:
                  logger.error(f"Ingestion failed", error=e)
                  logger.error(f"Transfer subprocess failed with exit code {e.returncode}")
                  logger.error(f"STDOUT: {e.stdout}")
                  logger.error(f"STDERR: {e.stderr}")
                  return False

async def main() -> None:
      synchronizer = Syncrhonizer()
      await synchronizer.init()

if __name__ == '__main__':
      try:
            asyncio.run(main())
      except KeyboardInterrupt:
            logger.info('Keyboard Interuppted. existing...')