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

EXCEL_FILE_BASE_URL = 'https://fahadtahir.pos4africa.com/index.php/reports/generate/detailed_sales'
ALL_TIME_REPORT = ''
TODAY_REPORT = f'{EXCEL_FILE_BASE_URL}?report_type=simple&report_date_range_simple=TODAY&sale_type=all&with_time=1&excel_export=0&export_excel=1'
SYNCRHONIZER_LOG_FILE = './_syncrhonizer.json'

class Syncrhonizer:
      def __init__(self):
            self.current_hashed = ""

      def _dedup_date_json_log(self) -> None:
            with open(SYNCRHONIZER_LOG_FILE, 'r') as sync_r:
                  str_log = sync_r.read()
                  if str_log.strip():
                        return dict(json.loads(str_log))
                  return {}

      def _write_json_log_file(self, log: dict) -> None:
            with open(SYNCRHONIZER_LOG_FILE, 'w') as sync_w:
                  log[datetime.now().strftime('%Y-%m-%d %H:%M:%S')] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                  log['last_sync'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                  sync_w.write(json.dumps(log))

      def _create_json_log_file(self) -> None:
            with open(SYNCRHONIZER_LOG_FILE, 'w') as sync_w:
                  log = {
                        'last_sync': None
                  }
                  sync_w.write(json.dumps(log))

      async def init(self) -> None:
            print('Starting the synchronizer...')
            while True:
                  print('Checking if it is time to fetch the Excel file... time:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                  if self._is_time():
                        print('It is time to fetch the Excel file. Fetching...')
                        report = await self._fetch_excel_file()
                        if report:
                              self._overide_excel_file('./Excels/DSR.xlsx', report)
                              print('Excel file downloaded successfully.')
                              hashed_report = sha256(report).hexdigest()
                              if self.current_hashed == hashed_report:
                                    print('Nothing Changes. Skipping data transfer...')
                                    await asyncio.sleep(250)
                                    continue
                              
                              self.current_hashed = hashed_report
                              print('Initiating data transfer from Excel to Supabase...')
                              try:
                                    if sys.platform == 'win32':
                                          subprocess.run('python -m pos4africa.main', shell=True)
                                    else: subprocess.run('python3 -m pos4africa.main', shell=True)
                                    print('Data transfer completed.')
                              except Exception as exc:
                                    print('Data transfer failed with error:', str(exc))
                        else:
                              print('Failed to download the Excel file.')
                  else:
                        print('It is not time to fetch the Excel file yet. Waiting...')
                  await asyncio.sleep(250)

      async def _fetch_excel_file(self):
            async with PosConnector(None, None) as connector:
                  resp = await connector.session.get(TODAY_REPORT)
                  xlsx = resp.content
                  return xlsx

      def _overide_excel_file(self, file_path: str, file_data: str) -> None:
            with open(file_path, 'wb') as xlsx_f:
                  xlsx_f.write(file_data)
            
      def _is_time(self) -> bool:
            now = datetime.now()
            print("Time: ", now)          
            return True


async def main() -> None:
      synchronizer = Syncrhonizer()
      await synchronizer.init()

if __name__ == '__main__':
      try:
            asyncio.run(main())
      except KeyboardInterrupt:
            print('Keyboard Interuppted. existing...')