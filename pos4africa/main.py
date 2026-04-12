"""
main.py — entrypoint

Run with:
python -m pos4africa.main
or:
python pos4africa/main.py
"""

from __future__ import annotations

import asyncio

from pos4africa.manager.host import HostManager
# from pos4africa.shared.utils.logger import configure_logging


async def main() -> None:
      # configure_logging()
      manager = HostManager()
      await manager.run()


if __name__ == "__main__":
      try:
            asyncio.run(main())
      except KeyboardInterrupt:
            print("Keyboard Interuppted. existing...")