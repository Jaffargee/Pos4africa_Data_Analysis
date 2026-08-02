from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from pos4africa.manager.memory.store import MemoryStore
from pos4africa.shared.models.sale import RawPayment, RawSale, RawSaleItem
from pos4africa.worker.components.base import BaseComponent


class CustomerScraper(BaseComponent):
      pass