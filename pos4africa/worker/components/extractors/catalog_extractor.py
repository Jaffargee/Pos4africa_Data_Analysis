from __future__ import annotations

from pathlib import Path

import pandas as pd

from pos4africa.shared.models.customer import Customer
from pos4africa.worker.components.base import BaseComponent
from pos4africa.manager.memory.store import MemoryStore
from pos4africa.worker.components.extractors.extractor import Extractor

class CustomerExtractor(BaseComponent, Extractor):
      pass