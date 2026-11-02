from __future__ import annotations

from pos4africa.worker.components.base import BaseComponent

from collections import Counter
from rapidfuzz import fuzz, process
import unicodedata, re

class Cleanner(BaseComponent):

      MULTI_NAME_PATTERN = re.compile(
            r",|/|&|\bAND\b",
            flags=re.IGNORECASE,
      )

      def __init__(self):
            super().__init__()


      def _normalize(self, name: str) -> str:
            if not name or name is None:
                  return ""
            # 1. Standardize Unicode representation.
            name = unicodedata.normalize("NFKC", name)
            # 2. Make comparisons case-insensitive.
            name = name.casefold()
            # 3. Replace punctuation with spaces.
            # This prevents words from accidentally joining together.
            characters = []
            for char in name:
                  if unicodedata.category(char).startswith("P"):
                        characters.append(" ")
                  else:
                        characters.append(char)
            name = "".join(characters)
            # 4. Collapse repeated whitespace and trim both ends.
            name = " ".join(name.split())
            return name.upper()


      def _exclude_mult_comment_names(self, comments: list[str]) -> list[str]:
            filtered_comments: list[str] = []

            for comment in comments:
                  if not comment or not comment.strip(): continue

                  parts = [part.strip() for part in self.MULTI_NAME_PATTERN.split(comment) if part.strip()]
            
                  if len(parts) <= 1:
                        filtered_comments.append(parts)

            return filtered_comments
