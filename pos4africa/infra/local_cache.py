import time
import json
from pathlib import Path
from appdirs import user_cache_dir
from pos4africa.config.settings import settings
from typing import Any
import tempfile, os

class LocalCache:

      def __init__(self):
            pass


      def __get_cache_dir_path(self) -> Path:
            return Path(user_cache_dir("pos4africa"))

      def __cache_path(self, key: str) -> Path:
            return self.__get_cache_dir_path() / f"{key}.json"

      def __read_cache(self, key: str, ttl: int = settings.default_ttl_seconds) -> Any | None:
            cp = self.__cache_path(key)

            if not cp.exists():
                  return None

            if time.time() - cp.stat().st_mtime > ttl:
                  return None

            try:
                  with cp.open("r", encoding="utf-8") as cf:
                        return json.load(cf)
            except Exception:
                  return None

      def __write_cache_atomic(self, key: str, data: Any) -> None:
            cp = self.__cache_path(key)
            temp_path = None
            try:      
                  cp.parent.mkdir(parents=True, exist_ok=True)

                  with tempfile.NamedTemporaryFile("w", dir=str(cp.parent), encoding="utf-8", delete=False) as tempF:
                        temp_path = tempF.name
                        json.dump(data, tempF, ensure_ascii=False, indent=None)
                        tempF.flush()
                        os.fsync(tempF.fileno())
                  os.replace(tempF.name, cp)
                  return True
            
            except Exception as exc:
                  if temp_path and os.path.exists(temp_path):
                        try:
                              os.unlink(temp_path)
                        except OSError:
                              pass
                        return False
                  return False
            
      def read_cache(self, key: str, ttl: int = settings.default_ttl_seconds) -> Any | None:
            return self.__read_cache(key, ttl)

      def write_cache_atomic(self, key: str, data: Any) -> True:
            return self.__write_cache_atomic(key, data)

lc = LocalCache()