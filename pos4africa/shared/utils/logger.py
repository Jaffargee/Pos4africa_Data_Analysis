
from __future__ import annotations

import logging
import sys
import structlog

from pos4africa.config.settings import settings

def configure_logging() -> None:
      
      log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
      
      shared_processors: list[structlog.types.Processor] = [
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
      ]
      
      if sys.stderr.isatty():
            # Dev: Colorful, human readable
            renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer()
            
      else:
            renderer = structlog.processors.JSONRenderer()
            
            
      structlog.configure(
            processors=[*shared_processors, renderer],
            wrapper_class=structlog.make_filtering_bound_logger(log_level),
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
      )
      
def get_logger(name: str) -> structlog.stdlib.BoundLogger:
      return structlog.get_logger(name)   