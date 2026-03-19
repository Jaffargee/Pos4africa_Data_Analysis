from .base import AppError

class ScraperError(AppError):
      pass

class ElementNotFoundError(ScraperError):
      pass

class EmptyValueError(ScraperError):
      pass