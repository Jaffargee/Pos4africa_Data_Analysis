
class AppError(Exception):
      def __init__(self, sale_id: int | str | None, message: str, code: str = None, context: dict = None):
            self.code = code
            self.context = context or {}

            msg = message
            if sale_id is not None:
                  msg = f"[Invoice {sale_id}] [{code}]:{msg}"
            if code:
                  msg = f"[{code}] {msg}"

            super().__init__(msg)