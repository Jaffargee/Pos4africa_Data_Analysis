from enum import Enum
from pos4africa.infra.pos_connector import PosConnector
from pos4africa.config.settings import settings
from pos4africa.worker.components.base import BaseComponent
from pos4africa.shared.errors.pos_errors import POSRejectedError

class FormInputs(Enum):
      FIRST_NAME = "first_name"
      LAST_NAME = "last_name"
      COUNTRY = "country"
      STATE = "state"


HEADERS = {
      "Content-Type": "application/x-www-form-urlencoded",
      "X-Requested-With": "XMLHttpRequest",
      "Accept": "application/json, text/javascript, */*; q=0.01",
      "Origin": settings.pos_base_url,
      "Referer": f"{settings.pos_base_url}{settings.pos_customer_form}",
}

DEFAULT_PAYLOAD = {
      "first_name": "",
      "last_name": "",
      "email": "",
      "phone_number": "",
      "address_1": "",
      "address_2": "",
      "city": "",
      "state": "",
      "zip": "",
      "country": "",
      "comments": "",
      "internal_notes": "",
      "credit_limit": "",
      "default_term_id": "",
      "company_name": "",
      "account_number": "",
      "taxable": "1",
      "tax_certificate": "",
      "customer_info_popup": "",
      "redirect_code": "0",
}

class CreateCustomer(BaseComponent):
      def __init__(self, connector: PosConnector) -> None:
            super().__init__()
            self.__connector: PosConnector = connector
            self.__payload: dict = {}

      def set_input_field_value(self, input_field: FormInputs, value: str | None) -> None:
            self.__payload[input_field.value] = value

      async def run(self) -> int:
            return await self.__save()

      async def _save(self) -> int:
            data = {**DEFAULT_PAYLOAD, **{k: v for k, v in self.__payload.items() if v}}
            resp = await self.__connector.session.post(
                  settings.pos_customer_form_action, data=data, headers=HEADERS
            )
            resp.raise_for_status()

            try:
                  body = resp.json()
            except ValueError as exc:
                  raise POSRejectedError("POS returned non-JSON (session expired?)") from exc
            try:
                  person_id = int(body.get("person_id") or -1)
            except (TypeError, ValueError):
                  person_id = -1

            if not body.get("success") or person_id <= 0:
                  raise POSRejectedError(body.get("message") or "unknown POS error")

            self.log.info("save.created_customer", person_id=person_id)
            return person_id




      