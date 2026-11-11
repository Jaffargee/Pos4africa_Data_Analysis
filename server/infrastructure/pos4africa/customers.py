from pos4africa.infra.pos_connector import PosConnector
from pos4africa.config.settings import settings
from server.domain.customers.models import PosCustomerPayload

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

class POSRejectedError(Exception):
      """POS answered but refused to create the customer. Retrying won't help."""

class Customer:
      def __init__(self, connector: PosConnector):
            self._connector = connector

      async def create(self, data: PosCustomerPayload) -> dict:
            return await self._create(data)

      async def update(self, data: PosCustomerPayload) -> dict:
            return await self._update(data)

      async def _update(self, data: PosCustomerPayload) -> dict:
            if not data:
                  raise ValueError("Customer data not set")

            payload = data.model_dump(mode="json")
            return await self.__write(settings.pos_customer_edit_form_action.format(person_id=data.person_id), payload)

      async def _create(self, data: PosCustomerPayload) -> dict:
            if not data:
                  raise ValueError("Customer data not set")

            payload = data.model_dump(mode="json")
            return await self.__write(settings.pos_customer_form_action, payload)
      
      async def __write(self, endpoint: str, payload: dict):
            if not self._customer_data:
                  raise ValueError("Customer data not set")

            if not endpoint.startswith("/"):
                  raise ValueError("Endpoint must start with '/'")

            if not endpoint.startswith(settings.pos_customer_form_action):
                  raise ValueError(f"Endpoint must start with {settings.pos_customer_form_action}")

            if not payload:
                  raise ValueError("Payload is empty")
            
            data = {**DEFAULT_PAYLOAD, **{k: v for k, v in payload.items() if v}}
            resp = await self._connector.session.post(
                  endpoint, data=data, headers=HEADERS
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
            return {"message": "Customer created successfully", "person_id": person_id}