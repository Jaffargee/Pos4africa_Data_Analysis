from pos4africa.infra.pos_connector import PosConnector
from pos4africa.worker.components.scrapers.customers.create_customer import CreateCustomer, FormInputs

class CustomerSync:
      def __init__( self, connector: PosConnector) -> None:
            self._connector = connector

      async def run(self) -> dict:
            pass

      async def _create_in_pos(self, row: dict, first_name: str) -> int:
            creator = CreateCustomer(self._connector)
            creator.set_input_field_value(FormInputs.FIRST_NAME, first_name)
            creator.set_input_field_value(FormInputs.LAST_NAME, (row.get("last_name") or "").strip())
            creator.set_input_field_value(FormInputs.STATE, row.get("state"))
            creator.set_input_field_value(FormInputs.COUNTRY, row.get("country"))
            return await creator.run()

