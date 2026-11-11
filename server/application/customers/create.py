from pos4africa.infra.pos_connector import PosConnector
from server.domain.customers.models import PosCustomerPayload
from server.infrastructure.pos4africa.customers import Customer

class Customer:
      def __init__(self, pos_connector: PosConnector):
            self._connector = pos_connector
            self._customer_data: PosCustomerPayload | None = None

      def set_customer_data(self, customer_data: PosCustomerPayload) -> None:
            self._customer_data = customer_data

      def create_customer(self) -> dict:
            pass