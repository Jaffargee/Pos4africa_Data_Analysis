from fastapi import APIRouter

from domain.customers.models import CustomerRequest

router = APIRouter(prefix="/customers", tags=["Customers"])


@router.get("/")
def get_customers():
      return {"message": "List of customers"}

@router.post("/create")
def create_customer(customer: CustomerRequest):
      # Here you would typically call a function to create the customer in your database
      return {"message": "Customer created", "customer": customer}