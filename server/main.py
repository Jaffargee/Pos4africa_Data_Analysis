import uvicorn
from fastapi import FastAPI
from api import customers_router, sales_router, items_router

app = FastAPI()

app.include_router(customers_router)
app.include_router(sales_router)
app.include_router(items_router)

# if __name__ == "__main__":
#       uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)