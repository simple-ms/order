from pydantic import BaseModel

class OrderCreate(BaseModel):
    product_id: int
    quantity: int