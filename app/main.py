import os
import uuid
import requests
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from .database import get_db, Base, engine
from . import models
from .auth import verify_token

PRODUCT_API_URL = os.getenv("PRODUCT_API_URL")

app = FastAPI()

Base.metadata.create_all(bind=engine)

class OrderCreate(BaseModel):
    product_id: int
    quantity: int

@app.post("/order")
def create_order(
    order: OrderCreate,
    db: Session = Depends(get_db),
    token_data: dict = Depends(verify_token)
):
    user_id = token_data.get("user_id")
    
    response = requests.get(f"{PRODUCT_API_URL}/product/{order.product_id}")

    if response.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to fetch product")

    product = response.json()

    if "stock" not in product:
        raise HTTPException(status_code=400, detail="Invalid product response")

    if product["stock"] < order.quantity:
        raise HTTPException(status_code=400, detail="Not enough stock available")
    
    new_order = models.Order(
        user_id=uuid.UUID(user_id),
        product_id=order.product_id,
        quantity=order.quantity
    )
    db.add(new_order)
    db.commit()
    db.refresh(new_order)
    return {
        "message": "Order created successfully",
        "order_id": new_order.id,
        "user_id": user_id
    }