import os
import requests
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from .database import get_db, init_db
from . import models

PRODUCT_API_URL = os.getenv("PRODUCT_API_URL")

app = FastAPI()

@app.on_event("startup")
def startup():
    init_db()

class OrderCreate(BaseModel):
    product_id: int
    quantity: int

@app.post("/order")
def create_order(order: OrderCreate, db: Session = Depends(get_db)):
    response = requests.get(f"{PRODUCT_API_URL}/product/{order.product_id}")

    if response.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to fetch product")

    product = response.json()

    if "stock" not in product:
        raise HTTPException(status_code=400, detail="Invalid product response")

    if product["stock"] < order.quantity:
        raise HTTPException(status_code=400, detail="Not enough stock available")
    
    new_order = models.Order(
        product_id=order.product_id,
        quantity=order.quantity
    )
    db.add(new_order)
    db.commit()
    db.refresh(new_order)
    return {"message": "Order created successfully", "id": new_order.id}