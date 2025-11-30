import uuid
import httpx
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from .database import get_db, Base, engine
from . import models
from .auth import verify_token
from .config import PRODUCT_API_URL
from .schemas import OrderCreate
from .logger import logger

app = FastAPI()

@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)
    logger.info("Order service started")

@app.post("/order")
async def create_order(
    order: OrderCreate,
    db: Session = Depends(get_db),
    token_data: dict = Depends(verify_token)
):
    user_id = token_data.get("user_id")
    logger.info(f"Order creation request from user {user_id} for product {order.product_id}, quantity: {order.quantity}")
    
    async with httpx.AsyncClient() as client:
        try:
            logger.info(f"Fetching product details from product service: {PRODUCT_API_URL}/product/{order.product_id}")
            response = await client.get(f"{PRODUCT_API_URL}/product/{order.product_id}")
        except httpx.RequestError as e:
            logger.error(f"Product service unavailable: {e}")
            raise HTTPException(status_code=503, detail="Product service unavailable")

    if response.status_code != 200:
        logger.warning(f"Failed to fetch product {order.product_id}: Status {response.status_code}")
        raise HTTPException(status_code=400, detail="Failed to fetch product")

    product = response.json()

    if "stock" not in product:
        logger.error(f"Invalid product response for product {order.product_id}: missing 'stock' field")
        raise HTTPException(status_code=400, detail="Invalid product response")

    if product["stock"] < order.quantity:
        logger.warning(f"Insufficient stock for product {order.product_id}: requested {order.quantity}, available {product['stock']}")
        raise HTTPException(status_code=400, detail="Not enough stock available")
    
    new_order = models.Order(
        user_id=uuid.UUID(user_id),
        product_id=order.product_id,
        quantity=order.quantity
    )
    db.add(new_order)
    db.commit()
    db.refresh(new_order)
    logger.info(f"Order created successfully: Order ID {new_order.id} for user {user_id}")
    return {
        "message": "Order created successfully",
        "order_id": new_order.id,
        "user_id": user_id
    }