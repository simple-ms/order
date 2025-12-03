import uuid
import httpx
from typing import List
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select

from .database import get_db
from .models import Order, OrderStatus
from .config import PRODUCT_API_URL
from .schemas import OrderCreate, OrderResponse, OrderStatusUpdate
from .logger import logger
from .kafka_producer import publish_order_created, publish_order_status_updated
from .dependencies import get_current_user_id, get_current_user_role

app = FastAPI(
    title="Order Service",
    description="Order management microservice with Kafka integration",
    version="1.0.0",
    docs_url="/docs/order",
    openapi_url="/openapi.json/order",
    redoc_url="/redoc/order"
)

security = HTTPBearer()


# --- HEALTH CHECK ---

@app.get("/order/health", tags=["Health"])
async def health_check():
    """Health check endpoint for monitoring."""
    return {"status": "healthy", "service": "order-service"}


# --- ORDER ENDPOINTS ---

@app.post(
    "/order",
    response_model=OrderResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Orders"]
)
async def create_order(
    order: OrderCreate,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """
    Create a new order.
    
    Steps:
    1. Validate user authentication
    2. Fetch product details from Product service
    3. Verify stock availability
    4. Create order in database
    5. Publish order_created event to Kafka
    """
    logger.info(
        f"Order creation request from user {user_id} for product {order.product_id}, "
        f"quantity: {order.quantity}"
    )
    
    # Fetch product details from Product service
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            logger.info(f"Fetching product details: {PRODUCT_API_URL}/product/{order.product_id}")
            response = await client.get(f"{PRODUCT_API_URL}/product/{order.product_id}")
        except httpx.RequestError as e:
            logger.error(f"Product service unavailable: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Product service unavailable"
            )

    if response.status_code != 200:
        logger.warning(f"Failed to fetch product {order.product_id}: Status {response.status_code}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to fetch product details"
        )

    product = response.json()
    
    # Validate product response
    if "stock" not in product or "price" not in product:
        logger.error(f"Invalid product response for product {order.product_id}: missing required fields")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid product response"
        )

    # Check stock availability
    if product["stock"] < order.quantity:
        logger.warning(
            f"Insufficient stock for product {order.product_id}: "
            f"requested {order.quantity}, available {product['stock']}"
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Not enough stock available. Available: {product['stock']}"
        )
    
    # Calculate total amount
    total_amount = product["price"] * order.quantity
    
    try:
        # Create order
        new_order = Order(
            user_id=uuid.UUID(user_id),
            product_id=order.product_id,
            quantity=order.quantity,
            total_amount=total_amount,
            status=OrderStatus.PENDING
        )
        db.add(new_order)
        await db.commit()
        await db.refresh(new_order)
        
        logger.info(
            f"Order created successfully: Order ID {new_order.id} for user {user_id}, "
            f"total amount: ${total_amount}"
        )
        
        # Publish order_created event to Kafka
        event_published = publish_order_created({
            "order_id": new_order.id,
            "user_id": new_order.user_id,
            "product_id": new_order.product_id,
            "quantity": new_order.quantity,
            "total_amount": new_order.total_amount,
            "status": new_order.status.value
        })
        
        if event_published:
            logger.info(f"order_created event published for order {new_order.id}")
        else:
            logger.warning(f"Failed to publish order_created event for order {new_order.id}")
        
        return new_order
        
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Database error while creating order: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred"
        )


@app.get(
    "/orders",
    response_model=List[OrderResponse],
    tags=["Orders"]
)
async def get_user_orders(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """
    Get all orders for the authenticated user.
    """
    logger.info(f"Fetching orders for user {user_id}")
    
    try:
        result = await db.execute(select(Order).filter(Order.user_id == user_id))
        orders = result.scalars().all()
        
        logger.info(f"Found {len(orders)} orders for user {user_id}")
        return orders
        
    except SQLAlchemyError as e:
        logger.error(f"Database error while fetching orders: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred"
        )


@app.get(
    "/orders/{order_id}",
    response_model=OrderResponse,
    tags=["Orders"]
)
async def get_order(
    order_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """
    Get a specific order by ID.
    
    Only the owner of the order can view it.
    """
    logger.info(f"Fetching order {order_id} for user {user_id}")
    
    try:
        result = await db.execute(
            select(Order).filter(
                Order.id == order_id,
                Order.user_id == user_id  # Ensure user owns this order
            )
        )
        order = result.scalar_one_or_none()
        
        if not order:
            logger.warning(f"Order not found or unauthorized: {order_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Order not found"
            )
        
        return order
        
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(f"Database error while fetching order: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred"
        )


@app.put(
    "/orders/{order_id}/status",
    response_model=OrderResponse,
    tags=["Orders"]
)
async def update_order_status(
    order_id: str,
    status_update: OrderStatusUpdate,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
    user_role: str = Depends(get_current_user_role)
):
    """
    Update order status.
    
    Only admins or sellers can update order status.
    Publishes order_status_updated event to Kafka.
    """
    # Check authorization (only admin or seller can update)
    if user_role not in ["admin", "seller"]:
        logger.warning(f"Unauthorized status update attempt by user {user_id} with role {user_role}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins and sellers can update order status"
        )

    logger.info(f"Updating order {order_id} status to {status_update.status}")
    
    try:
        result = await db.execute(select(Order).filter(Order.id == order_id))
        order = result.scalar_one_or_none()
        
        if not order:
            logger.warning(f"Order not found: {order_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Order not found"
            )
        
        # Update status
        old_status = order.status
        order.status = status_update.status
        await db.commit()
        await db.refresh(order)
        
        logger.info(f"Order {order_id} status updated from {old_status} to {order.status}")
        
        # Publish status update event
        event_published = publish_order_status_updated(
            order_id=str(order.id),
            status=order.status.value,
            user_id=str(order.user_id)
        )
        
        if event_published:
            logger.info(f"order_status_updated event published for order {order.id}")
        else:
            logger.warning(f"Failed to publish order_status_updated event for order {order.id}")
        
        return order
        
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Database error while updating order status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred"
        )