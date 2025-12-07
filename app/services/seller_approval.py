"""
Seller approval service for orders.

Handles seller approval of orders, which triggers stock reservation.
"""
import uuid
from typing import Optional
from fastapi import HTTPException, status
from sqlalchemy import select

from ..models.order import Order, OrderStatus
from ..models.product_seller_cache import ProductSellerCache
from ..repository import OrderRepository
from ..kafka_producer import publish_stock_reservation_request
from ..logger import logger


async def approve_order(
    order_id: uuid.UUID,
    seller_id: uuid.UUID,
    order_repository: OrderRepository
) -> Order:
    """
    Approve an order (seller only).
    
    This triggers stock reservation and moves order to PENDING status.
    Only the seller who owns the product can approve the order.
    """
    logger.info(f"Seller {seller_id} attempting to approve order {order_id}")
    
    try:
        # Get the order
        order = await order_repository.get_by_id(order_id)
        if not order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Order not found"
            )
        
        # Check if order is in AWAITING_APPROVAL status
        if order.status != OrderStatus.AWAITING_APPROVAL:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Order is not awaiting approval. Current status: {order.status}"
            )
        
        # Verify seller owns the product
        result = await order_repository.db.execute(
            select(ProductSellerCache)
            .filter(ProductSellerCache.product_id == order.product_id)
        )
        cache_entry = result.scalar_one_or_none()
        
        if not cache_entry:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found in seller cache"
            )
        
        if cache_entry.seller_id != seller_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only approve orders for your own products"
            )
        
        # Update order status to PENDING
        order.status = OrderStatus.PENDING
        await order_repository.update(order)
        logger.info(f"Order {order_id} approved, status changed to PENDING")
        
        # NOW reserve stock via Kafka
        correlation_id = str(uuid.uuid4())
        logger.info(f"Publishing stock reservation request for approved order {order_id}")
        publish_stock_reservation_request({
            "correlation_id": correlation_id,
            "order_id": str(order.id),
            "product_id": order.product_id,
            "quantity": order.quantity
        })
        
        return order
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error approving order: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to approve order"
        )


async def reject_order(
    order_id: uuid.UUID,
    seller_id: uuid.UUID,
    order_repository: OrderRepository,
    reason: Optional[str] = None
) -> Order:
    """
    Reject an order (seller only).
    
    Moves order to CANCELLED status without reserving stock.
    """
    logger.info(f"Seller {seller_id} attempting to reject order {order_id}")
    
    try:
        # Get the order
        order = await order_repository.get_by_id(order_id)
        if not order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Order not found"
            )
        
        # Check if order is in AWAITING_APPROVAL status
        if order.status != OrderStatus.AWAITING_APPROVAL:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Order is not awaiting approval. Current status: {order.status}"
            )
        
        # Verify seller owns the product
        result = await order_repository.db.execute(
            select(ProductSellerCache)
            .filter(ProductSellerCache.product_id == order.product_id)
        )
        cache_entry = result.scalar_one_or_none()
        
        if not cache_entry:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found in seller cache"
            )
        
        if cache_entry.seller_id != seller_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only reject orders for your own products"
            )
        
        # Update order status to CANCELLED
        order.status = OrderStatus.CANCELLED
        await order_repository.update(order)
        logger.info(f"Order {order_id} rejected by seller, status changed to CANCELLED")
        
        return order
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error rejecting order: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to reject order"
        )
