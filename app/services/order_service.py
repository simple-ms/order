"""
Order Service - Business Logic Layer

Uses Kafka saga pattern for stock reservation:
1. Creates order in PENDING state
2. Publishes stock_reservation_request to Kafka
3. Product service reserves stock and responds via Kafka
4. Order consumer updates order status based on response
"""
import uuid
from uuid import UUID
from typing import List, Dict
from fastapi import HTTPException, status
from sqlalchemy.exc import SQLAlchemyError

from ..models.order import Order, OrderStatus
from ..schemas.order import OrderCreate, OrderResponse, OrderStatusUpdate
from ..repository import OrderRepository
from ..logger import logger
from ..kafka_producer import (
    publish_stock_reservation_request,
    publish_stock_restoration_request,
    publish_order_created,
    publish_order_status_updated
)


class OrderService:
    """Service for order business logic using Kafka saga pattern."""
    
    def __init__(self, order_repository: OrderRepository):
        self.order_repository = order_repository
    
    async def create_order(self, user_id: UUID, order_data: OrderCreate, user_role: str = None) -> Order:
        """
        Create a new order using Kafka saga pattern.
        
        Only buyers can create orders. Sellers must have a buyer account to purchase.
        
        Steps:
        1. Validate user role (buyer only)
        2. Create order in database with PENDING status
        3. Publish stock_reservation_request event to Kafka
        4. Product service handles stock check and reservation
        5. Response comes back via Kafka (handled by consumer)
        
        Note: This is an eventual consistency pattern. The order is created
        immediately, but stock reservation happens asynchronously.
        """
        # Validate user role - only buyers can create orders
        if user_role and user_role == "seller":
            logger.warning(f"Seller {user_id} attempted to create order")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Sellers cannot create orders. Please use a buyer account to purchase products."
            )
        
        logger.info(
            f"Order creation request from user {user_id} for product {order_data.product_id}, "
            f"quantity: {order_data.quantity}"
        )
        
        # Generate correlation ID for saga tracking
        correlation_id = str(uuid.uuid4())
        
        try:
            # Step 1: Create order in database with PENDING status
            new_order = Order(
                user_id=user_id,
                product_id=order_data.product_id,
                quantity=order_data.quantity,
                total_amount=0.0,  # Will be updated by stock_reserved event
                status=OrderStatus.PENDING,
                # Add address fields if provided
                shipping_address=order_data.shipping_address,
                city=order_data.city,
                postal_code=order_data.postal_code,
                country=order_data.country
            )
            order = await self.order_repository.create(new_order)
            
            logger.info(f"Order created with ID: {order.id}, correlation_id: {correlation_id}")
            
            # Step 2: Publish stock reservation request to Kafka
            event_published = publish_stock_reservation_request({
                "correlation_id": correlation_id,
                "order_id": order.id,
                "user_id": user_id,
                "product_id": order_data.product_id,
                "quantity": order_data.quantity
            })
            
            if event_published:
                logger.info(
                    f"stock_reservation_request published for order {order.id}, "
                    f"correlation_id: {correlation_id}"
                )
            else:
                logger.warning(
                    f"Failed to publish stock_reservation_request for order {order.id}. "
                    "Stock reservation will not be processed."
                )
                # Mark order as failed if Kafka publish fails
                order.status = OrderStatus.FAILED
                await self.order_repository.update(order)
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Failed to process order. Please try again."
                )
            
            # Return order immediately (stock reservation happens asynchronously)
            # The order status will be updated by the Kafka consumer when
            # stock_reserved or stock_reservation_failed events are received
            return order
            
        except HTTPException:
            raise
        except SQLAlchemyError as e:
            await self.order_repository.rollback()
            logger.error(f"Database error while creating order: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred"
            )
    
    async def get_user_orders(
        self, 
        user_id: UUID, 
        skip: int = 0, 
        limit: int = 100
    ) -> List[Order]:
        """Get all orders for a user with pagination."""
        limit = min(limit, 100)
        
        logger.info(f"Fetching orders for user {user_id}: skip={skip}, limit={limit}")
        
        try:
            orders = await self.order_repository.get_all_by_user(user_id, skip, limit)
            logger.info(f"Found {len(orders)} orders for user {user_id}")
            return orders
        except SQLAlchemyError as e:
            logger.error(f"Database error while fetching orders: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred"
            )
    
    async def get_order(self, order_id: UUID, user_id: UUID) -> Order:
        """Get a specific order by ID for a user."""
        logger.info(f"Fetching order {order_id} for user {user_id}")
        
        try:
            order = await self.order_repository.get_by_id_and_user(order_id, user_id)
            
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
    
    async def update_order_status(
        self, 
        order_id: UUID, 
        status_update: OrderStatusUpdate,
        user_id: UUID,
        user_role: str
    ) -> Order:
        """Update order status (admin/seller only)."""
        # Check authorization
        if user_role not in ["admin", "seller", "system"]:
            logger.warning(f"Unauthorized status update attempt by user {user_id} with role {user_role}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only admins and sellers can update order status"
            )

        logger.info(f"Updating order {order_id} status to {status_update.status}")
        
        try:
            order = await self.order_repository.get_by_id(order_id)
            
            if not order:
                logger.warning(f"Order not found: {order_id}")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Order not found"
                )
            
            # Update status
            old_status = order.status
            order.status = status_update.status
            updated_order = await self.order_repository.update(order)
            
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
            
            return updated_order
        except HTTPException:
            raise
        except SQLAlchemyError as e:
            await self.order_repository.rollback()
            logger.error(f"Database error while updating order status: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred"
            )
    
    async def cancel_order(self, order_id: UUID, user_id: UUID) -> Dict[str, str]:
        """
        Cancel an order using Kafka for stock restoration.
        
        Steps:
        1. Verify order belongs to user and is cancellable
        2. Update order status to CANCELLED
        3. Publish stock_restoration_request event to Kafka
        """
        logger.info(f"Cancelling order {order_id} for user {user_id}")
        
        try:
            order = await self.order_repository.get_by_id_and_user(order_id, user_id)
            
            if not order:
                logger.warning(f"Order not found or unauthorized: {order_id}")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Order not found"
                )
            
            if order.status not in [OrderStatus.PENDING, OrderStatus.PAYMENT_PENDING]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Cannot cancel order with status: {order.status.value}"
                )
            
            # Update order status to CANCELLED
            order.status = OrderStatus.CANCELLED
            await self.order_repository.update(order)
            
            # Publish stock restoration request to Kafka (asynchronous)
            event_published = publish_stock_restoration_request({
                "order_id": order.id,
                "product_id": order.product_id,
                "quantity": order.quantity
            })
            
            if event_published:
                logger.info(f"stock_restoration_request published for order {order_id}")
            else:
                logger.warning(f"Failed to publish stock_restoration_request for order {order_id}")
            
            logger.info(f"Order {order_id} cancelled successfully")
            return {"message": "Order cancelled successfully"}
            
        except HTTPException:
            raise
        except SQLAlchemyError as e:
            await self.order_repository.rollback()
            logger.error(f"Database error while cancelling order: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred"
            )
