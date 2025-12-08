"""
Async Kafka consumer for Order Service.
Listens for payment events and stock reservation responses using aiokafka.
"""
import json
from aiokafka import AIOKafkaConsumer
from sqlalchemy import select

from ..settings import settings
from ..database import AsyncSessionLocal
from ..models import Order, OrderStatus
from .producer import publish_order_created
from ..logger import logger


async def handle_payment_completed(event_data: dict):
    """Handle payment_completed event from Payment Service."""
    order_id = event_data.get("order_id")
    
    logger.info(f"Processing payment_completed for order: {order_id}")
    
    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(
                select(Order).filter(Order.id == order_id)
            )
            order = result.scalar_one_or_none()
            
            if order:
                order.status = OrderStatus.PAID
                await db.commit()
                logger.info(f"Order {order_id} status updated to PAID")
            else:
                logger.warning(f"Order not found: {order_id}")
                
        except Exception as e:
            await db.rollback()
            logger.error(f"Error updating order status: {str(e)}")


async def handle_payment_failed(event_data: dict):
    """Handle payment_failed event from Payment Service."""
    order_id = event_data.get("order_id")
    reason = event_data.get("reason", "Unknown")
    
    logger.info(f"Processing payment_failed for order: {order_id}, reason: {reason}")
    
    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(
                select(Order).filter(Order.id == order_id)
            )
            order = result.scalar_one_or_none()
            
            if order:
                order.status = OrderStatus.FAILED
                await db.commit()
                logger.info(f"Order {order_id} status updated to FAILED")
            else:
                logger.warning(f"Order not found: {order_id}")
                
        except Exception as e:
            await db.rollback()
            logger.error(f"Error updating order status: {str(e)}")


async def handle_stock_reserved(event_data: dict):
    """
    Handle stock_reserved event from Product Service.
    This confirms that stock has been successfully reserved.
    """
    correlation_id = event_data.get("correlation_id")
    order_id = event_data.get("order_id")
    total_amount = event_data.get("total_amount")
    
    logger.info(f"Processing stock_reserved for order: {order_id}, correlation_id: {correlation_id}")
    
    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(
                select(Order).filter(Order.id == order_id)
            )
            order = result.scalar_one_or_none()
            
            if order:
                # Update order with total amount and confirm status
                order.total_amount = total_amount
                order.status = OrderStatus.PENDING  # Ready for payment
                await db.commit()
                
                logger.info(f"Order {order_id} confirmed with total_amount: {total_amount}")
                
                # Publish order_created event
                await publish_order_created({
                    "order_id": order.id,
                    "user_id": order.user_id,
                    "product_id": order.product_id,
                    "quantity": order.quantity,
                    "total_amount": order.total_amount,
                    "status": order.status.value
                })
            else:
                logger.warning(f"Order not found: {order_id}")
                
        except Exception as e:
            await db.rollback()
            logger.error(f"Error confirming order: {str(e)}")


async def handle_stock_reservation_failed(event_data: dict):
    """
    Handle stock_reservation_failed event from Product Service.
    This indicates that stock reservation failed.
    """
    correlation_id = event_data.get("correlation_id")
    order_id = event_data.get("order_id")
    reason = event_data.get("reason", "Unknown")
    
    logger.info(
        f"Processing stock_reservation_failed for order: {order_id}, "
        f"correlation_id: {correlation_id}, reason: {reason}"
    )
    
    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(
                select(Order).filter(Order.id == order_id)
            )
            order = result.scalar_one_or_none()
            
            if order:
                order.status = OrderStatus.FAILED
                await db.commit()
                logger.info(f"Order {order_id} marked as FAILED due to stock reservation failure")
            else:
                logger.warning(f"Order not found: {order_id}")
                
        except Exception as e:
            await db.rollback()
            logger.error(f"Error updating order status: {str(e)}")


async def handle_product_created(event_data: dict):
    """Handle product_created event from Product Service."""
    from ..kafka_handlers.product_events import handle_product_created as sync_handler
    
    async with AsyncSessionLocal() as db:
        try:
            # Convert async session to sync for the handler
            # Note: This is a temporary bridge - ideally product_events should be async too
            await db.run_sync(lambda session: sync_handler(event_data, session))
        except Exception as e:
            logger.error(f"Error handling product_created: {str(e)}")


async def handle_product_updated(event_data: dict):
    """Handle product_updated event from Product Service."""
    from ..kafka_handlers.product_events import handle_product_updated as sync_handler
    
    async with AsyncSessionLocal() as db:
        try:
            await db.run_sync(lambda session: sync_handler(event_data, session))
        except Exception as e:
            logger.error(f"Error handling product_updated: {str(e)}")


async def handle_product_deleted(event_data: dict):
    """Handle product_deleted event from Product Service."""
    from ..kafka_handlers.product_events import handle_product_deleted as sync_handler
    
    async with AsyncSessionLocal() as db:
        try:
            await db.run_sync(lambda session: sync_handler(event_data, session))
        except Exception as e:
            logger.error(f"Error handling product_deleted: {str(e)}")


async def start_payment_event_consumer():
    """Start the async Kafka consumer for payment, stock, and product events."""
    logger.info("Starting Order Service Async Kafka Consumer...")
    
    consumer = AIOKafkaConsumer(
        "payment-events",
        "stock-events",
        "product-events",
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    await consumer.start()
    logger.info("Listening for payment events, stock events, and product events...")
    
    try:
        async for message in consumer:
            try:
                event_data = message.value
                event_type = event_data.get("event_type")
                
                logger.info(f"Received event from topic '{message.topic}': {event_type}")
                
                # Payment events
                if event_type == "payment_completed":
                    await handle_payment_completed(event_data)
                elif event_type == "payment_failed":
                    await handle_payment_failed(event_data)
                # Stock events
                elif event_type == "stock_reserved":
                    await handle_stock_reserved(event_data)
                elif event_type == "stock_reservation_failed":
                    await handle_stock_reservation_failed(event_data)
                # Product events
                elif event_type == "product_created":
                    await handle_product_created(event_data)
                elif event_type == "product_updated":
                    await handle_product_updated(event_data)
                elif event_type == "product_deleted":
                    await handle_product_deleted(event_data)
                else:
                    logger.warning(f"Unknown event type: {event_type}")
                    
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")
    finally:
        await consumer.stop()
        logger.info("Kafka consumer stopped")


if __name__ == "__main__":
    import asyncio
    asyncio.run(start_payment_event_consumer())
