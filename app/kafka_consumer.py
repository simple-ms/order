"""
Kafka consumer for Order Service.
Listens for payment events and stock reservation responses.
"""
import json
from kafka import KafkaConsumer
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

from .settings import settings
from .models import Order, OrderStatus
from .kafka_producer import publish_order_created
from .logger import logger


# Synchronous database URL (for consumer)
SYNC_DATABASE_URL = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")

# Create synchronous engine for Kafka consumer
sync_engine = create_engine(SYNC_DATABASE_URL)


def get_sync_db_session():
    """Create a synchronous database session for Kafka consumer."""
    return Session(sync_engine)


def handle_payment_completed(event_data: dict):
    """Handle payment_completed event from Payment Service."""
    order_id = event_data.get("order_id")
    
    logger.info(f"Processing payment_completed for order: {order_id}")
    
    db = get_sync_db_session()
    try:
        order = db.execute(
            select(Order).filter(Order.id == order_id)
        ).scalar_one_or_none()
        
        if order:
            order.status = OrderStatus.PAID
            db.commit()
            logger.info(f"Order {order_id} status updated to PAID")
        else:
            logger.warning(f"Order not found: {order_id}")
            
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating order status: {str(e)}")
    finally:
        db.close()


def handle_payment_failed(event_data: dict):
    """Handle payment_failed event from Payment Service."""
    order_id = event_data.get("order_id")
    reason = event_data.get("reason", "Unknown")
    
    logger.info(f"Processing payment_failed for order: {order_id}, reason: {reason}")
    
    db = get_sync_db_session()
    try:
        order = db.execute(
            select(Order).filter(Order.id == order_id)
        ).scalar_one_or_none()
        
        if order:
            order.status = OrderStatus.FAILED
            db.commit()
            logger.info(f"Order {order_id} status updated to FAILED")
        else:
            logger.warning(f"Order not found: {order_id}")
            
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating order status: {str(e)}")
    finally:
        db.close()


def handle_stock_reserved(event_data: dict):
    """
    Handle stock_reserved event from Product Service.
    This confirms that stock has been successfully reserved.
    """
    correlation_id = event_data.get("correlation_id")
    order_id = event_data.get("order_id")
    total_amount = event_data.get("total_amount")
    
    logger.info(f"Processing stock_reserved for order: {order_id}, correlation_id: {correlation_id}")
    
    db = get_sync_db_session()
    try:
        order = db.execute(
            select(Order).filter(Order.id == order_id)
        ).scalar_one_or_none()
        
        if order:
            # Update order with total amount and confirm status
            order.total_amount = total_amount
            order.status = OrderStatus.PENDING  # Ready for payment
            db.commit()
            
            logger.info(f"Order {order_id} confirmed with total_amount: {total_amount}")
            
            # Publish order_created event
            publish_order_created({
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
        db.rollback()
        logger.error(f"Error confirming order: {str(e)}")
    finally:
        db.close()


def handle_stock_reservation_failed(event_data: dict):
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
    
    db = get_sync_db_session()
    try:
        order = db.execute(
            select(Order).filter(Order.id == order_id)
        ).scalar_one_or_none()
        
        if order:
            order.status = OrderStatus.FAILED
            db.commit()
            logger.info(f"Order {order_id} marked as FAILED due to stock reservation failure")
        else:
            logger.warning(f"Order not found: {order_id}")
            
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating order status: {str(e)}")
    finally:
        db.close()


def start_payment_event_consumer():
    """Start the Kafka consumer for payment, stock, and product events."""
    logger.info("Starting Order Service Kafka Consumer...")
    
    consumer = KafkaConsumer(
        "payment-events",
        "stock-events",
        "product-events",  # Added product events
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    logger.info("Listening for payment events, stock events, and product events...")
    
    for message in consumer:
        try:
            event_data = message.value
            event_type = event_data.get("event_type")
            
            logger.info(f"Received event from topic '{message.topic}': {event_type}")
            
            # Payment events
            if event_type == "payment_completed":
                handle_payment_completed(event_data)
            elif event_type == "payment_failed":
                handle_payment_failed(event_data)
            # Stock events
            elif event_type == "stock_reserved":
                handle_stock_reserved(event_data)
            elif event_type == "stock_reservation_failed":
                handle_stock_reservation_failed(event_data)
            # Product events
            elif event_type == "product_created":
                from app.kafka_handlers.product_events import handle_product_created
                db = get_sync_db_session()
                try:
                    handle_product_created(event_data, db)
                finally:
                    db.close()
            elif event_type == "product_updated":
                from app.kafka_handlers.product_events import handle_product_updated
                db = get_sync_db_session()
                try:
                    handle_product_updated(event_data, db)
                finally:
                    db.close()
            elif event_type == "product_deleted":
                from app.kafka_handlers.product_events import handle_product_deleted
                db = get_sync_db_session()
                try:
                    handle_product_deleted(event_data, db)
                finally:
                    db.close()
            else:
                logger.warning(f"Unknown event type: {event_type}")
                
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")


if __name__ == "__main__":
    start_payment_event_consumer()
