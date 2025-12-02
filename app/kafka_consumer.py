import json
import logging
import asyncio
from typing import Callable, Dict, Any
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from sqlalchemy import select
from .config import KAFKA_BOOTSTRAP_SERVERS
from .database import AsyncSessionLocal
from .models import Order, OrderStatus

logger = logging.getLogger("order-service")


class KafkaConsumerClient:
    """Kafka consumer client for consuming payment events."""
    
    def __init__(self, topic: str, group_id: str):
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self._connect()
    
    def _connect(self):
        """Initialize Kafka consumer connection."""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',  # Start from beginning if no offset
                enable_auto_commit=True,
                auto_commit_interval_ms=1000
            )
            logger.info(f"Kafka consumer connected to topic '{self.topic}' with group '{self.group_id}'")
        except Exception as e:
            logger.error(f"Failed to connect Kafka consumer: {str(e)}")
            self.consumer = None
    
    def consume(self, handler: Callable[[Dict[str, Any]], None]):
        """
        Start consuming messages and process with handler.
        
        Args:
            handler: Async function to process each message
        """
        if not self.consumer:
            logger.error("Kafka consumer not initialized")
            return
        
        logger.info(f"Starting to consume messages from topic '{self.topic}'")
        
        try:
            for message in self.consumer:
                try:
                    logger.info(f"Received message from partition {message.partition}, offset {message.offset}")
                    # Run async handler in event loop
                    asyncio.run(handler(message.value))
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
        except Exception as e:
            logger.error(f"Consumer error: {str(e)}")
        finally:
            self.close()
    
    def close(self):
        """Close Kafka consumer connection."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")


async def handle_payment_event(event: Dict[str, Any]):
    """
    Handle payment events and update order status accordingly.
    
    Args:
        event: Payment event data
    """
    event_type = event.get("event_type")
    order_id = event.get("order_id")
    
    if not order_id:
        logger.warning(f"Received payment event without order_id: {event}")
        return
    
    logger.info(f"Processing payment event: {event_type} for order {order_id}")
    
    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(select(Order).filter(Order.id == order_id))
            order = result.scalar_one_or_none()
            
            if not order:
                logger.warning(f"Order not found: {order_id}")
                return
            
            if event_type == "payment_completed":
                order.status = OrderStatus.PAID
                logger.info(f"Order {order_id} marked as PAID")
            elif event_type == "payment_failed":
                order.status = OrderStatus.FAILED
                logger.info(f"Order {order_id} marked as FAILED")
            else:
                logger.warning(f"Unknown payment event type: {event_type}")
                return
            
            await db.commit()
            logger.info(f"Order {order_id} status updated to {order.status}")
            
        except Exception as e:
            await db.rollback()
            logger.error(f"Error updating order status: {str(e)}")


def start_payment_event_consumer():
    """Start consuming payment events."""
    consumer = KafkaConsumerClient(
        topic="payment-events",
        group_id="order-service-group"
    )
    consumer.consume(handle_payment_event)
