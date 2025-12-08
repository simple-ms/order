"""
Async Kafka producer for Order Service.
Publishes order events and stock reservation requests using aiokafka.
"""
import json
from typing import Dict, Any, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from ..settings import settings
from ..logger import logger


class AsyncKafkaProducerClient:
    """Async Kafka producer client for publishing events."""
    
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self._started = False
    
    async def start(self):
        """Initialize and start Kafka producer connection."""
        if self._started:
            return
        
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                enable_idempotence=True,
                max_in_flight_requests_per_connection=1
            )
            await self.producer.start()
            self._started = True
            logger.info(f"Async Kafka producer connected to {settings.KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.error(f"Failed to start async Kafka producer: {str(e)}")
            self.producer = None
            self._started = False
    
    async def stop(self):
        """Stop Kafka producer connection."""
        if self.producer and self._started:
            await self.producer.stop()
            self._started = False
            logger.info("Async Kafka producer stopped")
    
    async def send_event(self, topic: str, event_data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """Send an event to Kafka topic."""
        if not self.producer or not self._started:
            await self.start()
        
        if not self.producer:
            logger.error("Kafka producer not initialized")
            return False
        
        try:
            metadata = await self.producer.send_and_wait(topic, value=event_data, key=key)
            logger.info(
                f"Event sent to topic '{topic}': "
                f"partition={metadata.partition}, offset={metadata.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Failed to send event to topic '{topic}': {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending event: {str(e)}")
            return False


# Global producer instance
kafka_producer = AsyncKafkaProducerClient()


# --- Stock Reservation Events ---

async def publish_stock_reservation_request(reservation_data: Dict[str, Any]) -> bool:
    """
    Publish stock_reservation_request event to Product Service.
    
    Args:
        reservation_data: Contains correlation_id, user_id, product_id, quantity, order_id
    """
    event = {
        "event_type": "stock_reservation_request",
        "correlation_id": reservation_data["correlation_id"],
        "order_id": str(reservation_data["order_id"]),
        "user_id": str(reservation_data["user_id"]),
        "product_id": reservation_data["product_id"],
        "quantity": reservation_data["quantity"]
    }
    return await kafka_producer.send_event(
        topic="stock-reservation-requests",
        event_data=event,
        key=reservation_data["correlation_id"]
    )


async def publish_stock_restoration_request(order_data: Dict[str, Any]) -> bool:
    """
    Publish stock_restoration_request event when order is cancelled.
    
    Args:
        order_data: Contains order_id, product_id, quantity
    """
    event = {
        "event_type": "stock_restoration_request",
        "order_id": str(order_data["order_id"]),
        "product_id": order_data["product_id"],
        "quantity": order_data["quantity"]
    }
    return await kafka_producer.send_event(
        topic="stock-reservation-requests",
        event_data=event,
        key=str(order_data["order_id"])
    )


# --- Order Events ---

async def publish_order_created(order_data: Dict[str, Any]) -> bool:
    """Publish order_created event."""
    event = {
        "event_type": "order_created",
        "order_id": str(order_data["order_id"]),
        "user_id": str(order_data["user_id"]),
        "product_id": order_data["product_id"],
        "quantity": order_data["quantity"],
        "total_amount": order_data["total_amount"],
        "status": order_data["status"]
    }
    return await kafka_producer.send_event(
        topic="order-events",
        event_data=event,
        key=str(order_data["order_id"])
    )


async def publish_order_status_updated(order_id: str, status: str, user_id: str) -> bool:
    """Publish order_status_updated event."""
    event = {
        "event_type": "order_status_updated",
        "order_id": order_id,
        "user_id": user_id,
        "status": status
    }
    return await kafka_producer.send_event(
        topic="order-events",
        event_data=event,
        key=order_id
    )
