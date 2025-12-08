"""
Kafka integration package for Order Service.
Provides async Kafka producer and consumer functionality.
"""
from .producer import (
    kafka_producer,
    publish_stock_reservation_request,
    publish_stock_restoration_request,
    publish_order_created,
    publish_order_status_updated
)
from .consumer import start_payment_event_consumer

__all__ = [
    "kafka_producer",
    "publish_stock_reservation_request",
    "publish_stock_restoration_request",
    "publish_order_created",
    "publish_order_status_updated",
    "start_payment_event_consumer"
]
