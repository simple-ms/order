#!/usr/bin/env python3
"""
Kafka consumer runner for Order Service.
This script starts the async Kafka consumer to listen for payment events.
"""

import sys
import signal
import logging
import asyncio
from app.kafka.consumer import start_payment_event_consumer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    """Handle shutdown signals gracefully."""
    logger.info("Received shutdown signal, stopping consumer...")
    sys.exit(0)


if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("Starting Order Service Async Kafka Consumer...")
    logger.info("Listening for payment events...")
    
    try:
        asyncio.run(start_payment_event_consumer())
    except Exception as e:
        logger.error(f"Consumer error: {str(e)}")
        sys.exit(1)
