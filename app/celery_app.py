"""
Celery application configuration for Order Service.
Handles background tasks like product cache synchronization.
"""
from celery import Celery
from .settings import settings

# Initialize Celery app
celery_app = Celery(
    'order-service',
    broker=f'redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/0',
    backend=f'redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/0'
)

# Celery configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=300,  # 5 minutes max
    task_soft_time_limit=240,  # 4 minutes soft limit
    worker_prefetch_multiplier=4,
    worker_max_tasks_per_child=1000,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_default_queue='order-tasks',  # Dedicated queue for order service
    task_default_routing_key='order-tasks',
)

# Periodic task schedule
celery_app.conf.beat_schedule = {
    'sync-product-cache-redis': {
        'task': 'app.tasks.sync_tasks.sync_product_cache_to_redis',
        'schedule': 30.0,  # Every 30 seconds
    },
}

# Auto-discover tasks
celery_app.autodiscover_tasks(['app.tasks'])
