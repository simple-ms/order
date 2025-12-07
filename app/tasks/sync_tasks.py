"""
Background tasks for Order Service.
"""
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from ..celery_app import celery_app
from ..models import ProductSellerCache
from ..cache import redis_client
from ..settings import settings
from ..logger import logger


# Synchronous database URL for Celery tasks
SYNC_DATABASE_URL = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
sync_engine = create_engine(SYNC_DATABASE_URL)


@celery_app.task(name='app.tasks.sync_tasks.sync_product_cache_to_redis')
def sync_product_cache_to_redis():
    """
    Periodic sync task to ensure Redis cache consistency.
    Refreshes Redis cache from PostgreSQL every 30 seconds.
    """
    logger.info("Starting product cache sync to Redis")
    
    db = Session(sync_engine)
    try:
        # Get all products from PostgreSQL cache
        result = db.execute(
            select(ProductSellerCache).limit(1000)
        )
        products = result.scalars().all()
        
        synced_count = 0
        for product in products:
            # Refresh Redis cache from PostgreSQL
            product_data = {
                'id': product.product_id,
                'seller_id': str(product.seller_id),
                'name': product.product_name,
                # Note: price and stock not stored in ProductSellerCache
                # They come from product events
            }
            redis_client.set_product(product.product_id, product_data)
            synced_count += 1
        
        logger.info(f"Product cache sync completed: {synced_count} products synced to Redis")
        
        return {
            'task': 'sync_product_cache_to_redis',
            'synced_count': synced_count
        }
        
    except Exception as e:
        logger.error(f"Error during product cache sync: {str(e)}")
        raise
    finally:
        db.close()
