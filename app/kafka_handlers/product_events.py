"""
Async Kafka event handlers for product events.
Handles product_created, product_updated, product_deleted events.
"""
from sqlalchemy import select, delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from ..models import ProductSellerCache
from ..cache import redis_client
from ..logger import logger


async def handle_product_created(event_data: dict, db: AsyncSession):
    """
    Handle product_created event from Product Service.
    Updates both PostgreSQL cache and Redis.
    """
    product_id = event_data.get("product_id")
    seller_id = event_data.get("seller_id")
    name = event_data.get("name")
    price = event_data.get("price")
    stock = event_data.get("stock")
    
    logger.info(f"Processing product_created event: product_id={product_id}, seller_id={seller_id}")
    
    try:
        # Upsert into PostgreSQL cache
        stmt = insert(ProductSellerCache).values(
            product_id=product_id,
            seller_id=seller_id,
            product_name=name
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['product_id'],
            set_={
                'seller_id': seller_id,
                'product_name': name
            }
        )
        await db.execute(stmt)
        await db.commit()
        
        # Cache in Redis with full product data
        product_data = {
            'id': product_id,
            'seller_id': str(seller_id),
            'name': name,
            'price': price,
            'stock': stock
        }
        redis_client.set_product(product_id, product_data)
        
        logger.info(f"Product {product_id} cached successfully for seller {seller_id} (PostgreSQL + Redis)")
        
    except Exception as e:
        await db.rollback()
        logger.error(f"Error caching product {product_id}: {str(e)}")
        raise


async def handle_product_updated(event_data: dict, db: AsyncSession):
    """
    Handle product_updated event from Product Service.
    Updates both PostgreSQL cache and Redis.
    """
    # Same logic as product_created (upsert)
    await handle_product_created(event_data, db)


async def handle_product_deleted(event_data: dict, db: AsyncSession):
    """
    Handle product_deleted event from Product Service.
    Removes from both PostgreSQL cache and Redis.
    """
    product_id = event_data.get("product_id")
    
    logger.info(f"Processing product_deleted event: product_id={product_id}")
    
    try:
        # Remove from PostgreSQL
        await db.execute(
            delete(ProductSellerCache).where(
                ProductSellerCache.product_id == product_id
            )
        )
        await db.commit()
        
        # Remove from Redis
        redis_client.delete_product(product_id)
        
        logger.info(f"Product {product_id} removed from cache (PostgreSQL + Redis)")
        
    except Exception as e:
        await db.rollback()
        logger.error(f"Error removing product {product_id} from cache: {str(e)}")
        raise
