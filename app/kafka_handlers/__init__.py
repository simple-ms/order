"""
Kafka event handlers for Product events in Order Service.

Listens to product lifecycle events (created, updated, deleted) from Product service
and maintains a local cache of product-seller mappings in ProductSellerCache table.
"""
from sqlalchemy.orm import Session
from sqlalchemy import select
from uuid import UUID

from ..models import ProductSellerCache
from ..logger import logger


def handle_product_created(event_data: dict, db: Session):
    """
    Handle product_created event from Product Service.
    
    Creates a new entry in ProductSellerCache to track which seller owns this product.
    This allows us to query seller's products without calling Product service.
    
    Args:
        event_data: Contains product_id, seller_id, name, price, stock
        db: Database session
    """
    product_id = event_data.get("product_id")
    seller_id = event_data.get("seller_id")
    name = event_data.get("name")
    
    logger.info(f"Processing product_created event: product_id={product_id}, seller_id={seller_id}")
    
    try:
        # Check if already exists (idempotency)
        existing = db.execute(
            select(ProductSellerCache).filter(ProductSellerCache.product_id == product_id)
        ).scalar_one_or_none()
        
        if existing:
            logger.warning(f"Product {product_id} already exists in cache, updating instead")
            existing.seller_id = UUID(seller_id)
            existing.product_name = name
        else:
            # Create new cache entry
            cache_entry = ProductSellerCache(
                product_id=product_id,
                seller_id=UUID(seller_id),
                product_name=name
            )
            db.add(cache_entry)
        
        db.commit()
        logger.info(f"Product {product_id} added to seller cache for seller {seller_id}")
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error handling product_created event: {str(e)}")


def handle_product_updated(event_data: dict, db: Session):
    """
    Handle product_updated event from Product Service.
    
    Updates the ProductSellerCache entry for this product.
    
    Args:
        event_data: Contains product_id, seller_id, name, price, stock
        db: Database session
    """
    product_id = event_data.get("product_id")
    seller_id = event_data.get("seller_id")
    name = event_data.get("name")
    
    logger.info(f"Processing product_updated event: product_id={product_id}")
    
    try:
        cache_entry = db.execute(
            select(ProductSellerCache).filter(ProductSellerCache.product_id == product_id)
        ).scalar_one_or_none()
        
        if cache_entry:
            cache_entry.seller_id = UUID(seller_id)
            cache_entry.product_name = name
            db.commit()
            logger.info(f"Product {product_id} updated in seller cache")
        else:
            logger.warning(f"Product {product_id} not found in cache, creating new entry")
            # Create if doesn't exist (handle out-of-order events)
            handle_product_created(event_data, db)
            
    except Exception as e:
        db.rollback()
        logger.error(f"Error handling product_updated event: {str(e)}")


def handle_product_deleted(event_data: dict, db: Session):
    """
    Handle product_deleted event from Product Service.
    
    Removes the product from ProductSellerCache.
    
    Args:
        event_data: Contains product_id, seller_id
        db: Database session
    """
    product_id = event_data.get("product_id")
    
    logger.info(f"Processing product_deleted event: product_id={product_id}")
    
    try:
        cache_entry = db.execute(
            select(ProductSellerCache).filter(ProductSellerCache.product_id == product_id)
        ).scalar_one_or_none()
        
        if cache_entry:
            db.delete(cache_entry)
            db.commit()
            logger.info(f"Product {product_id} removed from seller cache")
        else:
            logger.warning(f"Product {product_id} not found in cache (already deleted or never cached)")
            
    except Exception as e:
        db.rollback()
        logger.error(f"Error handling product_deleted event: {str(e)}")
