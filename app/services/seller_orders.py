"""
Seller Order Service - Get orders for seller's products

Uses ProductSellerCache (populated via Kafka events) instead of HTTP calls
to Product service for better performance and decoupling.
"""
from uuid import UUID
from typing import List
from fastapi import HTTPException, status
from sqlalchemy import select

from ..models.order import Order
from ..models.product_seller_cache import ProductSellerCache
from ..repository import OrderRepository
from ..logger import logger


async def get_seller_orders(
    seller_id: UUID,
    order_repository: OrderRepository,
    skip: int = 0,
    limit: int = 100
) -> List[Order]:
    """
    Get all orders for products owned by a seller.
    
    Uses ProductSellerCache (populated via Kafka product events) to determine
    which products belong to the seller, then queries orders for those products.
    
    This eliminates the need for HTTP calls to the Product service.
    """
    logger.info(f"Fetching orders for seller {seller_id}")
    
    try:
        # Query ProductSellerCache to get seller's product IDs
        result = await order_repository.db.execute(
            select(ProductSellerCache.product_id)
            .filter(ProductSellerCache.seller_id == seller_id)
        )
        
        product_ids = [row[0] for row in result.fetchall()]
        
        if not product_ids:
            logger.info(f"No products found for seller {seller_id} in cache")
            return []
        
        logger.info(f"Found {len(product_ids)} products for seller {seller_id}")
        
        # Query orders for these product IDs
        orders_result = await order_repository.db.execute(
            select(Order)
            .filter(Order.product_id.in_(product_ids))
            .order_by(Order.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        
        orders = list(orders_result.scalars().all())
        logger.info(f"Found {len(orders)} orders for seller {seller_id}")
        return orders
        
    except Exception as e:
        logger.error(f"Error fetching seller orders: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch seller orders"
        )
