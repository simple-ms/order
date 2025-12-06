"""
Seller Order Service - Get orders for seller's products

This requires cross-service communication to get seller's product IDs
"""
import httpx
from uuid import UUID
from typing import List
from fastapi import HTTPException, status

from ..models.order import Order
from ..repository import OrderRepository
from ..logger import logger
from ..settings import settings


async def get_seller_orders(
    seller_id: UUID,
    order_repository: OrderRepository,
    skip: int = 0,
    limit: int = 100
) -> List[Order]:
    """
    Get all orders for products owned by a seller.
    
    This requires:
    1. Fetch seller's product IDs from Product service
    2. Query orders for those product IDs
    """
    logger.info(f"Fetching orders for seller {seller_id}")
    
    try:
        # Call Product service to get seller's product IDs
        product_api_url = getattr(settings, 'PRODUCT_API_URL', 'http://product-api:8000')
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{product_api_url}/products/seller/{seller_id}",
                timeout=5.0
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to fetch seller products: {response.status_code}")
                return []
            
            products = response.json()
            product_ids = [p['id'] for p in products]
            
            if not product_ids:
                logger.info(f"No products found for seller {seller_id}")
                return []
        
        # Query orders for these product IDs
        from sqlalchemy import select
        result = await order_repository.db.execute(
            select(Order)
            .filter(Order.product_id.in_(product_ids))
            .order_by(Order.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        
        orders = list(result.scalars().all())
        logger.info(f"Found {len(orders)} orders for seller {seller_id}")
        return orders
        
    except httpx.TimeoutException:
        logger.error("Timeout calling Product service")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Product service unavailable"
        )
    except Exception as e:
        logger.error(f"Error fetching seller orders: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch seller orders"
        )
