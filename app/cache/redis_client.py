"""
Redis client for caching product data.
"""
import redis
import json
from typing import Optional, Dict
from ..settings import settings
from ..logger import logger


class RedisClient:
    """Redis client for product caching."""
    
    def __init__(self):
        self.redis = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=0,
            decode_responses=True
        )
        self.default_ttl = 300  # 5 minutes
    
    def get_product(self, product_id: int) -> Optional[Dict]:
        """Get product from Redis cache."""
        try:
            data = self.redis.get(f"product:{product_id}")
            if data:
                logger.info(f"Redis cache HIT for product {product_id}")
                return json.loads(data)
            logger.info(f"Redis cache MISS for product {product_id}")
            return None
        except Exception as e:
            logger.error(f"Redis get error: {str(e)}")
            return None
    
    def set_product(self, product_id: int, data: Dict, ttl: Optional[int] = None):
        """Set product in Redis cache with TTL."""
        try:
            ttl = ttl or self.default_ttl
            self.redis.setex(
                f"product:{product_id}",
                ttl,
                json.dumps(data)
            )
            logger.info(f"Product {product_id} cached in Redis (TTL: {ttl}s)")
        except Exception as e:
            logger.error(f"Redis set error: {str(e)}")
    
    def delete_product(self, product_id: int):
        """Delete product from Redis cache."""
        try:
            self.redis.delete(f"product:{product_id}")
            logger.info(f"Product {product_id} removed from Redis cache")
        except Exception as e:
            logger.error(f"Redis delete error: {str(e)}")
    
    def ping(self) -> bool:
        """Check Redis connection."""
        try:
            return self.redis.ping()
        except Exception as e:
            logger.error(f"Redis ping failed: {str(e)}")
            return False


# Global Redis client instance
redis_client = RedisClient()
