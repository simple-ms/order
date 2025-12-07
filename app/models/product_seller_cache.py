import uuid
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import String, Integer, DateTime, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from ..database import Base


class ProductSellerCache(Base):
    """
    Cache table for product-seller mappings.
    
    This table is populated and updated via Kafka events from the Product service.
    It allows the Order service to quickly look up which products belong to which sellers
    without making HTTP calls to the Product service.
    """
    
    __tablename__ = "product_seller_cache"

    product_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    seller_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    product_name: Mapped[str] = mapped_column(String(255))
    
    # Timestamp for cache invalidation
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )
    
    # Index for efficient seller queries
    __table_args__ = (
        Index('idx_product_seller_cache_seller', 'seller_id'),
    )
    
    def __repr__(self) -> str:
        return f"<ProductSellerCache(product_id={self.product_id}, seller_id={self.seller_id})>"
