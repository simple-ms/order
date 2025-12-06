from uuid import UUID
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..models.order import Order


class OrderRepository:
    """Repository for Order database operations."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def get_by_id(self, order_id: UUID) -> Order | None:
        """Get order by ID."""
        result = await self.db.execute(select(Order).filter(Order.id == order_id))
        return result.scalar_one_or_none()
    
    async def get_by_id_and_user(self, order_id: UUID, user_id: UUID) -> Order | None:
        """Get order by ID for a specific user."""
        result = await self.db.execute(
            select(Order).filter(
                Order.id == order_id,
                Order.user_id == user_id
            )
        )
        return result.scalar_one_or_none()
    
    async def get_all_by_user(
        self, 
        user_id: UUID, 
        skip: int = 0, 
        limit: int = 100
    ) -> List[Order]:
        """Get all orders for a user with pagination."""
        result = await self.db.execute(
            select(Order)
            .filter(Order.user_id == user_id)
            .order_by(Order.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        return list(result.scalars().all())
    
    async def get_orders_by_product_seller(
        self,
        seller_id: UUID,
        skip: int = 0,
        limit: int = 100
    ) -> List[Order]:
        """Get all orders for products owned by a seller.
        
        This requires joining with the product table to check seller_id.
        Since we don't have direct access to Product model here, we'll need
        to pass product_ids from the service layer.
        """
        # This method will be called from service after fetching seller's product IDs
        pass
    
    async def create(self, order: Order) -> Order:
        """Create a new order."""
        self.db.add(order)
        await self.db.commit()
        await self.db.refresh(order)
        return order
    
    async def update(self, order: Order) -> Order:
        """Update an existing order."""
        await self.db.commit()
        await self.db.refresh(order)
        return order
    
    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self.db.rollback()

