from uuid import UUID
from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from .database import get_db
from .repository import OrderRepository
from .services import OrderService


async def get_order_repository(db: AsyncSession = Depends(get_db)) -> OrderRepository:
    """Dependency to get OrderRepository instance."""
    return OrderRepository(db)


async def get_order_service(
    order_repository: OrderRepository = Depends(get_order_repository)
) -> OrderService:
    """Dependency to get OrderService instance."""
    return OrderService(order_repository)


async def get_current_user_id(x_user_id: str = Header(..., alias="X-User-Id")) -> UUID:
    """
    Extract user ID from X-User-Id header set by Nginx after token validation.
    """
    try:
        return UUID(x_user_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user ID in header"
        )


async def get_current_user_role(
    x_user_role: str = Header(..., alias="X-User-Role")
) -> str:
    """
    Extract user role from X-User-Role header set by Nginx after token validation.
    """
    return x_user_role
