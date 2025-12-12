from uuid import UUID
from typing import List
from fastapi import APIRouter, Depends, status, HTTPException

from ..schemas.order import OrderCreate, OrderResponse, OrderStatusUpdate
from ..services import OrderService
from ..repository import OrderRepository
from ..dependencies import get_order_service, get_order_repository, get_current_user_id, get_current_user_role

router = APIRouter(tags=["Orders"])


@router.post(
    "/orders",
    response_model=OrderResponse,
    status_code=status.HTTP_201_CREATED
)
async def create_order(
    order_data: OrderCreate,
    user_id: UUID = Depends(get_current_user_id),
    user_role: str = Depends(get_current_user_role),
    order_service: OrderService = Depends(get_order_service)
):
    """
    Create a new order.
    
    Only buyers can create orders. Sellers must use a buyer account to purchase.
    
    This endpoint:
    1. Fetches product details from Product service
    2. Validates stock availability
    3. Creates the order in database
    4. Decrements product stock
    5. Publishes order_created event to Kafka
    
    User ID is extracted from X-User-Id header (set by Nginx after token validation).
    """
    return await order_service.create_order(user_id, order_data, user_role=user_role)


@router.get(
    "/orders",
    response_model=List[OrderResponse]
)
async def get_orders(
    skip: int = 0,
    limit: int = 100,
    user_id: UUID = Depends(get_current_user_id),
    order_service: OrderService = Depends(get_order_service)
):
    """
    Get all orders for the authenticated user with pagination.
    
    User ID is extracted from X-User-Id header (set by Nginx after token validation).
    """
    return await order_service.get_user_orders(user_id, skip, limit)


@router.get(
    "/orders/seller",
    response_model=List[OrderResponse]
)
async def get_seller_orders_endpoint(
    skip: int = 0,
    limit: int = 100,
    user_id: UUID = Depends(get_current_user_id),
    user_role: str = Depends(get_current_user_role),
    order_repository: OrderRepository = Depends(get_order_repository)
):
    """
    Get all orders for products owned by the seller.
    
    Only accessible by users with 'seller' role.
    """
    if user_role != "seller":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only sellers can access this endpoint"
        )
    
    from ..services.seller_orders import get_seller_orders
    return await get_seller_orders(user_id, order_repository, skip, limit)


@router.get(
    "/orders/{order_id}",
    response_model=OrderResponse
)
async def get_order(
    order_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
    order_service: OrderService = Depends(get_order_service)
):
    """Get a specific order by ID."""
    return await order_service.get_order(order_id, user_id)


@router.patch(
    "/orders/{order_id}/status",
    response_model=OrderResponse
)
async def update_order_status(
    order_id: UUID,
    status_update: OrderStatusUpdate,
    user_id: UUID = Depends(get_current_user_id),
    user_role: str = Depends(get_current_user_role),
    order_service: OrderService = Depends(get_order_service)
):
    """
    Update order status.
    
    Only admins and sellers can update order status.
    Publishes order_status_updated event to Kafka.
    """
    return await order_service.update_order_status(order_id, status_update.status, user_id)


@router.post(
    "/orders/{order_id}/approve",
    response_model=OrderResponse
)
async def approve_order(
    order_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
    user_role: str = Depends(get_current_user_role),
    order_repository: OrderRepository = Depends(get_order_repository)
):
    """
    Approve an order (seller only).
    
    This triggers stock reservation and moves the order to PENDING status.
    Only the seller who owns the product can approve the order.
    """
    from ..services.seller_approval import approve_order as approve_order_service
    
    if user_role != "seller":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only sellers can approve orders"
        )
    
    return await approve_order_service(order_id, user_id, order_repository)


@router.post(
    "/orders/{order_id}/reject",
    response_model=OrderResponse
)
async def reject_order(
    order_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
    user_role: str = Depends(get_current_user_role),
    order_repository: OrderRepository = Depends(get_order_repository)
):
    """
    Reject an order (seller only).
    
    Moves the order to CANCELLED status without reserving stock.
    Only the seller who owns the product can reject the order.
    """
    from ..services.seller_approval import reject_order as reject_order_service
    
    if user_role != "seller":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only sellers can reject orders"
        )
    
    return await reject_order_service(order_id, user_id, order_repository)


@router.delete(
    "/orders/{order_id}",
    status_code=status.HTTP_200_OK
)
async def cancel_order(
    order_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
    order_service: OrderService = Depends(get_order_service)
):
    """
    Cancel an order.
    
    Only pending or payment_pending orders can be cancelled.
    Stock is automatically restored when an order is cancelled.
    """
    return await order_service.cancel_order(order_id, user_id)

