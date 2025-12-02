import uuid
from typing import Optional
from pydantic import BaseModel, Field


class OrderCreate(BaseModel):
    """Schema for creating a new order."""
    product_id: int = Field(..., gt=0, description="Product ID")
    quantity: int = Field(..., gt=0, description="Quantity to order")


class OrderResponse(BaseModel):
    """Schema for order response."""
    id: uuid.UUID
    user_id: uuid.UUID
    product_id: int
    quantity: int
    total_amount: float
    status: str
    
    class Config:
        from_attributes = True


class OrderStatusUpdate(BaseModel):
    """Schema for updating order status."""
    status: str = Field(..., description="New order status")