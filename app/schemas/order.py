import uuid
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator
from enum import Enum


class OrderStatusEnum(str, Enum):
    """Valid order status values."""
    PENDING = "pending"
    PAYMENT_PENDING = "payment_pending"
    PAID = "paid"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    FAILED = "failed"


class OrderCreate(BaseModel):
    """Schema for creating a new order."""
    product_id: int = Field(..., gt=0, description="Product ID")
    quantity: int = Field(..., gt=0, le=1000, description="Quantity to order (max 1000)")
    
    # Shipping address fields
    shipping_address: Optional[str] = Field(None, max_length=500, description="Shipping address")
    city: Optional[str] = Field(None, max_length=100, description="City")
    postal_code: Optional[str] = Field(None, max_length=20, description="Postal code")
    country: Optional[str] = Field(None, max_length=100, description="Country")
    
    @field_validator('quantity')
    @classmethod
    def validate_quantity(cls, v: int) -> int:
        """Validate quantity is reasonable."""
        if v <= 0:
            raise ValueError('Quantity must be greater than 0')
        if v > 1000:
            raise ValueError('Quantity cannot exceed 1000 items per order')
        return v


class OrderResponse(BaseModel):
    """Schema for order response."""
    id: uuid.UUID
    user_id: uuid.UUID
    product_id: int
    quantity: int
    total_amount: float
    status: str
    shipping_address: Optional[str] = None
    city: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class OrderStatusUpdate(BaseModel):
    """Schema for updating order status."""
    status: OrderStatusEnum = Field(..., description="New order status")

