from fastapi import Header, HTTPException, status
from .logger import logger


async def get_current_user_id(
    x_user_id: str = Header(None, alias="X-User-Id", include_in_schema=False)
) -> str:
    """
    Extract and validate user ID from X-User-Id header.
    
    This header is automatically set by Nginx after JWT validation.
    Users don't need to provide this manually.
    
    Args:
        x_user_id: User ID from request header (auto-injected by Nginx)
        
    Returns:
        User ID string
        
    Raises:
        HTTPException: If X-User-Id header is missing
    """
    if not x_user_id:
        logger.warning("Request failed: Missing X-User-Id header")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized - Missing user identification"
        )
    
    return x_user_id


async def get_current_user_role(
    x_user_role: str = Header(None, alias="X-User-Role", include_in_schema=False)
) -> str:
    """
    Extract user role from X-User-Role header.
    
    This header is automatically set by Nginx after JWT validation.
    Users don't need to provide this manually.
    
    Args:
        x_user_role: User role from request header (auto-injected by Nginx)
        
    Returns:
        User role string (defaults to 'user' if not provided)
    """
    return x_user_role or "user"
