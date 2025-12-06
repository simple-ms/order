from fastapi import FastAPI

from .views import health_router, order_router


def register_routes(app: FastAPI) -> None:
    """Register all API routes to the FastAPI application."""
    app.include_router(health_router)
    app.include_router(order_router)

