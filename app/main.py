"""
Order Service - Order Management Microservice

This is the main entry point for the Order service.
All routes are defined in views/ and registered via routes.py
"""
from fastapi import FastAPI

from .routes import register_routes

app = FastAPI(
    title="Order Service",
    description="Order management microservice with Kafka event publishing",
    version="1.0.0",
    docs_url="/docs/order",
    openapi_url="/openapi.json/order",
    redoc_url="/redoc/order"
)

# NOTE: CORS is handled by nginx gateway - no CORS middleware here

# Register all routes
register_routes(app)
