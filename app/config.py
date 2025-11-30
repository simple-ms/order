import os

DATABASE_URL = os.getenv("DATABASE_URL")

PRODUCT_API_URL = os.getenv("PRODUCT_API_URL")

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM")

ISSUER = os.getenv("ISSUER")
AUDIENCE = os.getenv("AUDIENCE")
