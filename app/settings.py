from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Order Service configuration settings."""
    
    DB_HOST: str = "order-db"
    DB_PORT: int = 5432
    DB_NAME: str = "order_db"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    
    # Redis connection parameters
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    
    # Backward compatibility: if DATABASE_URL is provided, it takes precedence
    # DATABASE_URL: str | None = None
    
    PRODUCT_API_URL: str = "http://product-api:8000"
    
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_CONSUMER_GROUP_ID: str = "order-service-group"
    
    SAGA_TIMEOUT_SECONDS: int = 30

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    @computed_field
    @property
    def database_url(self) -> str:
        """Construct database URL from individual parameters or use provided URL."""
        # if self.DATABASE_URL:
        #     return self.DATABASE_URL
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


settings = Settings()
