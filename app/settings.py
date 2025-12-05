from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CORSSettings(BaseSettings):
    """CORS configuration settings."""
    CORS_ORIGINS: str = "http://localhost:3000,http://localhost:8080,http://127.0.0.1:3000,http://127.0.0.1:8080"
    CORS_ALLOW_CREDENTIALS: bool = True
    CORS_ALLOW_METHODS: str = "*"
    CORS_ALLOW_HEADERS: str = "*"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    @property
    def origins_list(self) -> list[str]:
        """Parse CORS_ORIGINS into a list."""
        return [origin.strip() for origin in self.CORS_ORIGINS.split(",") if origin.strip()]


class Settings(BaseSettings):
    # Database connection parameters
    DB_HOST: str = "order-db"
    DB_PORT: int = 5432
    DB_NAME: str = "order_db"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    
    # Backward compatibility: if DATABASE_URL is provided, it takes precedence
    DATABASE_URL: str | None = None
    
    # Service URLs
    PRODUCT_API_URL: str = "http://product-api:8000"
    
    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    @computed_field
    @property
    def database_url(self) -> str:
        """Construct database URL from individual parameters or use provided URL."""
        if self.DATABASE_URL:
            return self.DATABASE_URL
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


settings = Settings()
cors_settings = CORSSettings()
