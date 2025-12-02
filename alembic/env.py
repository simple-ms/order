from logging.config import fileConfig
from sqlalchemy.ext.asyncio import async_engine_from_config
from sqlalchemy import pool
import asyncio
import os
import sys

from alembic import context

# --------------------------------------------------------
# 1. Add your app directory to sys.path so Alembic can find models
# --------------------------------------------------------
sys.path.append(os.getcwd())

# --------------------------------------------------------
# 2. Import Base and DATABASE_URL from your app
# --------------------------------------------------------
from app.database import Base          # Your declarative Base
from app.config import DATABASE_URL    # Loaded from env/Docker
from app import models                 # Ensure models are imported

# ----------------------------------------------------------------
# Alembic Config
# ----------------------------------------------------------------
config = context.config

# --------------------------------------------------------
# 3. Overwrite SQLAlchemy URL using your Python config
# --------------------------------------------------------
config.set_main_option("sqlalchemy.url", DATABASE_URL)

# --------------------------------------------------------
# 4. Logging config
# --------------------------------------------------------
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# --------------------------------------------------------
# 5. Set metadata for autogenerate
# --------------------------------------------------------
target_metadata = Base.metadata

# ----------------------------------------------------------------
# Migration Functions
# ----------------------------------------------------------------

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")

    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations in 'online' mode with async engine."""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(
            lambda sync_conn: context.configure(
                connection=sync_conn,
                target_metadata=target_metadata
            )
        )
        async with connection.begin():
            await connection.run_sync(lambda _: context.run_migrations())


# ----------------------------------------------------------------

if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())