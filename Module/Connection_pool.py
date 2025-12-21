"""
Simple SQL Connection Pool using SQLAlchemy

Usage:
    from Module.Connection_pool import Engine
    
    # Use the engine in your code
    with Engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM table"))
"""

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
import os

# Database connection configuration from environment variables
# Example connection string: mssql+pyodbc://user:pass@server/database?driver=ODBC+Driver+17+for+SQL+Server
DB_CONNECTION_STRING = os.getenv(
    "DB_CONNECTION_STRING",
    "mssql+pyodbc://localhost/YourDatabase?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

# Pool configuration
POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "10"))
POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "3600"))

# Create the SQLAlchemy engine with connection pooling
Engine = create_engine(
    DB_CONNECTION_STRING,
    poolclass=QueuePool,
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    pool_timeout=POOL_TIMEOUT,
    pool_recycle=POOL_RECYCLE,
    pool_pre_ping=True,  # Verify connections before using them
    echo=False,  # Set to True for SQL query debugging
)

print(f"Database connection pool initialized:")
print(f"  - Pool size: {POOL_SIZE}")
print(f"  - Max overflow: {MAX_OVERFLOW}")
print(f"  - Pool timeout: {POOL_TIMEOUT}s")
print(f"  - Pool recycle: {POOL_RECYCLE}s")
