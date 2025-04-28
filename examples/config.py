import os
import logging
from dotenv import load_dotenv
from typing import Optional

# --- Determine Environment and Load .env file ---

# 1. Get the application environment from ENV variable
#    Default to 'development' if not set
APP_ENV = os.getenv('APP_ENV', 'development').lower()

# 2. Construct the .env filename based on the environment
dotenv_filename = f".env.{APP_ENV}"

# 3. Check if the specific .env file exists
if os.path.exists(dotenv_filename):
    print(f"Loading environment variables from: {dotenv_filename}")
    # Load the specific file (e.g., .env.development or .env.production)
    # override=True means variables in this file take precedence over system env vars
    load_dotenv(dotenv_path=dotenv_filename, override=True)
else:
    print(f"Warning: {dotenv_filename} not found.")
    # Optionally, try loading a generic .env file as a fallback
    if os.path.exists(".env"):
        print("Loading environment variables from generic .env file.")
        load_dotenv(override=True) # Load .env if specific one not found
    else:
        print("No .env file found. Relying solely on system environment variables.")

# --- Define Configuration Variables ---
# Read variables using os.getenv(), providing defaults if necessary

# QTask Broker Configuration
# Default from client.py used if QTASK_BROKER_URL is not set anywhere
QTASK_BROKER_URL: Optional[str] = os.getenv('QTASK_BROKER_URL')

# Redis Configuration
REDIS_HOST: str = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT_STR: str = os.getenv('REDIS_PORT', '6379')
REDIS_USERNAME: Optional[str] = os.getenv('REDIS_USERNAME') # Returns None if not set
REDIS_PASSWORD: Optional[str] = os.getenv('REDIS_PASSWORD') # Returns None if not set

QTASK_BROKER_HOST: str = os.getenv('QTASK_BROKER_HOST', 'localhost')
QTASK_BROKER_PORT: str = os.getenv('QTASK_BROKER_PORT', '3000')

# Convert Redis Port to integer, handling potential errors
try:
    REDIS_PORT: int = int(REDIS_PORT_STR)
except ValueError:
    print(f"Warning: Invalid REDIS_PORT value '{REDIS_PORT_STR}'. Using default 6379.")
    REDIS_PORT = 6379

# Application/Logging Configuration (Example)
LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO').upper()

# --- Helper Function to Construct Redis URL (Optional but useful) ---
def get_redis_url() -> str:
    """Constructs the Redis URL from loaded configuration."""
    from urllib.parse import urlunparse
    netloc = ""
    user = REDIS_USERNAME
    passwd = REDIS_PASSWORD
    host = REDIS_HOST
    port = REDIS_PORT

    if user and passwd:
        netloc = f"{user}:{passwd}@{host}:{port}"
    elif user:
        netloc = f"{user}@{host}:{port}"
    else:
        if passwd:
            netloc = f":{passwd}@{host}:{port}"
        else:
            netloc = f"{host}:{port}"
    return urlunparse(("redis", netloc, "", "", "", ""))

# --- Log Loaded Configuration (Optional) ---
# Be careful not to log sensitive data like passwords in production
print("-" * 30)
print(f"Configuration loaded for environment: {APP_ENV}")
print(f"QTASK_BROKER_URL: {QTASK_BROKER_URL if QTASK_BROKER_URL else '(Not Set, client will use default)'}")
print(f"REDIS_HOST: {REDIS_HOST}")
print(f"REDIS_PORT: {REDIS_PORT}")
print(f"REDIS_USERNAME: {REDIS_USERNAME if REDIS_USERNAME else '(Not Set)'}")
print(f"REDIS_PASSWORD: {'****' if REDIS_PASSWORD else '(Not Set)'}") # Mask password
print(f"LOG_LEVEL: {LOG_LEVEL}")
print("-" * 30)

# You can add more configuration variables as needed following the same pattern.
