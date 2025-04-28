# qtask-client-py
Python client library for interacting with the QTask Broker API and consuming messages from its partitioned Redis Streams.

# Usage Guide: qtask-client-py

This guide explains how to install and use the `qtask-client` library to interact with the QTask Broker from your Python applications.

## 1. Environment Setup (using `uv`)

It is strongly recommended to use a virtual environment to manage your project's dependencies.

```bash
# Navigate to your microservice's root project folder
cd path/to/your/microservice

# Create a virtual environment named '.venv' (if it doesn't exist)
# Make sure you have 'uv' installed (see [https://github.com/astral-sh/uv](https://github.com/astral-sh/uv))
uv venv

# Activate the virtual environment
# Linux/macOS (bash/zsh):
source .venv/bin/activate
# Windows (PowerShell):
# .\.venv\Scripts\Activate.ps1
# Windows (cmd.exe):
# .\.venv\Scripts\activate.bat

# Your terminal prompt should now show (.venv) at the beginning
```

## 2. Library Installation

Since the `qtask-client-py` repository is public on GitHub, you can install it directly using `uv pip`.

```bash
# Make sure your virtual environment is activated

# Install the latest version from the 'main' branch
uv pip install git+[https://github.com/fazpi-ai/qtask-client-py.git@main](https://github.com/fazpi-ai/qtask-client-py.git@main)

# Optionally, you can install from a specific tag or commit:
# uv pip install git+[https://github.com/fazpi-ai/qtask-client-py.git@v0.1.0](https://github.com/fazpi-ai/qtask-client-py.git@v0.1.0)
# uv pip install git+[https://github.com/fazpi-ai/qtask-client-py.git](https://github.com/fazpi-ai/qtask-client-py.git)@<commit_hash>
```

This will download and install the `qtask_client` library and its dependencies (like `requests`) into your virtual environment.

## 3. Installing Other Dependencies

If your microservice has other dependencies, ensure they are listed in a `requirements.txt` file and install them:

```bash
# Make sure your virtual environment is activated

# (Optional) Create or update your requirements.txt.
# You DON'T need to add the git+... line here if you already installed it above.
# Example requirements.txt:
# fastapi
# uvicorn[standard]
# # Other dependencies...

# Install dependencies from the file
uv pip install -r requirements.txt
```

## 4. Using the Library in Your Python Code

Now you can import and use the client in your application.

```python
import logging
import time
import os # To read environment variables if needed
from typing import List # For type hint

# Import the main client class
# Import the worker too if you need the type hint
from qtask_client import QTaskClient, QTaskConsumerWorker, BrokerApiException

# --- Initial Setup ---

# Configure logging for your application (do this once)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s %(name)s - %(message)s'
)

# Get the necessary URLs (can come from environment variables, config, etc.)
# Make sure your QTask Broker (m0-broker) is running
BROKER_API_URL = os.environ.get("QTASK_BROKER_URL", "http://localhost:3000")
# Make sure your Redis instance is accessible
REDIS_URL = os.environ.get("QTASK_REDIS_URL", "redis://localhost:6379")

# --- Instantiate the Client ---
# Create a single client instance for your application
# (can be global or managed by a dependency injection framework)
try:
    qtask_client = QTaskClient(broker_api_url=BROKER_API_URL, redis_url=REDIS_URL)
except ValueError as e:
    logging.error(f"Error creating QTaskClient: {e}")
    # Handle the error appropriately (exit, retry, etc.)
    exit(1)


# --- Define Message Handlers ---
# Use the @qtask_client.handler decorator to register functions
# that will process messages from specific topics/groups.

@qtask_client.handler(topic="user_signups", group="welcome_email_sender")
def handle_new_user(data: dict, message_id: str, partition_index: int):
    """
    This handler will be executed for each message in the 'user_signups' topic
    consumed by the 'welcome_email_sender' group.
    """
    user_email = data.get("email")
    user_name = data.get("name")
    logging.info(f"[Signup Handler][P{partition_index}] Processing signup for {user_email} (Msg ID: {message_id})")
    if not user_email:
        logging.error(f"Message {message_id} missing user email.")
        # Not raising an exception here means the message will be considered
        # processed (ACKed). You might want to move it to a dead-letter queue instead.
        return

    try:
        # Your logic to send the welcome email would go here
        print(f"Simulating sending welcome email to {user_email}...")
        time.sleep(0.5) # Simulate I/O
        print(f"Welcome email sent to {user_email}.")
        # If everything goes well, the function ends, and the worker will ACK.
    except Exception as e:
        logging.error(f"Failed to send welcome email for {message_id} to {user_email}: {e}", exc_info=True)
        # IMPORTANT! Raise an exception if you want the message NOT to be
        # acknowledged (ACKed) so it can be retried or claimed later.
        raise # Re-raise the exception


@qtask_client.handler(topic="image_uploads", group="thumbnail_generator")
def handle_image_upload(data: dict, message_id: str, partition_index: int):
    image_url = data.get("url")
    logging.info(f"[Image Handler][P{partition_index}] Processing image {message_id}: {image_url}")
    # Logic to download image, generate thumbnail, save, etc.
    time.sleep(1) # Simulate longer work


# --- Create and Manage Consumers ---

# List to keep references to active workers
active_workers: List[QTaskConsumerWorker] = []

def start_consumers():
    """Function to create and start the consumers."""
    logging.info("Attempting to create and start consumers...")
    try:
        # Create consumer for 'user_signups'
        signup_worker = qtask_client.create_consumer(
            topic="user_signups",
            group="welcome_email_sender"
        )
        logging.info(f"Signup worker created (ID: {signup_worker.consumer_id}, Partition: {signup_worker.partition_index})")
        signup_worker.start() # Start the worker!
        active_workers.append(signup_worker)
        logging.info(f"Signup worker started.")

        # Create consumer for 'image_uploads'
        image_worker = qtask_client.create_consumer(
            topic="image_uploads",
            group="thumbnail_generator"
        )
        logging.info(f"Image worker created (ID: {image_worker.consumer_id}, Partition: {image_worker.partition_index})")
        image_worker.start() # Start the worker!
        active_workers.append(image_worker)
        logging.info(f"Image worker started.")

    except ValueError as e:
        # Error if no handler was registered
        logging.error(f"Configuration error creating consumer: {e}")
    except (BrokerApiException, RuntimeError) as e:
        # Error contacting the Broker API or assigning partition
        logging.error(f"API error creating consumer: {e}")
    except Exception as e:
        logging.error(f"Unexpected error creating consumer: {e}", exc_info=True)


def stop_consumers():
    """Function to stop all active workers."""
    logging.info(f"Stopping {len(active_workers)} active workers...")
    for worker in active_workers:
        try:
            logging.info(f"Stopping worker {worker.consumer_id}...")
            worker.stop()
            logging.info(f"Worker {worker.consumer_id} stopped.")
        except Exception as e:
            logging.error(f"Error stopping worker {worker.consumer_id}: {e}", exc_info=True)
    active_workers.clear()


# --- Main Entry Point of Your Application ---
if __name__ == "__main__":
    # Make sure the modules where you define handlers with @qtask_client.handler
    # have been imported before calling start_consumers().

    start_consumers()

    try:
        # Keep your application running
        # (This could be your web server, main loop, etc.)
        while True:
            logging.debug("Main loop running...")
            # Check worker status if needed
            all_alive = all(w._read_loop_thread.is_alive() for w in active_workers if w._read_loop_thread)
            if not all_alive and active_workers: # Check only if workers were started
                 logging.warning("One or more consumer threads seem to have stopped unexpectedly!")
                 # You could implement restart logic here if desired
            time.sleep(60)

    except KeyboardInterrupt:
        logging.info("Shutdown signal received (KeyboardInterrupt).")
    finally:
        # Ensure clean shutdown of workers and the client
        stop_consumers()
        qtask_client.close()
        logging.info("Application finished.")

