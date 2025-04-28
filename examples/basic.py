import logging
import os
import time

# Import necessary components from your library
# Ensure QTaskClient is imported from the updated client.py
from qtask_client import QTaskClient, BrokerApiException

import config

# Configure logging for your application (do this once)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s %(name)s - %(message)s"
)

# Get the necessary URLs (can come from environment variables, config, etc.)
BROKER_API_URL = os.environ.get(
    "QTASK_BROKER_URL", f"http://{config.QTASK_BROKER_HOST}:{config.QTASK_BROKER_PORT}"
)
REDIS_URL = os.environ.get(
    "QTASK_REDIS_URL", f"redis://{config.REDIS_HOST}:{config.REDIS_PORT}"
)

# --- Instantiate the Client ---
# Create a single client instance for your application
try:
    qtask_client = QTaskClient(
        broker_api_url=BROKER_API_URL,
        redis_host=config.REDIS_HOST,
        redis_port=config.REDIS_PORT,
        redis_username=config.REDIS_USERNAME,  # Pasa el usuario
        redis_password=config.REDIS_PASSWORD,
    )
    # --- CORRECTION: Use logging instead of logger ---
    logging.info("QTaskClient instantiated successfully.")
except ValueError as e:
    logging.error(f"Configuration error creating QTaskClient: {e}")
    exit(1)  # Exit if client cannot be configured
except Exception as e:
    logging.error(f"Unexpected error creating QTaskClient: {e}", exc_info=True)
    exit(1)


# --- Define and Register Handlers ---
# Use the decorator provided by the client instance


@qtask_client.handler(topic="user_signups", group="welcome_email_sender")
def handle_new_user(data: dict, message_id: str, partition_index: int):
    """
    Handler for processing new user signups.
    (Example: Send a welcome email)
    """
    user_email = data.get("email")
    user_id = data.get("userId")
    # Use logging here as well for consistency, or define a logger for the main script
    logging.info(
        f"[HANDLER user_signups P{partition_index}] Received signup for user {user_id} ({user_email}). ID: {message_id}"
    )
    # Simulate sending an email
    try:
        # Replace with your actual email sending logic
        print(f"   -> Simulating sending welcome email to {user_email}...")
        time.sleep(0.5)  # Simulate network latency
        print(f"   -> Welcome email sent for user {user_id}.")
        logging.info(
            f"[HANDLER user_signups P{partition_index}] Successfully processed message {message_id}."
        )
    except Exception as e:
        logging.error(
            f"[HANDLER user_signups P{partition_index}] Failed to process message {message_id} for user {user_id}: {e}",
            exc_info=True,
        )
        # Re-raise the exception so the message is not acknowledged and can be retried/handled
        raise


@qtask_client.handler(topic="image_uploads", group="thumbnail_generator")
def handle_image_upload(data: dict, message_id: str, partition_index: int):
    """
    Handler for processing image uploads.
    (Example: Generate a thumbnail)
    """
    image_path = data.get("path")
    image_id = data.get("imageId")
    logging.info(
        f"[HANDLER image_uploads P{partition_index}] Received upload for image {image_id} ({image_path}). ID: {message_id}"
    )
    # Simulate generating a thumbnail
    try:
        # Replace with your actual image processing logic
        print(f"   -> Simulating thumbnail generation for {image_path}...")
        time.sleep(1.0)  # Simulate processing time
        print(f"   -> Thumbnail generated for image {image_id}.")
        logging.info(
            f"[HANDLER image_uploads P{partition_index}] Successfully processed message {message_id}."
        )
    except Exception as e:
        logging.error(
            f"[HANDLER image_uploads P{partition_index}] Failed to process message {message_id} for image {image_id}: {e}",
            exc_info=True,
        )
        # Re-raise for retry/handling
        raise


# --- Main Entry Point of Your Application ---
if __name__ == "__main__":
    # Handlers are already registered by the time this block runs
    # because the decorators execute when the functions are defined.

    # --- Start All Consumers ---
    # The client now handles creating workers for all registered handlers
    # and starting them internally.
    logging.info("Starting all registered consumers...")
    qtask_client.start_all_consumers()
    logging.info("Consumers startup process initiated.")

    # --- Optional: Publish some test messages ---
    try:
        logging.info("Publishing a test message to user_signups...")
        qtask_client.publish(
            topic="user_signups",
            partition_key="user123@example.com",  # Use email or user ID as partition key
            data={
                "userId": "user123",
                "email": "user123@example.com",
                "timestamp": time.time(),
            },
        )
        logging.info("Publishing a test message to image_uploads...")
        qtask_client.publish(
            topic="image_uploads",
            partition_key="img_abc.jpg",  # Use image ID or path as partition key
            data={"imageId": "img_abc", "path": "/uploads/img_abc.jpg", "size": 1024},
        )
        logging.info("Test messages published.")
    except BrokerApiException as e:
        logging.error(f"Failed to publish test messages: {e}")
    except Exception as e:
        logging.error(f"Unexpected error publishing test messages: {e}", exc_info=True)

    # --- Keep Application Running ---
    logging.info("Application running. Press Ctrl+C to stop.")
    try:
        # Keep your application's main thread alive.
        # This could be your web server loop, a simple sleep loop, etc.
        while True:
            # The worker threads are running in the background.
            # You could add periodic health checks or other main loop logic here.
            time.sleep(60)  # Sleep for a minute

    except KeyboardInterrupt:
        logging.info("Shutdown signal received (KeyboardInterrupt).")
    except Exception as e:
        logging.error("An unexpected error occurred in the main loop.", exc_info=True)
    finally:
        # --- Ensure Clean Shutdown ---
        # The client's close method now handles stopping all managed workers
        # and closing the API connection.
        logging.info("Shutting down...")
        qtask_client.close()
        logging.info("Application finished.")
