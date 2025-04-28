import logging
import os
import time

# Import necessary components from your library
from qtask_client import (
    QTaskClient,
    BrokerApiException,
)  # Ensure QTaskClient is imported

# Configure logging for your application (do this once)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s %(name)s - %(message)s"
)

# --- Configuration Values (Example) ---
# You can still use os.environ.get here if you want fallback to env vars,
# but the example now shows passing them directly.
BROKER_URL_CONFIG = "http://localhost:3000"  # Example Broker URL
REDIS_HOST_CONFIG = "localhost"  # Example Redis Host
REDIS_PORT_CONFIG = 6379  # Example Redis Port
REDIS_USER_CONFIG = "default"  # Example Redis User (or None)
REDIS_PASS_CONFIG = ""  # Example Redis Password (or None)


# --- Instantiate the Client (using explicit arguments) ---
try:
    # Instantiate passing arguments explicitly
    # The client will use these values instead of environment variables or defaults
    qtask_client = QTaskClient(
        broker_api_url=BROKER_URL_CONFIG,
        redis_host=REDIS_HOST_CONFIG,
        redis_port=REDIS_PORT_CONFIG,
        redis_username=REDIS_USER_CONFIG,
        redis_password=REDIS_PASS_CONFIG,
        # Alternatively, construct the full redis_url and pass it:
        # redis_url=f"redis://{REDIS_USER_CONFIG}:{REDIS_PASS_CONFIG}@{REDIS_HOST_CONFIG}:{REDIS_PORT_CONFIG}"
    )
    logging.info("QTaskClient instantiated successfully (using explicit args).")
    # Log the resolved URLs for verification
    logging.info(f"-> Broker API URL used: {qtask_client.broker_api_url}")
    logging.info(f"-> Redis URL used: {qtask_client.redis_url}")
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
    logging.info(
        f"[HANDLER user_signups P{partition_index}] Received signup for user {user_id} ({user_email}). ID: {message_id}"
    )
    try:
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
    try:
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
        raise


# --- Main Entry Point of Your Application ---
if __name__ == "__main__":
    # Handlers are registered when the functions are defined above.

    # --- Start All Consumers ---
    logging.info("Starting all registered consumers...")
    qtask_client.start_all_consumers()
    logging.info("Consumers startup process initiated.")

    # --- Optional: Publish some test messages ---
    try:
        logging.info("Publishing a test message to user_signups...")
        qtask_client.publish(
            topic="user_signups",
            partition_key="user456@example.com",  # Use email or user ID as partition key
            data={
                "userId": "user456",
                "email": "user456@example.com",
                "timestamp": time.time(),
            },
        )
        logging.info("Publishing a test message to image_uploads...")
        qtask_client.publish(
            topic="image_uploads",
            partition_key="img_def.png",  # Use image ID or path as partition key
            data={"imageId": "img_def", "path": "/uploads/img_def.png", "size": 2048},
        )
        logging.info("Test messages published.")
    except BrokerApiException as e:
        logging.error(f"Failed to publish test messages: {e}")
    except Exception as e:
        logging.error(f"Unexpected error publishing test messages: {e}", exc_info=True)

    # --- Keep Application Running ---
    logging.info("Application running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(60)  # Keep main thread alive

    except KeyboardInterrupt:
        logging.info("Shutdown signal received (KeyboardInterrupt).")
    except Exception as e:
        logging.error("An unexpected error occurred in the main loop.", exc_info=True)
    finally:
        # --- Ensure Clean Shutdown ---
        logging.info("Shutting down...")
        qtask_client.close()
        logging.info("Application finished.")
