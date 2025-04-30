import asyncio
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

# --- Instantiate the Client Globally (Needed for Decorators) ---
# Create a single client instance for your application
try:
    qtask_client = QTaskClient(
        broker_api_url=f"http://{config.QTASK_BROKER_HOST}:{config.QTASK_BROKER_PORT}",
        redis_host=config.REDIS_HOST,
        redis_port=config.REDIS_PORT,
        redis_username=config.REDIS_USERNAME,  # Pasa el usuario
        redis_password=config.REDIS_PASSWORD,
    )
    logging.info("QTaskClient instantiated globally for decorators.")
except ValueError as e:
    logging.error(f"Configuration error creating QTaskClient globally: {e}")
    exit(1)  # Exit if client cannot be configured
except Exception as e:
    logging.error(f"Unexpected error creating QTaskClient globally: {e}", exc_info=True)
    exit(1)


# --- Define and Register Handlers ---
# Use the decorator provided by the client instance (now defined globally)


@qtask_client.handler(topic="whatsapp_messages", group="whatsapp_message_sender")
async def handle_whatsapp_message(data: dict, message_id: str, partition_index: int):
    """
    Handler for processing new whatsapp messages.
    (Example: Send a whatsapp message)
    """
    to = data.get("to")
    content = data.get("content")
    logging.info(
        f"[🛑 HANDLER whatsapp_messages P{partition_index}] Received message for user {to} ({content}). ID: {message_id}"
    )

# --- Main Async Function ---
async def main():
    # Client is already instantiated globally

    # --- Start All Consumers ---
    # Handlers are registered above using the global client instance
    logging.info("Starting all registered consumers...")
    await qtask_client.start_all_consumers()
    logging.info("Consumers startup process initiated.")

    # --- Keep Application Running ---
    logging.info("Application running. Press Ctrl+C to stop.")
    try:
        while True:
            # The worker tasks are running in the background via asyncio.
            await asyncio.sleep(60)

    except KeyboardInterrupt:
        logging.info("Shutdown signal received (KeyboardInterrupt).")
    except Exception as e:
        # Log errors happening in the main loop, but shutdown logic is in finally
        logging.error("An unexpected error occurred in the main loop.", exc_info=True)
        # Exception will be caught by the outer try/except around asyncio.run
    finally:
        # --- Ensure Clean Shutdown ---
        # Client is global, so we don't need to check if it exists here
        logging.info("Shutting down consumers...")
        await qtask_client.close()
        logging.info("Application finished.")


# --- Main Entry Point of Your Application ---
if __name__ == "__main__":
    # Handlers are registered above when the functions are defined, using the global client.
    # Execute the main asynchronous loop
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # The finally block inside main() handles the shutdown logging.
        # We might want a final message here if needed.
        logging.info("Application stopped by user.")
    except Exception as e:
        # Catch any unhandled exceptions from main() or asyncio itself
        logging.critical(
            f"Application exited due to unhandled exception: {e}", exc_info=True
        )
        exit(1)
