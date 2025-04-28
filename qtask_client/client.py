import logging
import random
import string
import time  # Added for example usage sleep
from typing import Dict, Callable, Optional, List, Any, Tuple

# Import other library components
from .api_client import (
    QTaskBrokerApiClient,
    BrokerApiException,
    NoPartitionsAvailableError,
)
from .consumer_worker import QTaskConsumerWorker, MessageHandler

# Configure logger for this module
logger = logging.getLogger(__name__)


class QTaskClient:
    """
    Main client and facade for interacting with the QTask Broker system.

    Provides simplified methods for publishing messages and creating/managing
    consumers based on registered handlers.
    """

    def __init__(self, broker_api_url: str, redis_url: str):
        """
        Initializes the QTask client.

        Args:
            broker_api_url (str): The base URL of the QTask Broker API.
            redis_url (str): The Redis connection URL for the Consumer Workers.
                             (e.g., "redis://user:pass@host:port")
        """
        if not broker_api_url:
            raise ValueError("broker_api_url cannot be empty.")
        if not redis_url:
            raise ValueError("redis_url cannot be empty.")

        self.broker_api_url = broker_api_url
        self.redis_url = redis_url
        self.api_client = QTaskBrokerApiClient(base_url=self.broker_api_url)

        # Registry for handlers: {(topic, group): handler_function}
        self._handler_registry: Dict[Tuple[str, str], MessageHandler] = {}
        # --- NEW: Internal list to manage active workers ---
        self._active_workers: List[QTaskConsumerWorker] = []
        self._consumers_started = False  # Flag to track if consumers were started

        logger.info(
            f"QTaskClient initialized. Broker API: {self.broker_api_url}, Redis URL: {self.redis_url}"
        )

    def _generate_consumer_id(self, topic: str, group: str) -> str:
        """Generates a reasonably unique consumer ID."""
        random_suffix = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        return f"consumer-{topic}-{group}-{random_suffix}"

    # --- Decorator for registering Handlers ---
    def handler(
        self, topic: str, group: str
    ) -> Callable[[MessageHandler], MessageHandler]:
        """
        Decorator to register a function as a handler for a specific topic and group.

        Usage:
            @qtask_client.handler(topic="MY_TOPIC", group="MY_GROUP")
            def my_handler_function(data, message_id, partition_index):
                # ... process message ...

        Args:
            topic (str): The base topic name.
            group (str): The consumer group name.

        Returns:
            The actual decorator.
        """
        if not topic or not group:
            raise ValueError(
                "Topic and group cannot be empty in the handler decorator."
            )

        def decorator(func: MessageHandler) -> MessageHandler:
            key = (topic, group)
            if key in self._handler_registry:
                logger.warning(
                    f"Overwriting existing handler for topic '{topic}', group '{group}'."
                )
            logger.info(
                f"Registering handler '{func.__name__}' for topic '{topic}', group '{group}'."
            )
            self._handler_registry[key] = func
            return func

        return decorator

    # --- Public Facade Methods ---

    def publish(
        self, topic: str, partition_key: str, data: Dict[str, Any]
    ) -> Tuple[int, str]:
        """
        Publishes a message to the specified topic.

        Args:
            topic: Base topic name.
            partition_key: Key used to determine the partition.
            data: Dictionary containing the message data.

        Returns:
            A tuple (partition_index: int, message_id: str) on success.

        Raises:
            BrokerApiException: If the Broker API returns an error or connection issues occur.
            ValueError: If the API response is invalid.
        """
        logger.debug(f"Publish request: Topic={topic}, PartitionKey={partition_key}")
        try:
            partition_index, message_id = self.api_client.push(
                topic, partition_key, data
            )
            return partition_index, message_id
        except Exception as e:
            logger.error(f"Failed to publish to topic '{topic}': {e}", exc_info=True)
            raise

    # Method remains largely the same but is now primarily for internal use by start_all_consumers
    # Could be made private (`_create_consumer`) if desired.
    def create_consumer(self, topic: str, group: str) -> Optional[QTaskConsumerWorker]:
        """
        Creates and configures a QTaskConsumerWorker for a specific topic/group.
        This is primarily used internally by start_all_consumers.

        Args:
            topic: Base topic name.
            group: Consumer group name.

        Returns:
            A QTaskConsumerWorker instance ready to be started, or None if assignment fails recoverably.

        Raises:
            ValueError: If no handler was found registered for the topic/group.
            BrokerApiException: If unrecoverable errors occur contacting the Broker API.
            RuntimeError: If a partition could not be assigned after max retries.
        """
        logger.debug(
            f"Attempting to create consumer for topic '{topic}', group '{group}'."
        )

        handler_key = (topic, group)
        registered_handler = self._handler_registry.get(handler_key)
        if registered_handler is None:
            msg = f"No handler registered for topic '{topic}', group '{group}'. Cannot create consumer."
            logger.error(msg)
            raise ValueError(msg)
        logger.debug(
            f"Handler '{registered_handler.__name__}' found for {handler_key}."
        )

        consumer_id = self._generate_consumer_id(topic, group)
        logger.debug(f"Generated consumer ID: {consumer_id}")

        try:
            logger.info(
                f"Requesting partition assignment from API for {consumer_id}..."
            )
            partition_index = self.api_client.assign_partition(
                topic, group, consumer_id
            )
            logger.info(f"Partition {partition_index} assigned to {consumer_id}.")
        except NoPartitionsAvailableError:
            # This is a potentially temporary state, log and return None
            logger.warning(
                f"No partitions currently available for topic '{topic}', group '{group}'. Consumer not created."
            )
            return None
        except (BrokerApiException, RuntimeError) as e:
            # These are more serious errors (connection, max retries exceeded)
            logger.error(
                f"Could not get partition assignment for {consumer_id}: {e}",
                exc_info=True,
            )
            raise  # Re-raise critical errors

        try:
            logger.info(
                f"Ensuring subscription via API for partition {partition_index}..."
            )
            subscribed = self.api_client.subscribe(topic, group, partition_index)
            if not subscribed:
                logger.warning(
                    f"API indicated failure ensuring subscription for partition {partition_index}, continuing anyway..."
                )
        except Exception as e:
            logger.warning(
                f"Error calling /subscribe for partition {partition_index} (continuing): {e}",
                exc_info=False,
            )

        logger.info(
            f"Creating QTaskConsumerWorker instance for partition {partition_index}..."
        )
        worker = QTaskConsumerWorker(
            redis_url=self.redis_url,
            topic=topic,
            group=group,
            partition_index=partition_index,
            consumer_id=consumer_id,
            handler=registered_handler,
        )
        logger.info(f"QTaskConsumerWorker created for {consumer_id}.")
        return worker

    # --- NEW: Method to start all consumers based on registered handlers ---
    def start_all_consumers(self):
        """
        Creates and starts consumer workers for all handlers registered via the decorator.

        Iterates through the registered handlers, creates a worker for each,
        starts it, and keeps track of it internally.
        Handles potential errors during worker creation gracefully.
        """
        if self._consumers_started:
            logger.warning(
                "Consumers already started. Call stop_all_consumers() first if you need to restart."
            )
            return

        if not self._handler_registry:
            logger.warning("No handlers registered. Cannot start any consumers.")
            return

        logger.info(
            f"Starting consumers for {len(self._handler_registry)} registered handler(s)..."
        )
        self._active_workers = (
            []
        )  # Clear any previous workers (e.g., if stopped and restarted)

        for (topic, group), handler in self._handler_registry.items():
            logger.info(
                f"Attempting to start consumer for topic='{topic}', group='{group}'..."
            )
            try:
                worker = self.create_consumer(topic=topic, group=group)
                if worker:
                    worker.start()  # Start the worker!
                    self._active_workers.append(worker)
                    logger.info(
                        f"Successfully started consumer {worker.consumer_id} for topic='{topic}', group='{group}'."
                    )
                else:
                    # create_consumer returned None (e.g., no partitions available)
                    logger.warning(
                        f"Consumer for topic='{topic}', group='{group}' not started (likely no partitions available currently)."
                    )

            except (ValueError, BrokerApiException, RuntimeError) as e:
                # Log errors during creation/start but continue trying to start others
                logger.error(
                    f"Failed to create/start consumer for topic='{topic}', group='{group}': {e}",
                    exc_info=True,
                )
            except Exception as e:
                logger.error(
                    f"Unexpected error starting consumer for topic='{topic}', group='{group}': {e}",
                    exc_info=True,
                )

        self._consumers_started = True
        logger.info(
            f"Finished starting consumers. {len(self._active_workers)} worker(s) are active."
        )

    # --- NEW: Method to stop all managed consumers ---
    def stop_all_consumers(self):
        """Stops all consumer workers that were started by this client instance."""
        if not self._consumers_started:
            logger.info("No consumers were started by this client instance.")
            return

        logger.info(f"Stopping {len(self._active_workers)} active worker(s)...")
        stopped_count = 0
        for worker in self._active_workers:
            try:
                logger.info(f"Stopping worker {worker.consumer_id}...")
                worker.stop()
                stopped_count += 1
                logger.info(f"Worker {worker.consumer_id} stopped.")
            except Exception as e:
                logger.error(
                    f"Error stopping worker {worker.consumer_id}: {e}", exc_info=True
                )

        logger.info(
            f"Finished stopping consumers. {stopped_count}/{len(self._active_workers)} stopped successfully."
        )
        self._active_workers = []  # Clear the list after stopping
        self._consumers_started = False

    def list_topics(self) -> List[str]:
        """
        Gets the list of managed base topics from the Broker.

        Returns:
            List of topic names.

        Raises:
            BrokerApiException: If errors occur contacting the Broker API.
        """
        logger.debug("Requesting list of topics from API...")
        try:
            return self.api_client.get_topics()
        except Exception as e:
            logger.error(f"Failed to get topic list: {e}", exc_info=True)
            raise

    def close(self):
        """
        Stops all managed consumers and closes underlying connections.
        It's recommended to call this during application shutdown.
        """
        logger.info("Closing QTaskClient...")
        # --- NEW: Ensure consumers are stopped before closing API client ---
        self.stop_all_consumers()
        self.api_client.close()
        logger.info("QTaskClient closed.")


# --- Example Usage (Updated) ---
if __name__ == "__main__":
    # --- Basic Logging Setup ---
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s %(name)s - %(message)s"
    )

    # --- Configuration ---
    BROKER_URL = "http://localhost:3000"  # URL of your Broker API
    REDIS_CONN_URL = "redis://localhost:6379"  # URL of your Redis

    # --- Create client instance ---
    qtask_client = QTaskClient(broker_api_url=BROKER_URL, redis_url=REDIS_CONN_URL)

    # --- Register Handlers (Same as before) ---
    @qtask_client.handler(topic="orders", group="processors")
    def handle_order(data: Dict, message_id: str, partition_index: int):
        logger.info(
            f"[ORDER HANDLER][P{partition_index}] Processing order {message_id}: {data.get('order_id')}"
        )
        # Simulate work or potential errors
        time.sleep(random.uniform(0.1, 0.3))
        if random.random() < 0.05:  # Simulate occasional error
            logger.error(
                f"[ORDER HANDLER][P{partition_index}] Simulated error processing order {message_id}"
            )
            raise ValueError("Simulated processing error")
        logger.info(f"[ORDER HANDLER][P{partition_index}] Finished order {message_id}")

    @qtask_client.handler(topic="notifications", group="senders")
    def handle_notification(data: Dict, message_id: str, partition_index: int):
        logger.info(
            f"[NOTIF HANDLER][P{partition_index}] Sending notification {message_id}: for {data.get('user')}"
        )
        time.sleep(random.uniform(0.05, 0.15))
        logger.info(
            f"[NOTIF HANDLER][P{partition_index}] Sent notification {message_id}"
        )

    # --- Main Application Logic ---
    try:
        # List initial topics
        print("\n--- Listing Topics ---")
        try:
            topics = qtask_client.list_topics()
            print(f"Managed Topics: {topics}")
        except BrokerApiException as e:
            print(f"Error listing topics: {e}")

        # Publish some messages (optional)
        print("\n--- Publishing Test Messages ---")
        try:
            qtask_client.publish("orders", "cust1", {"order_id": "ord_A", "value": 10})
            qtask_client.publish("orders", "cust2", {"order_id": "ord_B", "value": 25})
            qtask_client.publish(
                "notifications", "userX", {"user": "userX", "alert": "System update"}
            )
            qtask_client.publish("orders", "cust1", {"order_id": "ord_C", "value": 5})
            print("Published test messages.")
        except BrokerApiException as e:
            print(f"Error publishing test messages: {e}")

        # --- Start Consumers (Simplified) ---
        print("\n--- Starting All Consumers ---")
        # This single call now handles creating and starting workers for all registered handlers
        qtask_client.start_all_consumers()

        # --- Keep the application running ---
        print("\n--- Application Running (Press Ctrl+C to stop) ---")
        # Your main application logic would go here (e.g., web server, background task)
        # For this example, we just sleep.
        while True:
            # You could add health checks for workers if needed,
            # although the workers themselves handle reconnections.
            time.sleep(30)  # Keep main thread alive

    except KeyboardInterrupt:
        print("\nShutdown signal received (KeyboardInterrupt).")
    except Exception as e:
        print(f"\nAn unexpected error occurred in the main loop: {e}")
        logger.error("Unexpected main loop error", exc_info=True)
    finally:
        # --- Ensure clean shutdown ---
        print("\n--- Shutting Down ---")
        # This single call now handles stopping all managed workers and closing connections
        qtask_client.close()
        print("Application finished.")
