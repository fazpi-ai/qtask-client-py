import logging
import random
import string
import time
import os # Import os module to read environment variables
from typing import Dict, Callable, Optional, List, Any, Tuple
from urllib.parse import urlunparse # To build Redis URL

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
    consumers based on registered handlers. Configuration can be provided
    via constructor arguments or environment variables.
    """

    # --- Default Configuration Values ---
    DEFAULT_BROKER_URL = "http://localhost:3000"
    DEFAULT_REDIS_HOST = "localhost"
    DEFAULT_REDIS_PORT = 6379
    DEFAULT_REDIS_USERNAME = None # Or 'default' if that's standard for your Redis setup
    DEFAULT_REDIS_PASSWORD = None

    def __init__(
        self,
        broker_api_url: Optional[str] = None,
        redis_url: Optional[str] = None,
        # --- OR provide individual Redis components ---
        redis_host: Optional[str] = None,
        redis_port: Optional[int] = None,
        redis_username: Optional[str] = None,
        redis_password: Optional[str] = None,
    ):
        """
        Initializes the QTask client.

        Configuration priority:
        1. Explicit constructor arguments (broker_api_url, redis_url).
        2. Individual Redis components if redis_url is not given (redis_host, etc.).
        3. Environment variables (QTASK_BROKER_URL, REDIS_HOST, REDIS_PORT, etc.).
        4. Default values.

        Args:
            broker_api_url (Optional[str]): Base URL of the QTask Broker API.
                                            Defaults to env QTASK_BROKER_URL or DEFAULT_BROKER_URL.
            redis_url (Optional[str]): Full Redis connection URL (e.g., "redis://user:pass@host:port").
                                       Takes precedence over individual components if provided.
                                       Defaults to constructing from other args/env/defaults.
            redis_host (Optional[str]): Redis host. Defaults to env REDIS_HOST or DEFAULT_REDIS_HOST.
            redis_port (Optional[int]): Redis port. Defaults to env REDIS_PORT or DEFAULT_REDIS_PORT.
            redis_username (Optional[str]): Redis username. Defaults to env REDIS_USERNAME or DEFAULT_REDIS_USERNAME.
            redis_password (Optional[str]): Redis password. Defaults to env REDIS_PASSWORD or DEFAULT_REDIS_PASSWORD.
        """

        # --- Determine Broker API URL ---
        if broker_api_url:
            self.broker_api_url = broker_api_url
            logger.info(f"Using provided Broker API URL: {self.broker_api_url}")
        else:
            self.broker_api_url = os.environ.get("QTASK_BROKER_URL", self.DEFAULT_BROKER_URL)
            logger.info(f"Using Broker API URL from env/default: {self.broker_api_url}")

        if not self.broker_api_url: # Should not happen with default, but check anyway
            raise ValueError("Could not determine Broker API URL.")

        # --- Determine Redis URL ---
        if redis_url:
            self.redis_url = redis_url
            logger.info(f"Using provided Redis URL: {self.redis_url}")
        else:
            # Construct from components (args -> env -> defaults)
            _host = redis_host or os.environ.get("REDIS_HOST", self.DEFAULT_REDIS_HOST)
            _port_str = os.environ.get("REDIS_PORT", str(self.DEFAULT_REDIS_PORT))
            try:
                 _port = redis_port or int(_port_str)
            except (ValueError, TypeError):
                 logger.warning(f"Invalid REDIS_PORT environment variable '{_port_str}'. Using default {self.DEFAULT_REDIS_PORT}.")
                 _port = self.DEFAULT_REDIS_PORT

            # Handle optional username/password, allowing empty strings from env
            _user_env = os.environ.get("REDIS_USERNAME")
            _pass_env = os.environ.get("REDIS_PASSWORD")

            _user = redis_username if redis_username is not None else _user_env
            _pass = redis_password if redis_password is not None else _pass_env

            # Construct the URL: redis://[user:pass@]host:port
            netloc = ""
            if _user and _pass:
                netloc = f"{_user}:{_pass}@{_host}:{_port}"
            elif _user: # Username only (less common for Redis standard auth)
                 netloc = f"{_user}@{_host}:{_port}"
            else: # No auth or password only (ACL might use password without user)
                 if _pass:
                      netloc = f":{_pass}@{_host}:{_port}" # Format for password only
                 else:
                      netloc = f"{_host}:{_port}"

            self.redis_url = urlunparse(("redis", netloc, "", "", "", ""))
            logger.info(f"Constructed Redis URL from components/env/defaults: {self.redis_url}")

        if not self.redis_url: # Should not happen, but check
            raise ValueError("Could not determine Redis URL.")

        # --- Initialize API Client and Worker Management ---
        self.api_client = QTaskBrokerApiClient(base_url=self.broker_api_url)
        self._handler_registry: Dict[Tuple[str, str], MessageHandler] = {}
        self._active_workers: List[QTaskConsumerWorker] = []
        self._consumers_started = False

        logger.info(
            f"QTaskClient initialized. Broker API: {self.broker_api_url}, Redis URL: {self.redis_url}"
        )

    def _generate_consumer_id(self, topic: str, group: str) -> str:
        """Generates a reasonably unique consumer ID."""
        random_suffix = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        return f"consumer-{topic}-{group}-{random_suffix}"

    def handler(
        self, topic: str, group: str
    ) -> Callable[[MessageHandler], MessageHandler]:
        """Decorator to register a message handler."""
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

    def publish(
        self, topic: str, partition_key: str, data: Dict[str, Any]
    ) -> Tuple[int, str]:
        """Publishes a message."""
        logger.debug(f"Publish request: Topic={topic}, PartitionKey={partition_key}")
        try:
            partition_index, message_id = self.api_client.push(
                topic, partition_key, data
            )
            return partition_index, message_id
        except Exception as e:
            logger.error(f"Failed to publish to topic '{topic}': {e}", exc_info=True)
            raise

    def create_consumer(self, topic: str, group: str) -> Optional[QTaskConsumerWorker]:
        """Creates a consumer worker instance (used internally)."""
        logger.debug(
            f"Attempting to create consumer for topic '{topic}', group '{group}'."
        )
        handler_key = (topic, group)
        registered_handler = self._handler_registry.get(handler_key)
        if registered_handler is None:
            msg = f"No handler registered for topic '{topic}', group '{group}'. Cannot create consumer."
            logger.error(msg)
            raise ValueError(msg)

        consumer_id = self._generate_consumer_id(topic, group)
        logger.debug(f"Generated consumer ID: {consumer_id}")

        try:
            logger.info(f"Requesting partition assignment from API for {consumer_id}...")
            partition_index = self.api_client.assign_partition(topic, group, consumer_id)
            logger.info(f"Partition {partition_index} assigned to {consumer_id}.")
        except NoPartitionsAvailableError:
            logger.warning(f"No partitions currently available for topic '{topic}', group '{group}'. Consumer not created.")
            return None
        except (BrokerApiException, RuntimeError) as e:
            logger.error(f"Could not get partition assignment for {consumer_id}: {e}", exc_info=True)
            raise

        try:
            logger.info(f"Ensuring subscription via API for partition {partition_index}...")
            subscribed = self.api_client.subscribe(topic, group, partition_index)
            if not subscribed: logger.warning(f"API indicated failure ensuring subscription for partition {partition_index}, continuing anyway...")
        except Exception as e:
            logger.warning(f"Error calling /subscribe for partition {partition_index} (continuing): {e}", exc_info=False)

        logger.info(f"Creating QTaskConsumerWorker instance for partition {partition_index}...")
        worker = QTaskConsumerWorker(
            redis_url=self.redis_url, # Worker still uses the final URL
            topic=topic,
            group=group,
            partition_index=partition_index,
            consumer_id=consumer_id,
            handler=registered_handler,
        )
        logger.info(f"QTaskConsumerWorker created for {consumer_id}.")
        return worker

    def start_all_consumers(self):
        """Creates and starts consumer workers for all registered handlers."""
        if self._consumers_started:
            logger.warning("Consumers already started. Call stop_all_consumers() first if you need to restart.")
            return
        if not self._handler_registry:
            logger.warning("No handlers registered. Cannot start any consumers.")
            return

        logger.info(f"Starting consumers for {len(self._handler_registry)} registered handler(s)...")
        self._active_workers = []
        for (topic, group), handler in self._handler_registry.items():
            logger.info(f"Attempting to start consumer for topic='{topic}', group='{group}'...")
            try:
                worker = self.create_consumer(topic=topic, group=group)
                if worker:
                    worker.start()
                    self._active_workers.append(worker)
                    logger.info(f"Successfully started consumer {worker.consumer_id} for topic='{topic}', group='{group}'.")
                else:
                    logger.warning(f"Consumer for topic='{topic}', group='{group}' not started (likely no partitions available currently).")
            except (ValueError, BrokerApiException, RuntimeError) as e:
                logger.error(f"Failed to create/start consumer for topic='{topic}', group='{group}': {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Unexpected error starting consumer for topic='{topic}', group='{group}': {e}", exc_info=True)

        self._consumers_started = True
        logger.info(f"Finished starting consumers. {len(self._active_workers)} worker(s) are active.")

    def stop_all_consumers(self):
        """Stops all consumer workers managed by this client."""
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
                logger.error(f"Error stopping worker {worker.consumer_id}: {e}", exc_info=True)

        logger.info(f"Finished stopping consumers. {stopped_count}/{len(self._active_workers)} stopped successfully.")
        self._active_workers = []
        self._consumers_started = False

    def list_topics(self) -> List[str]:
        """Gets the list of managed base topics from the Broker."""
        logger.debug("Requesting list of topics from API...")
        try:
            return self.api_client.get_topics()
        except Exception as e:
            logger.error(f"Failed to get topic list: {e}", exc_info=True)
            raise

    def close(self):
        """Stops consumers and closes connections."""
        logger.info("Closing QTaskClient...")
        self.stop_all_consumers()
        self.api_client.close()
        logger.info("QTaskClient closed.")


# --- Example Usage (Demonstrating Env Var Configuration) ---
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s %(name)s - %(message)s"
    )

    # --- Configuration (Now relying on Env Vars or Defaults) ---
    # Set environment variables before running this script, e.g.:
    # export QTASK_BROKER_URL="http://127.0.0.1:3000"
    # export REDIS_HOST="127.0.0.1"
    # export REDIS_PORT="6379"
    # export REDIS_USERNAME="myuser" # Optional
    # export REDIS_PASSWORD="mypassword" # Optional

    print("\n--- Creating Client Instance (using Env Vars/Defaults) ---")
    try:
        # Instantiate WITHOUT passing URLs explicitly
        qtask_client = QTaskClient()
        print(f"Client created. Broker: {qtask_client.broker_api_url}, Redis: {qtask_client.redis_url}")
    except ValueError as e:
        print(f"Error creating client: {e}")
        exit(1)

    # --- Register Handlers (Same as before) ---
    @qtask_client.handler(topic="config_test", group="testers")
    def handle_config_test(data: Dict, message_id: str, partition_index: int):
        logger.info(
            f"[CONFIG_TEST HANDLER][P{partition_index}] Received test message {message_id}: {data}"
        )
        time.sleep(0.1)

    # --- Main Application Logic ---
    try:
        print("\n--- Listing Topics ---")
        try:
            topics = qtask_client.list_topics()
            print(f"Managed Topics: {topics}")
        except BrokerApiException as e:
            print(f"Error listing topics: {e}")

        print("\n--- Publishing Test Message ---")
        try:
            qtask_client.publish("config_test", "test_key", {"status": "ok"})
            print("Published test message.")
        except BrokerApiException as e:
            print(f"Error publishing test message: {e}")

        print("\n--- Starting All Consumers ---")
        qtask_client.start_all_consumers()

        print("\n--- Application Running (Press Ctrl+C to stop) ---")
        while True:
            time.sleep(30)

    except KeyboardInterrupt:
        print("\nShutdown signal received.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        logger.error("Unexpected main loop error", exc_info=True)
    finally:
        print("\n--- Shutting Down ---")
        qtask_client.close()
        print("Application finished.")

