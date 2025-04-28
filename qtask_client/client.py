import logging
import random
import string
import time # Added for example usage sleep
from typing import Dict, Callable, Optional, List, Any, Tuple

# Import other library components
# Renamed imports
from .api_client import QTaskBrokerApiClient, BrokerApiException
from .consumer_worker import QTaskConsumerWorker, MessageHandler

# Configure logger for this module
logger = logging.getLogger(__name__)

# Renamed class
class QTaskClient:
    """
    Main client and facade for interacting with the QTask Broker system.

    Provides simplified methods for publishing messages and creating consumers,
    managing communication with the Broker API and worker instantiation.
    Also allows registering message handlers via decorators.
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
        # Use renamed class
        self.api_client = QTaskBrokerApiClient(base_url=self.broker_api_url)

        # Registry for handlers: {(topic, group): handler_function}
        self._handler_registry: Dict[Tuple[str, str], MessageHandler] = {}

        # Log message updated
        logger.info(f"QTaskClient initialized. Broker API: {self.broker_api_url}, Redis URL: {self.redis_url}")

    def _generate_consumer_id(self, topic: str, group: str) -> str:
        """Generates a reasonably unique consumer ID."""
        # Similar to the JS client, but using Python tools
        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        # We could also add the PID if desired: os.getpid()
        return f"consumer-{topic}-{group}-{random_suffix}"

    # --- Decorator for registering Handlers ---
    def handler(self, topic: str, group: str) -> Callable[[MessageHandler], MessageHandler]:
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
            raise ValueError("Topic and group cannot be empty in the handler decorator.")

        def decorator(func: MessageHandler) -> MessageHandler:
            key = (topic, group)
            if key in self._handler_registry:
                logger.warning(f"Overwriting existing handler for topic '{topic}', group '{group}'.")
            logger.info(f"Registering handler '{func.__name__}' for topic '{topic}', group '{group}'.")
            self._handler_registry[key] = func
            # Return the original function unmodified
            return func
        return decorator

    # --- Public Facade Methods ---

    def publish(self, topic: str, partition_key: str, data: Dict[str, Any]) -> Tuple[int, str]:
        """
        Publishes a message to the specified topic.

        Determines the partition using the partition_key and calls the Broker API.

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
            partition_index, message_id = self.api_client.push(topic, partition_key, data)
            return partition_index, message_id
        except Exception as e:
            logger.error(f"Failed to publish to topic '{topic}': {e}", exc_info=True)
            # Re-raise the exception for the caller to handle
            raise

    # Return type hint updated
    def create_consumer(self, topic: str, group: str) -> QTaskConsumerWorker:
        """
        Creates and configures a QTaskConsumerWorker to consume from an assigned
        partition of the specified topic/group.

        Uses the handler previously registered with the @handler decorator.

        Args:
            topic: Base topic name.
            group: Consumer group name.

        Returns:
            A QTaskConsumerWorker instance ready to be started with worker.start().

        Raises:
            ValueError: If no handler was found registered for the topic/group.
            BrokerApiException: If unrecoverable errors occur contacting the Broker API.
            RuntimeError: If a partition could not be assigned after retries.
        """
        logger.info(f"Request to create consumer for topic '{topic}', group '{group}'.")

        # 1. Find the registered handler
        handler_key = (topic, group)
        registered_handler = self._handler_registry.get(handler_key)
        if registered_handler is None:
            # Error message updated
            msg = f"No handler registered for topic '{topic}', group '{group}'. Use the @qtask_client.handler decorator."
            logger.error(msg)
            raise ValueError(msg)
        logger.debug(f"Handler '{registered_handler.__name__}' found for {handler_key}.")

        # 2. Generate a consumer ID
        consumer_id = self._generate_consumer_id(topic, group)
        logger.debug(f"Generated consumer ID: {consumer_id}")

        # 3. Get partition assignment from the API (with retries)
        try:
            logger.info(f"Requesting partition assignment from API for {consumer_id}...")
            partition_index = self.api_client.assign_partition(topic, group, consumer_id)
            logger.info(f"Partition {partition_index} assigned to {consumer_id}.")
        except Exception as e:
            logger.error(f"Could not get partition assignment for {consumer_id}: {e}", exc_info=True)
            # Re-raise the original exception (could be BrokerApiException, RuntimeError)
            raise

        # 4. (Optional but recommended) Call /subscribe to ensure group/stream
        try:
            logger.info(f"Ensuring subscription via API for partition {partition_index}...")
            subscribed = self.api_client.subscribe(topic, group, partition_index)
            if not subscribed:
                 # This shouldn't happen if assign_partition worked, but just in case
                 logger.warning(f"API indicated failure ensuring subscription for partition {partition_index}, continuing anyway...")
        except Exception as e:
            # Log but continue, as the assignment was successful
            logger.warning(f"Error calling /subscribe for partition {partition_index} (continuing): {e}", exc_info=False)


        # 5. Create the Worker instance
        # Use renamed class
        logger.info(f"Creating QTaskConsumerWorker instance for partition {partition_index}...")
        worker = QTaskConsumerWorker(
            redis_url=self.redis_url,
            topic=topic,
            group=group,
            partition_index=partition_index,
            consumer_id=consumer_id,
            handler=registered_handler # Use the registered handler
        )
        # Log message updated
        logger.info(f"QTaskConsumerWorker created for {consumer_id}. Call .start() to begin consuming.")
        return worker

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
        """Closes underlying connections (currently, the API client)."""
        # Log message updated
        logger.info("Closing QTaskClient...")
        self.api_client.close()
        logger.info("QTaskClient closed.")


# --- Example Usage ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s %(name)s - %(message)s')

    # --- Configuration ---
    BROKER_URL = "http://localhost:3000" # URL of your Broker API
    REDIS_CONN_URL = "redis://localhost:6379" # URL of your Redis

    # --- Create client instance ---
    # In a real app, this would likely be a global or injected instance
    # Use renamed class
    qtask_client = QTaskClient(broker_api_url=BROKER_URL, redis_url=REDIS_CONN_URL)

    # --- Register Handlers ---
    # Use the actual client instance name in the decorator
    @qtask_client.handler(topic="orders", group="processors")
    def handle_order(data: Dict, message_id: str, partition_index: int):
        logger.info(f"[ORDER HANDLER][P{partition_index}] Processing order {message_id}: {data.get('order_id')}")
        time.sleep(0.2) # Simulate work
        if data.get('order_id') == 'o_error':
             raise ValueError("Simulated invalid order")

    @qtask_client.handler(topic="notifications", group="senders")
    def handle_notification(data: Dict, message_id: str, partition_index: int):
        logger.info(f"[NOTIF HANDLER][P{partition_index}] Sending notification {message_id}: for {data.get('user')}")
        time.sleep(0.1)

    # --- Use the Client ---
    try:
        # List initial topics
        print("\n--- Listing Topics (Initial) ---")
        try:
            # Use renamed instance
            topics = qtask_client.list_topics()
            print(f"Topics: {topics}")
        except BrokerApiException as e:
            print(f"Error listing topics: {e}")

        # Publish messages
        print("\n--- Publishing Messages ---")
        try:
            # Use renamed instance
            p1, id1 = qtask_client.publish("orders", "customer_A", {"order_id": "o123", "amount": 100})
            print(f"Order o123 published to partition {p1}, ID: {id1}")
            p2, id2 = qtask_client.publish("orders", "customer_B", {"order_id": "o456", "amount": 50})
            print(f"Order o456 published to partition {p2}, ID: {id2}")
            p3, id3 = qtask_client.publish("notifications", "user1", {"user": "user1", "msg": "Hello"})
            print(f"Notification 1 published to partition {p3}, ID: {id3}")
            p4, id4 = qtask_client.publish("orders", "customer_C", {"order_id": "o_error", "amount": 10})
            print(f"Order o_error published to partition {p4}, ID: {id4}")

        except BrokerApiException as e:
            print(f"Error publishing: {e}")

        # List topics after publishing
        print("\n--- Listing Topics (After Publish) ---")
        try:
            # Use renamed instance
            topics = qtask_client.list_topics()
            print(f"Topics: {topics}") # Should include 'orders' and 'notifications'
        except BrokerApiException as e:
            print(f"Error listing topics: {e}")

        # Create consumers (but don't start them here in the main example)
        print("\n--- Creating Consumers ---")
        workers_to_start = []
        try:
            # Create an order consumer
            # Use renamed instance
            order_worker = qtask_client.create_consumer(topic="orders", group="processors")
            print(f"Worker for 'orders' created (ID: {order_worker.consumer_id}, Partition: {order_worker.partition_index})")
            workers_to_start.append(order_worker)

            # Create a notification consumer
            # Use renamed instance
            notif_worker = qtask_client.create_consumer(topic="notifications", group="senders")
            print(f"Worker for 'notifications' created (ID: {notif_worker.consumer_id}, Partition: {notif_worker.partition_index})")
            workers_to_start.append(notif_worker)

            # Try creating consumer without registered handler
            try:
                 print("\nAttempting to create consumer for topic/group without handler...")
                 # Use renamed instance
                 qtask_client.create_consumer(topic="inventory", group="updaters")
            except ValueError as e:
                 print(f"Expected error: {e}")


        except (BrokerApiException, RuntimeError, ValueError) as e:
            print(f"Error creating consumer: {e}")

        # In a real application, you would start the workers here or elsewhere:
        # print("\n--- Starting Workers (simulated) ---")
        # for w in workers_to_start:
        #     print(f"Starting worker {w.consumer_id}...")
        #     # w.start() # Uncomment to actually start

        # time.sleep(30) # Keep alive for processing

        # print("\n--- Stopping Workers (simulated) ---")
        # for w in workers_to_start:
        #     print(f"Stopping worker {w.consumer_id}...")
        #     # w.stop() # Uncomment to actually stop

    finally:
        # Use renamed instance
        qtask_client.close()
        # Log message updated
        print("\nQTaskClient closed.")

