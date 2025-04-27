import requests
import time
import logging
import math
import random
from typing import Dict, Any, List, Optional, Tuple

# Configure logger for the client library
logger = logging.getLogger(__name__)


# --- Custom Exceptions (Optional but recommended) ---
class BrokerApiException(Exception):
    """Base exception for errors related to the Broker API."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        original_exception: Optional[Exception] = None,
    ):
        self.status_code = status_code
        self.original_exception = original_exception
        super().__init__(message)


class BrokerConnectionError(BrokerApiException):
    """Error connecting to the Broker API."""

    pass


class BrokerRequestError(BrokerApiException):
    """Error in the request or unexpected API response (e.g., 4xx, 5xx)."""

    pass


class NoPartitionsAvailableError(BrokerRequestError):
    """Specific error when the API indicates no partitions are available (409)."""

    pass


# --- Main API Client Class ---
# Renamed from M0BrokerApiClient
class QTaskBrokerApiClient:
    """
    Synchronous HTTP client for interacting with the QTask Broker API.
    """

    DEFAULT_TIMEOUT_SECONDS = 10  # Default timeout for HTTP requests

    # --- Retry Parameters for assign_partition ---
    INITIAL_RETRY_DELAY_MS = 1000
    MAX_RETRY_DELAY_MS = 30000
    RETRY_FACTOR = 2.0
    NO_PARTITION_RETRY_DELAY_MS = 15000
    MAX_ASSIGN_ATTEMPTS = 10  # Limit to prevent infinite loops

    def __init__(self, base_url: str, timeout: int = DEFAULT_TIMEOUT_SECONDS):
        """
        Initializes the API client.

        Args:
            base_url (str): The base URL of the QTask Broker API (e.g., "http://localhost:3000").
            timeout (int): Timeout in seconds for HTTP requests.
        """
        if not base_url.endswith("/"):
            base_url += "/"
        self.base_url = base_url
        self.timeout = timeout
        # Create a requests Session to reuse connections (better performance)
        self.session = requests.Session()
        # You could add default headers here if needed
        # self.session.headers.update({'Content-Type': 'application/json'})
        # Log message updated
        logger.info(f"QTaskBrokerApiClient initialized for base URL: {self.base_url}")

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Helper method to perform HTTP requests and handle common errors."""
        url = f"{self.base_url}{endpoint}"
        request_kwargs = {"timeout": self.timeout, **kwargs}

        try:
            logger.debug(
                f"Making request: {method.upper()} {url} with kwargs: {kwargs}"
            )
            response = self.session.request(method, url, **request_kwargs)
            logger.debug(
                f"Response received: Status={response.status_code}, Body={response.text[:100]}..."
            )  # Log only the beginning of the body

            # Raise exception for HTTP errors (4xx, 5xx)
            response.raise_for_status()
            return response

        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout during request {method.upper()} {url}: {e}")
            raise BrokerConnectionError(
                f"Timeout connecting to {url}", original_exception=e
            ) from e
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error on {method.upper()} {url}: {e}")
            raise BrokerConnectionError(
                f"Could not connect to {url}", original_exception=e
            ) from e
        except requests.exceptions.RequestException as e:
            logger.error(f"Error during request {method.upper()} {url}: {e}")
            status_code = e.response.status_code if e.response is not None else None
            # Try to get error detail from response if JSON
            detail = "Unknown error"
            if e.response is not None:
                try:
                    error_data = e.response.json()
                    detail = error_data.get("detail", e.response.text)
                except requests.exceptions.JSONDecodeError:
                    detail = e.response.text

            # Raise specific exception for 409 on assign-partition
            if endpoint == "assign-partition" and status_code == 409:
                logger.warning(f"API returned 409 (Conflict) on {url}: {detail}")
                raise NoPartitionsAvailableError(
                    detail, status_code=status_code, original_exception=e
                ) from e
            else:
                logger.error(f"HTTP error {status_code} on {url}: {detail}")
                raise BrokerRequestError(
                    f"Error {status_code}: {detail}",
                    status_code=status_code,
                    original_exception=e,
                ) from e

    def assign_partition(self, topic: str, group: str, consumer_id: str) -> int:
        """
        Requests a partition assignment from the API, with retries.

        Args:
            topic: Base topic name.
            group: Consumer group name.
            consumer_id: Unique consumer ID.

        Returns:
            The assigned partition index.

        Raises:
            BrokerApiException: If an unrecoverable connection or API error occurs.
            NoPartitionsAvailableError: If the API indicates no partitions are available (after retries if applicable).
            RuntimeError: If the maximum number of assignment attempts is exceeded.
        """
        endpoint = "assign-partition"
        payload = {"topic": topic, "group": group, "consumerId": consumer_id}
        attempts = 0
        current_delay_ms = self.INITIAL_RETRY_DELAY_MS

        while attempts < self.MAX_ASSIGN_ATTEMPTS:
            attempts += 1
            logger.info(
                f"[Assigner Attempt {attempts}/{self.MAX_ASSIGN_ATTEMPTS}] Requesting partition for {consumer_id}..."
            )
            try:
                response = self._make_request("post", endpoint, json=payload)
                data = response.json()  # Assume successful response is JSON

                if data.get("success") and isinstance(data.get("partitionIndex"), int):
                    partition_index = data["partitionIndex"]
                    logger.info(
                        f"[Assigner] Partition {partition_index} assigned by API to {consumer_id}."
                    )
                    return partition_index
                else:
                    # Unexpected but successful (2xx) response?
                    logger.error(f"Unexpected response from {endpoint}: {data}")
                    raise BrokerRequestError(
                        f"Invalid response from {endpoint}",
                        status_code=response.status_code,
                    )

            except NoPartitionsAvailableError as e:
                # Specific 409 error: no partitions, retry after a long delay
                wait_ms = self.NO_PARTITION_RETRY_DELAY_MS
                logger.warning(
                    f"{e}. Retrying assignment in {wait_ms / 1000:.1f}s..."
                )
                # Reset exponential backoff counter for this case
                current_delay_ms = self.INITIAL_RETRY_DELAY_MS
                time.sleep(wait_ms / 1000)
                continue  # Retry the while loop

            except (BrokerConnectionError, BrokerRequestError) as e:
                # Other recoverable errors (connection, 5xx?) -> retry with backoff
                # Calculate delay with jitter
                jitter = current_delay_ms * 0.2 * (random.random() - 0.5)
                wait_ms = min(self.MAX_RETRY_DELAY_MS, current_delay_ms + jitter)
                logger.warning(
                    f"Recoverable error during assignment: {e}. Retrying in {wait_ms / 1000:.1f}s..."
                )
                time.sleep(wait_ms / 1000)
                # Increase delay for next time
                current_delay_ms = min(
                    self.MAX_RETRY_DELAY_MS, current_delay_ms * self.RETRY_FACTOR
                )
                continue  # Retry the while loop

            except Exception as e:
                # Unexpected, unrecoverable errors
                logger.error(
                    f"Unrecoverable error during assignment: {e}", exc_info=True
                )
                raise BrokerApiException(
                    f"Unexpected error during assignment: {e}", original_exception=e
                ) from e

        # If we exit the loop due to exceeding attempts
        logger.error(
            f"Exceeded maximum attempts ({self.MAX_ASSIGN_ATTEMPTS}) to assign partition for {consumer_id}."
        )
        raise RuntimeError(
            f"Could not assign partition for {consumer_id} after {self.MAX_ASSIGN_ATTEMPTS} attempts."
        )

    def subscribe(self, topic: str, group: str, partition_index: int) -> bool:
        """
        Calls the /subscribe endpoint to ensure the subscription.

        Args:
            topic: Base topic name.
            group: Group name.
            partition_index: Partition index.

        Returns:
            True if the subscription was ensured, False otherwise (based on response).

        Raises:
            BrokerApiException: If a connection or API error occurs.
        """
        endpoint = "subscribe"
        payload = {"topic": topic, "group": group, "partitionIndex": partition_index}
        try:
            response = self._make_request("post", endpoint, json=payload)
            data = response.json()
            success = data.get("success", False)
            if success:
                logger.info(
                    f"Subscription ensured for {topic}/{group} on partition {partition_index}."
                )
            else:
                logger.warning(
                    f"API indicated failure ensuring subscription for {topic}/{group} on partition {partition_index}: {data.get('message')}"
                )
            return success
        except BrokerApiException as e:
            logger.error(f"Error calling /subscribe: {e}")
            raise  # Re-raise the exception for the caller to handle
        except Exception as e:
            logger.error(f"Unexpected error in subscribe: {e}", exc_info=True)
            raise BrokerApiException(
                f"Unexpected error in subscribe: {e}", original_exception=e
            ) from e

    def push(
        self, topic: str, partition_key: str, data: Dict[str, Any]
    ) -> Tuple[int, str]:
        """
        Publishes a message by calling the /push endpoint.

        Args:
            topic: Base topic name.
            partition_key: Key used to determine the partition.
            data: Dictionary containing the message data.

        Returns:
            A tuple (partition_index: int, message_id: str).

        Raises:
            BrokerApiException: If a connection or API error occurs.
            ValueError: If the API response is invalid.
        """
        endpoint = "push"
        payload = {"topic": topic, "partitionKey": partition_key, "data": data}
        try:
            response = self._make_request("post", endpoint, json=payload)
            response_data = response.json()
            if (
                response.status_code == 201 # Check for Created status
                and response_data.get("success")
                and isinstance(response_data.get("partition"), int)
                and isinstance(response_data.get("messageId"), str)
            ):
                partition = response_data["partition"]
                message_id = response_data["messageId"]
                logger.info(
                    f"Message published to topic '{topic}', partition {partition}, ID: {message_id}"
                )
                return partition, message_id
            else:
                logger.error(
                    f"Unexpected response from /push: Status={response.status_code}, Body={response_data}"
                )
                raise ValueError(
                    f"Invalid response from /push endpoint: {response_data}"
                )
        except BrokerApiException as e:
            logger.error(f"Error calling /push: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in push: {e}", exc_info=True)
            raise BrokerApiException(
                f"Unexpected error in push: {e}", original_exception=e
            ) from e

    def get_topics(self) -> List[str]:
        """
        Gets the list of managed topics from the /topics endpoint.

        Returns:
            A list of topic names.

        Raises:
            BrokerApiException: If a connection or API error occurs.
            ValueError: If the API response is invalid.
        """
        endpoint = "topics"
        try:
            response = self._make_request("get", endpoint)
            data = response.json()
            if isinstance(data.get("topics"), list):
                topics = data["topics"]
                logger.info(f"Retrieved {len(topics)} managed topics.")
                return topics
            else:
                logger.error(f"Unexpected response from /topics: {data}")
                raise ValueError(f"Invalid response from /topics endpoint: {data}")
        except BrokerApiException as e:
            logger.error(f"Error calling /topics: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_topics: {e}", exc_info=True)
            raise BrokerApiException(
                f"Unexpected error in get_topics: {e}", original_exception=e
            ) from e

    def close(self):
        """Closes the underlying requests session."""
        logger.info("Closing API client HTTP session.")
        self.session.close()


# --- Example Usage ---
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    # Assume the Broker API is running at localhost:3000
    API_URL = "http://localhost:3000"
    # Renamed class instantiation
    client = QTaskBrokerApiClient(base_url=API_URL)

    try:
        # 1. Get topics (might be empty initially)
        print("\n--- Getting Topics ---")
        topics = client.get_topics()
        print(f"Current topics: {topics}")

        # 2. Publish a message
        print("\n--- Publishing Message ---")
        topic_push = "sensor_readings"
        pkey_push = "device_123"
        data_push = {"temperature": 25.5, "humidity": 60}
        try:
            part, msg_id = client.push(topic_push, pkey_push, data_push)
            print(f"Published to partition {part} with ID {msg_id}")
        except BrokerApiException as e:
            print(f"Error publishing: {e}")

        # 3. Get topics again
        print("\n--- Getting Topics (after push) ---")
        topics = client.get_topics()
        print(f"Current topics: {topics}")  # Should include 'sensor_readings'

        # 4. Try assigning a partition
        print("\n--- Assigning Partition ---")
        topic_assign = "user_actions"
        group_assign = "analytics_group"
        consumer_assign = f"consumer-{random.randint(1000, 9999)}"
        try:
            assigned_part = client.assign_partition(
                topic_assign, group_assign, consumer_assign
            )
            print(f"Partition assigned to {consumer_assign}: {assigned_part}")

            # 5. Subscribe to the assigned partition
            print("\n--- Subscribing to Partition ---")
            subscribed = client.subscribe(topic_assign, group_assign, assigned_part)
            print(f"Subscription successful: {subscribed}")

        except NoPartitionsAvailableError:
            print("No partitions available for assignment at the moment.")
        except BrokerApiException as e:
            print(f"Error during assignment/subscription: {e}")
        except RuntimeError as e:
            print(f"Error: {e}")  # E.g., max retries exceeded

    finally:
        client.close()

