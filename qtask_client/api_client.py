# qtask_client/api_client.py (Refactorizado para asyncio y httpx)
import httpx  # <--- Usar httpx en lugar de requests
import asyncio # <--- Importar asyncio para sleep y manejo async
import time # <-- Aún útil para calcular delays
import logging
import math
import random
from typing import Dict, Any, List, Optional, Tuple

# Configurar logger
logger = logging.getLogger(__name__)


# --- Custom Exceptions (Sin cambios en definición, pero el origen cambiará) ---
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


# --- Main API Client Class (Async) ---
class QTaskBrokerApiClient:
    """
    ASYNCHRONOUS HTTP client for interacting with the QTask Broker API using httpx.
    """
    DEFAULT_TIMEOUT_SECONDS = 10  # Default timeout for HTTP requests

    # --- Retry Parameters (Sin cambios en valores) ---
    INITIAL_RETRY_DELAY_MS = 1000
    MAX_RETRY_DELAY_MS = 30000
    RETRY_FACTOR = 2.0
    NO_PARTITION_RETRY_DELAY_MS = 15000
    MAX_ASSIGN_ATTEMPTS = 10

    def __init__(self, base_url: str, timeout: int = DEFAULT_TIMEOUT_SECONDS):
        """
        Initializes the ASYNCHRONOUS API client.

        Args:
            base_url (str): The base URL of the QTask Broker API.
            timeout (int): Default timeout in seconds for HTTP requests.
        """
        if not base_url.endswith("/"):
            base_url += "/"
        self.base_url = base_url
        # --- Usar httpx.AsyncClient ---
        # Configurar timeouts para httpx
        timeouts = httpx.Timeout(timeout, connect=timeout*2) # Timeout general y de conexión
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=timeouts, follow_redirects=True)
        # -----------------------------
        logger.info(f"QTaskBrokerApiClient (async) initialized for base URL: {self.base_url}")

    # --- Convertido a async def ---
    async def _make_request(self, method: str, endpoint: str, **kwargs) -> httpx.Response:
        """Helper method to perform ASYNCHRONOUS HTTP requests."""
        # La URL completa se maneja con base_url en el cliente httpx
        request_kwargs = {**kwargs} # Timeout ya está en el cliente

        try:
            logger.debug(
                f"Making ASYNC request: {method.upper()} {endpoint} with kwargs: {kwargs}"
            )
            # --- Usar await y el cliente httpx ---
            response = await self._client.request(method, endpoint, **request_kwargs)
            # ------------------------------------
            logger.debug(
                f"Response received: Status={response.status_code}, Body={response.text[:100]}..."
            )

            # --- raise_for_status() en httpx ---
            response.raise_for_status()
            # ---------------------------------
            return response

        # --- Manejo de excepciones de httpx ---
        except httpx.TimeoutException as e:
            logger.error(f"Timeout during ASYNC request {method.upper()} {endpoint}: {e}")
            raise BrokerConnectionError(
                f"Timeout connecting to {self.base_url}{endpoint}", original_exception=e
            ) from e
        except httpx.RequestError as e: # Captura errores de conexión, etc.
            logger.error(f"Connection/Request error on ASYNC {method.upper()} {endpoint}: {e}")
            # Intentar obtener status code si es un HTTPStatusError
            status_code = e.response.status_code if isinstance(e, httpx.HTTPStatusError) and e.response else None
            detail = str(e) # Mensaje de error base

            if isinstance(e, httpx.HTTPStatusError) and e.response is not None:
                # Intentar obtener detalle del cuerpo de la respuesta
                try:
                    error_data = e.response.json()
                    detail = error_data.get("detail", e.response.text)
                except json.JSONDecodeError:
                    detail = e.response.text

                # Raise específico para 409 en assign-partition
                if endpoint == "assign-partition" and status_code == 409:
                    logger.warning(f"API returned 409 (Conflict) on {endpoint}: {detail}")
                    raise NoPartitionsAvailableError(
                        detail, status_code=status_code, original_exception=e
                    ) from e

            # Para otros errores HTTP o de conexión
            logger.error(f"HTTP/Request error {status_code or 'N/A'} on {endpoint}: {detail}")
            raise BrokerRequestError(
                f"Error {status_code or 'N/A'}: {detail}",
                status_code=status_code,
                original_exception=e,
            ) from e
        # ---------------------------------------

    # --- Convertido a async def ---
    async def assign_partition(self, topic: str, group: str, consumer_id: str) -> int:
        """
        Requests a partition assignment ASYNCHRONOUSLY, with retries.
        """
        endpoint = "assign-partition"
        payload = {"topic": topic, "group": group, "consumerId": consumer_id}
        attempts = 0
        current_delay_ms = self.INITIAL_RETRY_DELAY_MS

        while attempts < self.MAX_ASSIGN_ATTEMPTS:
            attempts += 1
            logger.info(
                f"[Assigner Attempt {attempts}/{self.MAX_ASSIGN_ATTEMPTS}] Requesting partition for {consumer_id} (async)..."
            )
            try:
                # --- Usar await ---
                response = await self._make_request("post", endpoint, json=payload)
                data = response.json()

                if data.get("success") and isinstance(data.get("partitionIndex"), int):
                    partition_index = data["partitionIndex"]
                    logger.info(
                        f"[Assigner] Partition {partition_index} assigned by API to {consumer_id} (async)."
                    )
                    return partition_index
                else:
                    logger.error(f"Unexpected successful response from {endpoint} (async): {data}")
                    raise BrokerRequestError(
                        f"Invalid successful response from {endpoint}",
                        status_code=response.status_code,
                    )

            except NoPartitionsAvailableError as e:
                wait_ms = self.NO_PARTITION_RETRY_DELAY_MS
                logger.warning(
                    f"{e}. Retrying assignment in {wait_ms / 1000:.1f}s (async)..."
                )
                current_delay_ms = self.INITIAL_RETRY_DELAY_MS
                # --- Usar asyncio.sleep ---
                await asyncio.sleep(wait_ms / 1000)
                # -------------------------
                continue

            except (BrokerConnectionError, BrokerRequestError) as e:
                # Asegurarse de que el error no sea un 409 ya manejado
                if isinstance(e, NoPartitionsAvailableError):
                     # Esto no debería ocurrir si la lógica anterior es correcta
                     logger.error("Caught NoPartitionsAvailableError unexpectedly in generic handler.")
                     raise # Re-lanzar para evitar bucle infinito si hay bug

                jitter = current_delay_ms * 0.2 * (random.random() - 0.5)
                wait_ms = min(self.MAX_RETRY_DELAY_MS, current_delay_ms + jitter)
                logger.warning(
                    f"Recoverable error during assignment (async): {e}. Retrying in {wait_ms / 1000:.1f}s..."
                )
                # --- Usar asyncio.sleep ---
                await asyncio.sleep(wait_ms / 1000)
                # -------------------------
                current_delay_ms = min(
                    self.MAX_RETRY_DELAY_MS, current_delay_ms * self.RETRY_FACTOR
                )
                continue

            except Exception as e:
                logger.error(
                    f"Unrecoverable error during assignment (async): {e}", exc_info=True
                )
                raise BrokerApiException(
                    f"Unexpected error during assignment (async): {e}", original_exception=e
                ) from e

        logger.error(
            f"Exceeded maximum attempts ({self.MAX_ASSIGN_ATTEMPTS}) to assign partition for {consumer_id} (async)."
        )
        raise RuntimeError(
            f"Could not assign partition for {consumer_id} after {self.MAX_ASSIGN_ATTEMPTS} attempts."
        )

    # --- Convertido a async def ---
    async def subscribe(self, topic: str, group: str, partition_index: int) -> bool:
        """
        Calls the /subscribe endpoint ASYNCHRONOUSLY.
        """
        endpoint = "subscribe"
        payload = {"topic": topic, "group": group, "partitionIndex": partition_index}
        try:
            # --- Usar await ---
            response = await self._make_request("post", endpoint, json=payload)
            data = response.json()
            success = data.get("success", False)
            if success:
                logger.info(
                    f"Subscription ensured for {topic}/{group} on partition {partition_index} (async)."
                )
            else:
                logger.warning(
                    f"API indicated failure ensuring subscription for {topic}/{group} on partition {partition_index} (async): {data.get('message')}"
                )
            return success
        except BrokerApiException as e:
            logger.error(f"Error calling /subscribe (async): {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in subscribe (async): {e}", exc_info=True)
            raise BrokerApiException(
                f"Unexpected error in subscribe (async): {e}", original_exception=e
            ) from e

    # --- Convertido a async def ---
    async def push(
        self, topic: str, partition_key: str, data: Dict[str, Any]
    ) -> Tuple[int, str]:
        """
        Publishes a message ASYNCHRONOUSLY via the /push endpoint.
        """
        endpoint = "push"
        payload = {"topic": topic, "partitionKey": partition_key, "data": data}
        try:
            # --- Usar await ---
            response = await self._make_request("post", endpoint, json=payload)
            response_data = response.json()
            # Verificar status code 201 explícitamente
            if (
                response.status_code == 201
                and response_data.get("success")
                and isinstance(response_data.get("partition"), int)
                and isinstance(response_data.get("messageId"), str)
            ):
                partition = response_data["partition"]
                message_id = response_data["messageId"]
                logger.info(
                    f"Message published (async) to topic '{topic}', partition {partition}, ID: {message_id}"
                )
                return partition, message_id
            else:
                # Loguear error si el status o el cuerpo no son los esperados
                logger.error(
                    f"Unexpected response from /push (async): Status={response.status_code}, Body={response_data}"
                )
                # Lanzar error si la respuesta no es válida, incluso si no fue un error HTTP
                raise ValueError(
                    f"Invalid response from /push endpoint (async): {response_data}"
                )
        except BrokerApiException as e:
            logger.error(f"Error calling /push (async): {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in push (async): {e}", exc_info=True)
            raise BrokerApiException(
                f"Unexpected error in push (async): {e}", original_exception=e
            ) from e

    # --- Convertido a async def ---
    async def get_topics(self) -> List[str]:
        """
        Gets the list of managed topics ASYNCHRONOUSLY.
        """
        endpoint = "topics"
        try:
            # --- Usar await ---
            response = await self._make_request("get", endpoint)
            data = response.json()
            if isinstance(data.get("topics"), list):
                topics = data["topics"]
                logger.info(f"Retrieved {len(topics)} managed topics (async).")
                return topics
            else:
                logger.error(f"Unexpected response from /topics (async): {data}")
                raise ValueError(f"Invalid response from /topics endpoint (async): {data}")
        except BrokerApiException as e:
            logger.error(f"Error calling /topics (async): {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_topics (async): {e}", exc_info=True)
            raise BrokerApiException(
                f"Unexpected error in get_topics (async): {e}", original_exception=e
            ) from e

    # --- Convertido a async def ---
    async def close(self):
        """Closes the underlying httpx client session."""
        logger.info("Closing API client HTTP session (async)...")
        # --- Usar await para cerrar cliente httpx ---
        await self._client.aclose()
        # ------------------------------------------
        logger.info("API client HTTP session closed (async).")


# --- Example Usage (Necesita adaptarse para correr con asyncio.run) ---
async def main_example():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    API_URL = "http://localhost:3000" # Ajusta si es necesario
    client = QTaskBrokerApiClient(base_url=API_URL)

    try:
        print("\n--- Getting Topics (async) ---")
        topics = await client.get_topics()
        print(f"Current topics: {topics}")

        print("\n--- Publishing Message (async) ---")
        topic_push = "sensor_readings_async"
        pkey_push = "device_async_123"
        data_push = {"temperature": 26.0, "humidity": 61}
        try:
            part, msg_id = await client.push(topic_push, pkey_push, data_push)
            print(f"Published to partition {part} with ID {msg_id}")
        except BrokerApiException as e:
            print(f"Error publishing: {e}")

        print("\n--- Getting Topics (after push, async) ---")
        topics = await client.get_topics()
        print(f"Current topics: {topics}")

        print("\n--- Assigning Partition (async) ---")
        topic_assign = "user_actions_async"
        group_assign = "analytics_group_async"
        consumer_assign = f"consumer-async-{random.randint(1000, 9999)}"
        try:
            assigned_part = await client.assign_partition(
                topic_assign, group_assign, consumer_assign
            )
            print(f"Partition assigned to {consumer_assign}: {assigned_part}")

            print("\n--- Subscribing to Partition (async) ---")
            subscribed = await client.subscribe(topic_assign, group_assign, assigned_part)
            print(f"Subscription successful: {subscribed}")

        except NoPartitionsAvailableError:
            print("No partitions available for assignment at the moment.")
        except BrokerApiException as e:
            print(f"Error during assignment/subscription: {e}")
        except RuntimeError as e:
            print(f"Error: {e}")

    finally:
        await client.close() # Usar await para cerrar

if __name__ == "__main__":
    print("Running QTaskBrokerApiClient async example...")
    # --- Usar asyncio.run para ejecutar el ejemplo async ---
    try:
        asyncio.run(main_example())
    except KeyboardInterrupt:
        print("\nAsync example interrupted.")
    print("Async example finished.")