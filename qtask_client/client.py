import logging
import random
import string
import time
import os
import asyncio # <--- Importar asyncio
from typing import Dict, Callable, Optional, List, Any, Tuple, Coroutine # <-- Añadir Coroutine
from urllib.parse import urlunparse

# --- Importar componentes ASÍNCRONOS ---
# Asegúrate de que estos imports apunten a los archivos refactorizados y corregidos
from .api_client import (
    QTaskBrokerApiClient, # Importar la versión async
    BrokerApiException,
    NoPartitionsAvailableError,
    BrokerConnectionError,
    BrokerRequestError
)
from .consumer_worker import QTaskConsumerWorker, MessageHandler # Importar la versión async

# Configurar logger
logger = logging.getLogger(__name__)


class QTaskClient:
    """
    Main ASYNCHRONOUS client and facade for interacting with the QTask Broker system.
    Provides async methods for publishing messages and creating/managing consumers.
    """

    # --- Default Configuration Values (Sin cambios) ---
    DEFAULT_BROKER_URL = "http://localhost:3000"
    DEFAULT_REDIS_HOST = "localhost"
    DEFAULT_REDIS_PORT = 6379
    DEFAULT_REDIS_USERNAME = None
    DEFAULT_REDIS_PASSWORD = None

    def __init__(
        self,
        broker_api_url: Optional[str] = None,
        redis_url: Optional[str] = None,
        redis_host: Optional[str] = None,
        redis_port: Optional[int] = None,
        redis_username: Optional[str] = None,
        redis_password: Optional[str] = None,
        # api_timeout: int = QTaskBrokerApiClient.DEFAULT_TIMEOUT_SECONDS # Opcional
    ):
        """Initializes the ASYNCHRONOUS QTask client."""

        # --- Determinar Broker API URL (Lógica sin cambios) ---
        if broker_api_url:
            self.broker_api_url = broker_api_url
            logger.info(f"Using provided Broker API URL: {self.broker_api_url}")
        else:
            self.broker_api_url = os.environ.get("QTASK_BROKER_URL", self.DEFAULT_BROKER_URL)
            logger.info(f"Using Broker API URL from env/default: {self.broker_api_url}")
        if not self.broker_api_url:
            raise ValueError("Could not determine Broker API URL.")

        # --- Determinar Redis URL (Lógica sin cambios) ---
        if redis_url:
            self.redis_url = redis_url
            logger.info(f"Using provided Redis URL: {self.redis_url}")
        else:
            _host = redis_host or os.environ.get("REDIS_HOST", self.DEFAULT_REDIS_HOST)
            _port_str = os.environ.get("REDIS_PORT", str(self.DEFAULT_REDIS_PORT))
            try:
                 _port = redis_port or int(_port_str)
            except (ValueError, TypeError):
                 logger.warning(f"Invalid REDIS_PORT environment variable '{_port_str}'. Using default {self.DEFAULT_REDIS_PORT}.")
                 _port = self.DEFAULT_REDIS_PORT
            _user_env = os.environ.get("REDIS_USERNAME")
            _pass_env = os.environ.get("REDIS_PASSWORD")
            _user = redis_username if redis_username is not None else _user_env
            _pass = redis_password if redis_password is not None else _pass_env
            netloc = ""
            if _user and _pass: netloc = f"{_user}:{_pass}@{_host}:{_port}"
            elif _user: netloc = f"{_user}@{_host}:{_port}"
            else:
                 if _pass: netloc = f":{_pass}@{_host}:{_port}"
                 else: netloc = f"{_host}:{_port}"
            self.redis_url = urlunparse(("redis", netloc, "", "", "", ""))
            logger.info(f"Constructed Redis URL from components/env/defaults: {self.redis_url}")
        if not self.redis_url:
            raise ValueError("Could not determine Redis URL.")

        # --- Instanciar API Client ASÍNCRONO ---
        self.api_client = QTaskBrokerApiClient(base_url=self.broker_api_url) # Asume que __init__ es sync
        # --------------------------------------
        self._handler_registry: Dict[Tuple[str, str], MessageHandler] = {}
        self._active_workers: Dict[Tuple[str, str], QTaskConsumerWorker] = {}
        self._worker_tasks: Dict[Tuple[str, str], asyncio.Task] = {}
        self._consumers_started = False
        self._client_lock = asyncio.Lock()

        logger.info(
            f"QTaskClient (async) initialized. Broker API: {self.broker_api_url}, Redis URL: {self.redis_url}"
        )

    def _generate_consumer_id(self, topic: str, group: str) -> str:
        """Generates a reasonably unique consumer ID."""
        random_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        return f"consumer-async-{topic}-{group}-{random_suffix}"

    # El decorador en sí mismo no necesita ser async
    def handler(self, topic: str, group: str) -> Callable[[MessageHandler], MessageHandler]:
        """Decorator to register a message handler (can be sync or async)."""
        if not topic or not group:
            raise ValueError("Topic and group cannot be empty in the handler decorator.")
        def decorator(func: MessageHandler) -> MessageHandler:
            key = (topic, group)
            if key in self._handler_registry:
                logger.warning(f"Overwriting existing handler for topic '{topic}', group '{group}'.")
            handler_type = "async" if asyncio.iscoroutinefunction(func) else "sync"
            logger.info(f"Registering {handler_type} handler '{func.__name__}' for topic '{topic}', group '{group}'.")
            self._handler_registry[key] = func
            return func
        return decorator

    # --- CORREGIDO: Convertido a async def ---
    async def publish(
        self, topic: str, partition_key: str, data: Dict[str, Any]
    ) -> Tuple[int, str]:
        """Publishes a message ASYNCHRONOUSLY."""
        logger.debug(f"Publish request (async): Topic={topic}, PartitionKey={partition_key}")
        try:
            # --- Usar await ---
            partition_index, message_id = await self.api_client.push(
                topic, partition_key, data
            )
            return partition_index, message_id
        except Exception as e:
            logger.error(f"Failed to publish to topic '{topic}' (async): {e}", exc_info=False)
            raise

    # --- CORREGIDO: Convertido a async def ---
    async def _create_and_start_consumer_task(self, topic: str, group: str) -> Optional[asyncio.Task]:
        """Creates and starts a single consumer worker ASYNCHRONOUSLY."""
        handler_key = (topic, group)
        registered_handler = self._handler_registry.get(handler_key)
        if registered_handler is None:
            logger.error(f"No handler registered for topic '{topic}', group '{group}'. Cannot create consumer.")
            return None

        consumer_id = self._generate_consumer_id(topic, group)
        logger.debug(f"Generated consumer ID: {consumer_id}")

        try:
            logger.info(f"Requesting partition assignment from API for {consumer_id} (async)...")
            # --- Usar await ---
            partition_index = await self.api_client.assign_partition(topic, group, consumer_id)
            logger.info(f"Partition {partition_index} assigned to {consumer_id} (async).")
        except NoPartitionsAvailableError:
            logger.warning(f"No partitions currently available for topic '{topic}', group '{group}'. Consumer not created.")
            return None
        except (BrokerApiException, RuntimeError) as e:
            logger.error(f"Could not get partition assignment for {consumer_id} (async): {e}", exc_info=True)
            return None

        try:
            logger.info(f"Ensuring subscription via API for partition {partition_index} (async)...")
            # --- Usar await ---
            subscribed = await self.api_client.subscribe(topic, group, partition_index)
            if not subscribed:
                logger.warning(f"API indicated failure ensuring subscription for partition {partition_index} (async), continuing anyway...")
        except Exception as e:
            logger.warning(f"Error calling /subscribe for partition {partition_index} (async, continuing): {e}", exc_info=False)

        logger.info(f"Creating QTaskConsumerWorker instance for partition {partition_index} (async)...")
        # --- Instanciar worker ASÍNCRONO ---
        worker = QTaskConsumerWorker(
            redis_url=self.redis_url,
            topic=topic,
            group=group,
            partition_index=partition_index,
            consumer_id=consumer_id,
            handler=registered_handler,
        )
        logger.info(f"QTaskConsumerWorker created for {consumer_id} (async).")

        # --- Iniciar worker y guardar tarea ---
        logger.info(f"Starting worker task for {consumer_id}...")
        # --- Usar await para worker.start() ---
        # worker.start() ahora es async y devuelve None al completarse (o lanza excepción)
        # Creamos la tarea para ejecutarlo en background
        task = asyncio.create_task(worker.start(), name=f"Worker-{consumer_id}")
        self._active_workers[handler_key] = worker
        self._worker_tasks[handler_key] = task
        logger.info(f"Successfully started consumer task for {consumer_id}.")
        return task # Devolver la tarea creada

    # --- CORREGIDO: Convertido a async def ---
    async def start_all_consumers(self):
        """Creates and starts consumer workers ASYNCHRONOUSLY for all registered handlers."""
        async with self._client_lock:
            if self._consumers_started:
                logger.warning("Consumers already started. Call stop_all_consumers() first if you need to restart.")
                return # Devolver None explícitamente
            if not self._handler_registry:
                logger.warning("No handlers registered. Cannot start any consumers.")
                return # Devolver None explícitamente

            logger.info(f"Starting consumers for {len(self._handler_registry)} registered handler(s) (async)...")
            self._active_workers = {}
            self._worker_tasks = {}
            start_tasks = []

            for (topic, group), handler in self._handler_registry.items():
                logger.info(f"Attempting to start consumer task for topic='{topic}', group='{group}' (async)...")
                # Llamar al método async _create_and_start_consumer_task
                start_tasks.append(self._create_and_start_consumer_task(topic, group))

            # Esperar a que todas las tareas de inicio terminen
            results = await asyncio.gather(*start_tasks, return_exceptions=True)

            successful_starts = sum(1 for res in results if isinstance(res, asyncio.Task))
            failed_starts = len(results) - successful_starts

            self._consumers_started = True
            logger.info(f"Finished starting consumers (async). {successful_starts} worker task(s) initiated. {failed_starts} failed to start.")
            # No necesita devolver nada explícitamente (devuelve None por defecto)

    # --- CORREGIDO: Convertido a async def ---
    async def stop_all_consumers(self):
        """Stops all consumer workers ASYNCHRONOUSLY."""
        async with self._client_lock:
            if not self._consumers_started:
                logger.info("No consumers were started by this client instance.")
                return

            logger.info(f"Stopping {len(self._active_workers)} active worker(s) (async)...")
            stop_tasks = []
            workers_to_remove = list(self._active_workers.keys())

            for key in workers_to_remove:
                worker = self._active_workers.pop(key, None)
                task = self._worker_tasks.pop(key, None)
                if worker:
                    logger.info(f"Requesting stop for worker {worker.consumer_id}...")
                    # --- Usar await para worker.stop() ---
                    stop_tasks.append(asyncio.create_task(worker.stop(), name=f"Stop-{worker.consumer_id}"))
                if task and not task.done():
                     logger.warning(f"Worker task for {key} still running after stop request, cancelling.")
                     task.cancel()
                     stop_tasks.append(task)

            if stop_tasks:
                 results = await asyncio.gather(*stop_tasks, return_exceptions=True)
                 logger.debug(f"Stop tasks results: {results}")

            logger.info(f"Finished stopping consumers (async).")
            self._active_workers = {}
            self._worker_tasks = {}
            self._consumers_started = False

    # --- CORREGIDO: Convertido a async def ---
    async def list_topics(self) -> List[str]:
        """Gets the list of managed base topics ASYNCHRONOUSLY."""
        logger.debug("Requesting list of topics from API (async)...")
        try:
            # --- Usar await ---
            return await self.api_client.get_topics()
        except Exception as e:
            logger.error(f"Failed to get topic list (async): {e}", exc_info=True)
            raise

    # --- CORREGIDO: Convertido a async def ---
    async def close(self):
        """Stops consumers and closes connections ASYNCHRONOUSLY."""
        logger.info("Closing QTaskClient (async)...")
        await self.stop_all_consumers() # Usa await
        await self.api_client.close()   # Usa await
        logger.info("QTaskClient closed (async).")


# --- Example Usage (Adaptado para asyncio) ---
async def main_example():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s %(name)s - %(message)s"
    )
    print("\n--- Creating Async Client Instance (using Env Vars/Defaults) ---")
    try:
        qtask_client = QTaskClient()
        print(f"Client created. Broker: {qtask_client.broker_api_url}, Redis: {qtask_client.redis_url}")
    except ValueError as e:
        print(f"Error creating client: {e}")
        return

    @qtask_client.handler(topic="config_test_async", group="testers_async")
    async def handle_config_test_async(data: Dict, message_id: str, partition_index: int):
        logger.info(f"[ASYNC HANDLER][P{partition_index}] Received test message {message_id}: {data}")
        await asyncio.sleep(0.1)

    @qtask_client.handler(topic="sync_topic", group="sync_group")
    def handle_sync_test(data: Dict, message_id: str, partition_index: int):
        logger.info(f"[SYNC HANDLER][P{partition_index}] Received test message {message_id}: {data}")
        time.sleep(0.2)

    try:
        print("\n--- Listing Topics (async) ---")
        try:
            topics = await qtask_client.list_topics() # await
            print(f"Managed Topics: {topics}")
        except BrokerApiException as e:
            print(f"Error listing topics: {e}")

        print("\n--- Publishing Test Messages (async) ---")
        try:
            await qtask_client.publish("config_test_async", "test_key_async", {"status": "ok_async"}) # await
            print("Published async test message.")
            await qtask_client.publish("sync_topic", "test_key_sync", {"status": "ok_sync"}) # await
            print("Published sync test message.")
        except BrokerApiException as e:
            print(f"Error publishing test message: {e}")

        print("\n--- Starting All Consumers (async) ---")
        await qtask_client.start_all_consumers() # await

        print("\n--- Application Running (Press Ctrl+C to stop) ---")
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
             loop.add_signal_handler(sig, stop_event.set)
        await stop_event.wait()

    except KeyboardInterrupt:
        print("\nShutdown signal received.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        logger.error("Unexpected main loop error", exc_info=True)
    finally:
        print("\n--- Shutting Down (async) ---")
        await qtask_client.close() # await
        print("Application finished.")

import signal # Necesario para el ejemplo

if __name__ == "__main__":
    print("Running QTaskClient async example...")
    try:
        asyncio.run(main_example())
    except KeyboardInterrupt:
        print("\nAsync example interrupted by user.")
    print("Async example finished.")