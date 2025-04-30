# qtask_client/consumer_worker.py (Refactorizado para asyncio)
import redis.asyncio as redis # <--- Importar redis.asyncio
import asyncio # <--- Importar asyncio
import time # <-- Aún útil para calcular idle time
import json
import logging
import sys
from typing import Callable, Dict, Any, Optional, Coroutine # <-- Añadir Coroutine

# Importar excepciones de redis-py (desde el paquete base)
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
    ResponseError as RedisResponseError,
    AuthenticationError as RedisAuthenticationError,
    RedisError
)


# Configurar logger
logger = logging.getLogger(__name__)

# --- Tipo de Handler actualizado ---
# Ahora puede ser una función normal o una corutina (async def)
MessageHandler = Callable[[Dict[str, Any], str, int], Coroutine[Any, Any, Any] | Any]

class QTaskConsumerWorker:
    """
    Represents a worker that ASYNCHRONOUSLY consumes messages from a specific partition
    of a topic/group in Redis Streams using asyncio.

    Manages the Redis connection, sending heartbeats, reading new messages,
    claiming pending messages, and executing a user-provided handler.
    """
    # --- Configuration Constants (Sin cambios en valores) ---
    HEARTBEAT_INTERVAL_SECONDS = 15
    HEARTBEAT_TTL_SECONDS = 30
    REDIS_RECONNECT_DELAY_SECONDS = 5
    READ_LOOP_ERROR_DELAY_SECONDS = 5
    CLAIM_INTERVAL_SECONDS = 60
    MIN_IDLE_TIME_MS = 60000
    PENDING_BATCH_SIZE = 10
    READ_BATCH_SIZE = 1 # Leer de a uno para procesar más rápido individualmente? Ajustable.
    READ_BLOCK_MILLISECONDS = 5000
    TASK_INITIAL_DELAY_SECONDS = 0.1 # Renombrado desde THREAD_INITIAL_DELAY_SECONDS
    ACK_RETRY_ATTEMPTS = 1

    def __init__(self,
                 redis_url: str,
                 topic: str,
                 group: str,
                 partition_index: int,
                 consumer_id: str,
                 handler: MessageHandler):
        """
        Initializes the ASYNCHRONOUS Consumer Worker.

        Args:
            redis_url (str): Redis connection URL.
            topic (str): Base topic name.
            group (str): Consumer group name.
            partition_index (int): Partition index assigned.
            consumer_id (str): Unique ID for this worker.
            handler (MessageHandler): Function or Coroutine function to process messages.
        """
        self.redis_url = redis_url
        self.topic = topic
        self.group = group
        self.partition_index = partition_index
        self.consumer_id = consumer_id
        self.handler = handler # Puede ser sync o async

        self.stream_key = f"stream:{self.topic}:{self.partition_index}"
        self.heartbeat_key = f"heartbeat:{self.consumer_id}"

        # --- Usar cliente redis.asyncio ---
        self.redis_client: Optional[redis.Redis] = None
        # --- Usar primitivas asyncio ---
        self._stop_event = asyncio.Event() # Evento para señalar parada
        self._tasks: List[asyncio.Task] = [] # Lista para guardar tareas de fondo
        self._redis_lock = asyncio.Lock() # Lock asíncrono para operaciones Redis críticas
        self._is_running = False # Flag para estado

        logger.info(f"[{self.consumer_id}] Worker initialized for stream '{self.stream_key}', group '{self.group}' (async)")

    # --- Convertido a async def ---
    async def _connect_redis(self) -> bool:
        """Attempts to connect (or reconnect) to Redis ASYNCHRONOUSLY. MUST be called with _redis_lock held."""
        # Asume que el lock ya está adquirido
        if self.redis_client:
            try:
                # Usar await para ping
                if await self.redis_client.ping():
                    return True
            except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError): # Añadir asyncio.TimeoutError
                logger.warning(f"[{self.consumer_id}] Redis ping failed, attempting to reconnect...")
                try:
                    # close() en redis-py async es síncrono pero cierra el pool
                    self.redis_client.close()
                    # await self.redis_client.connection_pool.disconnect() # Podría ser necesario
                except Exception: pass # Ignorar errores al cerrar conexión rota
                self.redis_client = None
            except Exception as e:
                logger.error(f"[{self.consumer_id}] Unexpected error during Redis ping (async): {e}")
                try: self.redis_client.close()
                except Exception: pass
                self.redis_client = None

        if self.redis_client is None:
            logger.info(f"[{self.consumer_id}] Attempting to connect to Redis at {self.redis_url} (async)...")
            try:
                # Crear cliente async
                new_client = redis.from_url(self.redis_url, decode_responses=True)
                # Usar await para ping
                await new_client.ping()
                self.redis_client = new_client
                logger.info(f"[{self.consumer_id}] Successfully connected to Redis (async).")
                return True
            except (RedisConnectionError, RedisTimeoutError, RedisAuthenticationError, asyncio.TimeoutError) as e:
                logger.error(f"[{self.consumer_id}] Failed to connect to Redis (async): {e}")
                self.redis_client = None
                return False
            except Exception as e:
                logger.error(f"[{self.consumer_id}] Unexpected error connecting to Redis (async): {e}", exc_info=True)
                self.redis_client = None
                return False

        return False # No debería alcanzarse

    # --- Convertido a async def ---
    async def _ensure_group_exists(self):
        """Ensures the consumer group exists ASYNCHRONOUSLY. MUST be called with _redis_lock held."""
        if not self.redis_client:
            logger.warning(f"[{self.consumer_id}] Cannot ensure group, Redis client not connected.")
            return False
        try:
            # Usar await para xgroup_create
            await self.redis_client.xgroup_create(self.stream_key, self.group, id='$', mkstream=True)
            logger.info(f"[{self.consumer_id}] Group '{self.group}' ensured for stream '{self.stream_key}' (async).")
            return True
        except RedisResponseError as e: # Usar excepción importada
            if "BUSYGROUP" in str(e):
                logger.info(f"[{self.consumer_id}] Group '{self.group}' already exists for stream '{self.stream_key}' (async).")
                return True
            else:
                logger.error(f"[{self.consumer_id}] Error (ResponseError) ensuring group (async): {e}")
                return False
        except Exception as e:
            logger.error(f"[{self.consumer_id}] Unexpected error ensuring group (async): {e}", exc_info=True)
            return False

    # --- Convertido a async def ---
    async def _send_heartbeat_loop(self):
        """Loop executed as an asyncio Task to periodically send heartbeats."""
        logger.debug(f"[{self.consumer_id}] Starting heartbeat task...")
        await asyncio.sleep(self.TASK_INITIAL_DELAY_SECONDS)

        while not self._stop_event.is_set():
            client_to_use = None
            reconnect_needed = False
            async with self._redis_lock: # Usar lock async
                client_to_use = self.redis_client

            if client_to_use:
                try:
                    logger.debug(f"[{self.consumer_id}] Sending heartbeat (SETEX {self.heartbeat_key} {self.HEARTBEAT_TTL_SECONDS})")
                    # Usar await para set (o setex)
                    await client_to_use.set(self.heartbeat_key, 'alive', ex=self.HEARTBEAT_TTL_SECONDS)
                except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError) as e:
                    logger.warning(f"[{self.consumer_id}] Connection error sending heartbeat (async): {e}. Read loop will handle reconnect.")
                    # No modificar self.redis_client aquí
                except Exception as e:
                    logger.error(f"[{self.consumer_id}] Unexpected error sending heartbeat (async): {e}", exc_info=True)
            else:
                logger.debug(f"[{self.consumer_id}] Skipping heartbeat, client instance is None.")

            try:
                # Usar asyncio.wait_for para esperar el evento o el intervalo
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.HEARTBEAT_INTERVAL_SECONDS)
                # Si wait_for no lanza TimeoutError, significa que _stop_event fue seteado
                if self._stop_event.is_set():
                     break # Salir del bucle si el evento está seteado
            except asyncio.TimeoutError:
                # Es normal llegar aquí, simplemente continuamos el bucle
                pass
            except Exception as e:
                 logger.error(f"[{self.consumer_id}] Error in heartbeat loop wait: {e}", exc_info=True)
                 # Pequeña pausa para evitar bucle rápido en caso de error inesperado
                 await asyncio.sleep(1)

        logger.info(f"[{self.consumer_id}] Heartbeat task stopped.")

    # --- Convertido a async def ---
    async def _process_single_message(self, message_id: str, message_data: Dict[str, Any]):
        """Processes a single message: parses, calls handler (sync or async), and acknowledges."""
        if self._stop_event.is_set():
             logger.debug(f"[{self.consumer_id}] Skipping message {message_id} processing (stopping).")
             return

        logger.info(f"[{self.consumer_id}][P{self.partition_index}] Processing message ID: {message_id}")
        payload_str = message_data.get('payload')

        if payload_str is None:
            logger.warning(f"[{self.consumer_id}] Message {message_id} has no 'payload' field. Attempting ACK.")
            await self._acknowledge_message(message_id)
            return

        handler_success = False
        try:
            data = json.loads(payload_str)

            # --- Llamar al handler (puede ser sync o async) ---
            if asyncio.iscoroutinefunction(self.handler):
                # Si el handler es async def, usar await
                logger.debug(f"[{self.consumer_id}] Awaiting async handler for {message_id}...")
                await self.handler(data, message_id, self.partition_index)
            else:
                # Si el handler es síncrono, ejecutarlo en threadpool para no bloquear event loop
                # (Esto es crucial si el handler síncrono hace I/O bloqueante)
                logger.debug(f"[{self.consumer_id}] Running sync handler in threadpool for {message_id}...")
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.handler, data, message_id, self.partition_index)
            # -------------------------------------------------

            handler_success = True
            logger.info(f"[{self.consumer_id}] Handler executed successfully for {message_id}.")

        except json.JSONDecodeError as e:
            logger.error(f"[{self.consumer_id}] Failed to parse JSON for message {message_id}: {e}")
            # No hacer ACK
        except Exception as e:
            logger.error(f"[{self.consumer_id}] Error executing handler for message {message_id}: {e}", exc_info=True)
            # No hacer ACK

        # Acknowledge solo si el handler tuvo éxito
        if handler_success:
            await self._acknowledge_message(message_id)

    # --- Convertido a async def ---
    async def _acknowledge_message(self, message_id: str):
        """Internal helper to acknowledge a message ASYNCHRONOUSLY with retry."""
        if self._stop_event.is_set(): return

        attempts = 0
        acknowledged = False
        while attempts <= self.ACK_RETRY_ATTEMPTS and not acknowledged and not self._stop_event.is_set():
            client_to_use = None
            reconnect_needed = False
            async with self._redis_lock:
                client_to_use = self.redis_client

            if client_to_use:
                try:
                    # Usar await para xack
                    ack_result = await client_to_use.xack(self.stream_key, self.group, message_id)
                    logger.info(f"[{self.consumer_id}] Message {message_id} acknowledged (ACK: {ack_result})")
                    acknowledged = True
                except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError) as e:
                    logger.warning(f"[{self.consumer_id}] Connection/Timeout error acknowledging message {message_id} (Attempt {attempts+1}/{self.ACK_RETRY_ATTEMPTS+1}): {e}")
                    reconnect_needed = True
                except Exception as e:
                    logger.error(f"[{self.consumer_id}] Unexpected error acknowledging message {message_id}: {e}")
                    break # Salir en errores inesperados
            else:
                logger.warning(f"[{self.consumer_id}] Cannot ACK message {message_id}, Redis client is None (Attempt {attempts+1}/{self.ACK_RETRY_ATTEMPTS+1}).")
                reconnect_needed = True

            if reconnect_needed and attempts < self.ACK_RETRY_ATTEMPTS:
                 logger.info(f"[{self.consumer_id}] Attempting reconnect before retrying ACK for {message_id}...")
                 async with self._redis_lock:
                      await self._connect_redis() # Conectar con lock adquirido
                 await asyncio.sleep(0.1) # Pequeña pausa

            attempts += 1

        if not acknowledged and not self._stop_event.is_set():
             logger.error(f"[{self.consumer_id}] Failed to acknowledge message {message_id} after {attempts} attempts.")

    # --- Convertido a async def ---
    async def _claim_and_process_pending(self):
        """Attempts to claim and process pending messages ASYNCHRONOUSLY."""
        if self._stop_event.is_set(): return

        logger.info(f"[{self.consumer_id}] Checking for pending messages (min_idle_time={self.MIN_IDLE_TIME_MS}ms)...")
        try:
            start_id = '0-0'
            total_claimed_in_run = 0
            while not self._stop_event.is_set():
                client_to_use = None
                reconnect_needed = False
                async with self._redis_lock:
                    client_to_use = self.redis_client

                if not client_to_use:
                    logger.warning(f"[{self.consumer_id}] Cannot claim pending messages, Redis client is None. Waiting...")
                    await asyncio.sleep(self.REDIS_RECONNECT_DELAY_SECONDS)
                    continue # Volver a intentar obtener cliente

                # --- Realizar XAUTOCLAIM async ---
                claimed_messages = []
                next_id = start_id
                try:
                    # Usar await para xautoclaim
                    result = await client_to_use.xautoclaim(
                        name=self.stream_key,
                        groupname=self.group,
                        consumername=self.consumer_id,
                        min_idle_time=self.MIN_IDLE_TIME_MS,
                        start_id=start_id,
                        count=self.PENDING_BATCH_SIZE
                    )
                    # xautoclaim devuelve None si no hay nada o si el stream no existe
                    if result:
                        next_id = result[0]
                        claimed_messages = result[1]
                    else:
                        next_id = None # O '0-0' si queremos seguir intentando desde el principio
                        claimed_messages = []

                except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError) as e:
                     logger.warning(f"[{self.consumer_id}] Connection error during XAUTOCLAIM (async): {e}. Read loop will handle reconnect.")
                     await asyncio.sleep(self.REDIS_RECONNECT_DELAY_SECONDS)
                     continue # Reintentar ciclo exterior
                except Exception as claim_err:
                     logger.error(f"[{self.consumer_id}] Error during XAUTOCLAIM execution (async): {claim_err}", exc_info=True)
                     break # Salir del bucle de claim

                # --- Procesar mensajes reclamados ---
                if not claimed_messages:
                    log_msg = f"[{self.consumer_id}] No {'initial ' if start_id == '0-0' else ''}pending messages found."
                    logger.debug(log_msg)
                    break # Salir del bucle de claim

                logger.info(f"[{self.consumer_id}][Pending] Claimed {len(claimed_messages)} pending messages.")
                total_claimed_in_run += len(claimed_messages)
                # Procesar concurrentemente? O secuencial? Por ahora secuencial.
                for message_id, message_data in claimed_messages:
                    if self._stop_event.is_set(): break
                    # Usar await para procesar (que a su vez llama a _acknowledge_message)
                    await self._process_single_message(message_id, message_data)

                if self._stop_event.is_set(): break

                # Si Redis devuelve un next_id nulo o '0-0', terminamos
                if not next_id or next_id == '0-0':
                     logger.debug(f"[{self.consumer_id}][Pending] Redis indicated end of pending messages.")
                     break
                else:
                     start_id = next_id # Continuar reclamando desde el nuevo ID

                await asyncio.sleep(0.1) # Pausa si hay muchos mensajes

            # Log final
            log_final = f"[{self.consumer_id}] Pending message check completed."
            if total_claimed_in_run > 0: log_final += f" Claimed in this run: {total_claimed_in_run}."
            logger.info(log_final)

        except Exception as e:
            logger.error(f"[{self.consumer_id}] Unexpected error during pending message claiming loop (async): {e}", exc_info=True)

    # --- Convertido a async def ---
    async def _run_claim_cycle(self):
        """Executes claim cycles periodically using asyncio.sleep."""
        logger.debug(f"[{self.consumer_id}] Starting claim cycle task...")
        await asyncio.sleep(self.TASK_INITIAL_DELAY_SECONDS) # Delay inicial

        while not self._stop_event.is_set():
             if self.CLAIM_INTERVAL_SECONDS <= 0:
                  logger.info(f"[{self.consumer_id}] Claim cycle disabled (interval <= 0).")
                  break # Salir si está deshabilitado

             try:
                 await self._claim_and_process_pending()
             except Exception as e:
                  logger.error(f"[{self.consumer_id}] Error in claim cycle execution (async): {e}", exc_info=True)

             # Esperar para el próximo ciclo o hasta que se detenga
             try:
                 await asyncio.wait_for(self._stop_event.wait(), timeout=self.CLAIM_INTERVAL_SECONDS)
                 if self._stop_event.is_set(): break # Salir si se detuvo
             except asyncio.TimeoutError:
                 pass # Continuar al siguiente ciclo
             except Exception as e:
                  logger.error(f"[{self.consumer_id}] Error in claim cycle wait: {e}", exc_info=True)
                  await asyncio.sleep(1) # Pausa corta en error inesperado

        logger.info(f"[{self.consumer_id}] Claim cycle task stopped.")


    # --- Convertido a async def ---
    async def _read_loop(self):
        """Main loop executed as an asyncio Task to read new messages."""
        logger.debug(f"[{self.consumer_id}] Starting read loop task...")
        last_error_time = 0
        await asyncio.sleep(self.TASK_INITIAL_DELAY_SECONDS)

        while not self._stop_event.is_set():
            reconnect_attempted = False
            # --- Chequeo de conexión y reconexión ---
            async with self._redis_lock:
                if not self.redis_client:
                     reconnect_needed = True
                else:
                     try:
                          if not await self.redis_client.ping():
                               logger.warning(f"[{self.consumer_id}] Read loop: Redis ping failed. Attempting reconnect...")
                               reconnect_needed = True
                          else:
                               reconnect_needed = False
                     except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError) as e:
                          logger.warning(f"[{self.consumer_id}] Read loop: Redis connection error on ping ({e}). Attempting reconnect...")
                          reconnect_needed = True
                     except Exception as ping_err:
                          logger.error(f"[{self.consumer_id}] Read loop: Unexpected error during ping: {ping_err}")
                          reconnect_needed = True

                if reconnect_needed:
                    logger.info(f"[{self.consumer_id}] Read loop: Attempting reconnect...")
                    reconnect_attempted = True
                    if await self._connect_redis(): # Conectar con lock adquirido
                        await self._ensure_group_exists() # Asegurar grupo con lock adquirido
                        logger.info(f"[{self.consumer_id}] Read loop: Reconnected successfully.")
                    else:
                        logger.info(f"[{self.consumer_id}] Read loop: Reconnect failed. Retrying in {self.REDIS_RECONNECT_DELAY_SECONDS}s...")
                        # Salir del with lock ANTES de dormir
                        # El sleep ocurrirá fuera del lock
                        needs_sleep = True
                    needs_sleep = reconnect_needed and not self.redis_client # Dormir solo si falló la reconexión

            # Dormir fuera del lock si la reconexión falló
            if needs_sleep:
                 await asyncio.sleep(self.REDIS_RECONNECT_DELAY_SECONDS)
                 continue # Volver al inicio del bucle para reintentar conexión

            # Si acabamos de intentar reconectar (con o sin éxito), volver a evaluar
            if reconnect_attempted:
                 continue

            # --- Proceder con la lectura ---
            client_to_use = None
            async with self._redis_lock: # Obtener cliente bajo lock
                client_to_use = self.redis_client

            if not client_to_use:
                 logger.warning(f"[{self.consumer_id}] Read loop: Client became None unexpectedly after check. Retrying loop.")
                 await asyncio.sleep(0.1)
                 continue

            # *** AÑADIR LOGGING AQUÍ ***
            logger.debug(f"[{self.consumer_id}] ===> Attempting XREADGROUP on {self.stream_key} with ID '>' (BLOCK={self.READ_BLOCK_MILLISECONDS}ms)")

            try:
                logger.debug(f"[{self.consumer_id}] Waiting for new messages (BLOCK={self.READ_BLOCK_MILLISECONDS}ms)...")
                # Usar await para xreadgroup
                response = await client_to_use.xreadgroup(
                    groupname=self.group,
                    consumername=self.consumer_id,
                    streams={self.stream_key: '>'}, # Leer solo mensajes nuevos
                    count=self.READ_BATCH_SIZE,
                    block=self.READ_BLOCK_MILLISECONDS # Timeout para esperar mensajes
                )

                if response:
                    logger.info(f"[{self.consumer_id}] ===> XREADGROUP SUCCESS! Received raw response: {response}")
                else:
                    # Loguear cuando no recibe nada después de esperar
                    logger.debug(f"[{self.consumer_id}] ===> XREADGROUP returned empty response (timeout or no new messages).")
    
                if self._stop_event.is_set(): break

                if response:
                    logger.debug(f"[{self.consumer_id}] Entering message processing for {len(response[0][1])} message(s)...")
                    for stream, messages in response:
                        logger.info(f"[{self.consumer_id}][New] Received {len(messages)} new messages from {stream}.")
                        # Procesar concurrentemente? O secuencial? Por ahora secuencial.
                        for message_id, message_data in messages:
                            if self._stop_event.is_set(): break
                            # Usar await para procesar
                            await self._process_single_message(message_id, message_data)
                        if self._stop_event.is_set(): break
                # else: No new messages

                last_error_time = 0 # Resetear timestamp de error

            except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError) as e:
                logger.warning(f"[{self.consumer_id}] ===> XREADGROUP communication error: {type(e).__name__} - {e}")
                logger.warning(f"[{self.consumer_id}] Connection/Timeout error in read loop (XREADGROUP async): {e}. Will attempt reconnect on next iteration.")
                async with self._redis_lock: # Adquirir lock para modificar cliente
                    try:
                        if self.redis_client == client_to_use: # Asegurarse que es la misma instancia
                             self.redis_client.close()
                             self.redis_client = None
                    except Exception: pass
                # No dormir aquí, el chequeo al inicio del bucle lo hará
            except Exception as e:
                logger.error(f"[{self.consumer_id}] ===> Unexpected XREADGROUP error: {type(e).__name__} - {e}", exc_info=True)
                current_time = time.time()
                # Loguear error solo si ha pasado un tiempo para evitar spam
                if current_time - last_error_time > self.READ_LOOP_ERROR_DELAY_SECONDS:
                    logger.error(f"[{self.consumer_id}] Unexpected error in read loop (async): {e}", exc_info=True)
                    last_error_time = current_time
                # Esperar antes de continuar
                await asyncio.sleep(self.READ_LOOP_ERROR_DELAY_SECONDS)

        logger.info(f"[{self.consumer_id}] Read loop task stopped.")

    # --- Convertido a async def ---
    async def start(self):
        """Starts the worker ASYNCHRONOUSLY: connects, starts background tasks."""
        if self._is_running:
             logger.warning(f"[{self.consumer_id}] Worker is already running.")
             return

        logger.info(f"[{self.consumer_id}] Starting worker (async)...")
        self._stop_event.clear()
        self._tasks = [] # Limpiar lista de tareas

        # --- Conexión inicial ---
        async with self._redis_lock:
            if not await self._connect_redis() or not await self._ensure_group_exists():
                logger.error(f"[{self.consumer_id}] Failed to connect or ensure group during async startup. Worker cannot start.")
                # Intentar cerrar si se conectó parcialmente
                if self.redis_client:
                     try: self.redis_client.close()
                     except Exception: pass
                     self.redis_client = None
                return # No iniciar tareas

        # --- Iniciar Tareas de Fondo ---
        try:
            logger.info(f"[{self.consumer_id}] Creating background tasks...")
            # Crear tareas asyncio para los bucles
            self._tasks.append(asyncio.create_task(self._send_heartbeat_loop(), name=f"{self.consumer_id}-Heartbeat"))
            self._tasks.append(asyncio.create_task(self._read_loop(), name=f"{self.consumer_id}-ReadLoop"))
            if self.CLAIM_INTERVAL_SECONDS > 0:
                 self._tasks.append(asyncio.create_task(self._run_claim_cycle(), name=f"{self.consumer_id}-ClaimCycle"))
            else:
                 logger.info(f"[{self.consumer_id}] Pending message claiming cycle disabled.")

            self._is_running = True
            logger.info(f"[{self.consumer_id}] Worker started successfully with {len(self._tasks)} background tasks.")

        except Exception as e:
             logger.error(f"[{self.consumer_id}] Error creating background tasks: {e}", exc_info=True)
             # Si falla al crear tareas, intentar detener las que se hayan podido crear
             await self.stop()


    # --- Convertido a async def ---
    async def stop(self):
        """Stops the worker gracefully ASYNCHRONOUSLY."""
        if not self._is_running or self._stop_event.is_set():
            logger.warning(f"[{self.consumer_id}] Worker is already stopping or stopped.")
            return

        logger.info(f"[{self.consumer_id}] Stopping worker (async)...")
        self._stop_event.set() # Señalar a las tareas que deben parar

        # --- Cancelar y esperar tareas ---
        logger.debug(f"[{self.consumer_id}] Cancelling {len(self._tasks)} background tasks...")
        # Cancelar todas las tareas primero
        for task in self._tasks:
            if not task.done():
                task.cancel()

        # Esperar a que todas las tareas terminen (o sean canceladas)
        # Usar return_exceptions=True para que no pare si una tarea lanza excepción al cancelar
        results = await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.debug(f"[{self.consumer_id}] Background tasks finished or cancelled. Results: {results}")

        self._tasks = [] # Limpiar lista

        # --- Limpiar Recursos Redis ---
        async with self._redis_lock:
            if self.redis_client:
                # Borrar heartbeat
                try:
                    logger.info(f"[{self.consumer_id}] Deleting heartbeat key: {self.heartbeat_key}")
                    await self.redis_client.delete(self.heartbeat_key)
                except Exception as e:
                    logger.warning(f"[{self.consumer_id}] Could not delete heartbeat key on stop (async): {e}")

                # Cerrar conexión
                try:
                    logger.info(f"[{self.consumer_id}] Closing Redis connection (async)...")
                    self.redis_client.close() # close() es sync
                    # await self.redis_client.connection_pool.disconnect() # Podría ser necesario
                except Exception as e:
                    logger.error(f"[{self.consumer_id}] Error closing Redis connection (async): {e}")
                finally:
                     self.redis_client = None

        self._is_running = False
        logger.info(f"[{self.consumer_id}] Worker stopped (async).")

# --- Example Usage (Adaptado para asyncio) ---
async def main_example():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s %(name)s - %(message)s')
    REDIS_URL_EXAMPLE = "redis://localhost:6379"
    EXAMPLE_TOPIC = "my_async_event"
    EXAMPLE_GROUP = "async_processors"
    EXAMPLE_PARTITION_INDEX = 0
    EXAMPLE_CONSUMER_ID = f"worker-async-{EXAMPLE_TOPIC}-{EXAMPLE_GROUP}-p{EXAMPLE_PARTITION_INDEX}-{random.randint(1000,9999)}"
    processed_count = 0
    processed_ids = set()
    lock = asyncio.Lock() # Usar asyncio.Lock

    async def simple_async_handler(data: Dict[str, Any], message_id: str, partition_index: int):
        nonlocal processed_count
        async with lock: # Usar async with
            processed_count += 1
            current_count = processed_count
            processed_ids.add(message_id)
        logger.info(f"[Example Handler][P{partition_index}] Message #{current_count} (ID: {message_id}) received: {data}")
        await asyncio.sleep(random.uniform(0.1, 0.3)) # Usar asyncio.sleep
        logger.info(f"[Example Handler][P{partition_index}] Message #{current_count} (ID: {message_id}) processed.")

    worker = QTaskConsumerWorker(
        redis_url=REDIS_URL_EXAMPLE, topic=EXAMPLE_TOPIC, group=EXAMPLE_GROUP,
        partition_index=EXAMPLE_PARTITION_INDEX, consumer_id=EXAMPLE_CONSUMER_ID,
        handler=simple_async_handler # Pasar el handler async
    )

    # Iniciar el worker usando await
    await worker.start()

    try:
        # Mantener el ejemplo corriendo
        while True:
             await asyncio.sleep(10)
             async with lock: # Acceder a la variable compartida con lock
                  count = processed_count
             logger.info(f"[Main] Worker active. Messages processed so far: {count}")
             # Podrías añadir lógica para publicar mensajes aquí si quieres probar
    except asyncio.CancelledError:
         logger.info("[Main] Main task cancelled.")
    except KeyboardInterrupt:
         logger.info("[Main] Ctrl+C detected. Stopping worker...")
    except Exception as e:
         logger.error(f"[Main] Unexpected error: {e}", exc_info=True)
    finally:
         logger.info("[Main] Stopping worker...")
         await worker.stop() # Usar await para detener
         logger.info("[Main] Program finished.")
         logger.info(f"[Main] Total unique messages processed: {len(processed_ids)}")


if __name__ == "__main__":
    print("Running QTaskConsumerWorker async example...")
    try:
        # Usar asyncio.run para el ejemplo async
        asyncio.run(main_example())
    except KeyboardInterrupt:
        print("\nAsync example interrupted by user.")
    print("Async example finished.")