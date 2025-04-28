import redis
import threading
import time
import json
import logging
import sys
from typing import Callable, Dict, Any, Optional

# Configure logger for this module
logger = logging.getLogger(__name__)

# Type hint for the message handler function
MessageHandler = Callable[[Dict[str, Any], str, int], Any]

class QTaskConsumerWorker:
    """
    Represents a worker that consumes messages from a specific partition
    of a topic/group in Redis Streams.

    Manages the Redis connection, sending heartbeats, reading new messages,
    claiming pending messages, and executing a user-provided handler
    to process the messages.
    """
    # --- Configuration Constants ---
    HEARTBEAT_INTERVAL_SECONDS = 15
    HEARTBEAT_TTL_SECONDS = 30
    REDIS_RECONNECT_DELAY_SECONDS = 5
    READ_LOOP_ERROR_DELAY_SECONDS = 5
    CLAIM_INTERVAL_SECONDS = 60
    MIN_IDLE_TIME_MS = 60000
    PENDING_BATCH_SIZE = 10
    READ_BATCH_SIZE = 1
    READ_BLOCK_MILLISECONDS = 5000
    # --- NEW: Short initial delay for threads ---
    THREAD_INITIAL_DELAY_SECONDS = 0.1

    def __init__(self,
                 redis_url: str,
                 topic: str,
                 group: str,
                 partition_index: int,
                 consumer_id: str,
                 handler: MessageHandler):
        """
        Initializes the Consumer Worker.

        Args:
            redis_url (str): Redis connection URL.
            topic (str): Base topic name.
            group (str): Consumer group name.
            partition_index (int): Partition index assigned.
            consumer_id (str): Unique ID for this worker.
            handler (MessageHandler): Function to process messages.
        """
        self.redis_url = redis_url
        self.topic = topic
        self.group = group
        self.partition_index = partition_index
        self.consumer_id = consumer_id
        self.handler = handler

        self.stream_key = f"stream:{self.topic}:{self.partition_index}"
        self.heartbeat_key = f"heartbeat:{self.consumer_id}"

        self.redis_client: Optional[redis.Redis] = None
        self._stop_event = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._read_loop_thread: Optional[threading.Thread] = None
        self._claim_timer: Optional[threading.Timer] = None

        logger.info(f"[{self.consumer_id}] Worker initialized for stream '{self.stream_key}', group '{self.group}'")

    def _connect_redis(self) -> bool:
        """Attempts to connect (or reconnect) to Redis."""
        try:
            if self.redis_client and self.redis_client.ping():
                return True
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            logger.warning(f"[{self.consumer_id}] Redis ping failed, attempting to reconnect...")
            self.redis_client = None
        except Exception as e:
             logger.error(f"[{self.consumer_id}] Unexpected error during Redis ping: {e}")
             self.redis_client = None

        logger.info(f"[{self.consumer_id}] Attempting to connect to Redis at {self.redis_url}...")
        try:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            self.redis_client.ping()
            logger.info(f"[{self.consumer_id}] Successfully connected to Redis.")
            return True
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, redis.exceptions.AuthenticationError) as e:
            logger.error(f"[{self.consumer_id}] Failed to connect to Redis: {e}")
            self.redis_client = None
            return False
        except Exception as e:
            logger.error(f"[{self.consumer_id}] Unexpected error connecting to Redis: {e}", exc_info=True)
            self.redis_client = None
            return False

    def _ensure_group_exists(self):
        """Ensures the consumer group exists for the stream (using MKSTREAM)."""
        if not self.redis_client:
            logger.warning(f"[{self.consumer_id}] Cannot ensure group, Redis client not connected.")
            return False
        try:
            self.redis_client.xgroup_create(self.stream_key, self.group, id='$', mkstream=True)
            logger.info(f"[{self.consumer_id}] Group '{self.group}' ensured for stream '{self.stream_key}'.")
            return True
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(f"[{self.consumer_id}] Group '{self.group}' already exists for stream '{self.stream_key}'.")
                return True
            else:
                logger.error(f"[{self.consumer_id}] Error (ResponseError) ensuring group: {e}")
                return False
        except Exception as e:
            logger.error(f"[{self.consumer_id}] Unexpected error ensuring group: {e}", exc_info=True)
            return False

    def _send_heartbeat_loop(self):
        """Loop executed in a thread to periodically send heartbeats."""
        logger.debug(f"[{self.consumer_id}] Starting heartbeat loop...")
        # --- REFINEMENT: Add initial delay ---
        time.sleep(self.THREAD_INITIAL_DELAY_SECONDS)

        while not self._stop_event.is_set():
            # Check connection status *before* attempting to send
            if self.redis_client and self.redis_client.connection:
                try:
                    logger.debug(f"[{self.consumer_id}] Sending heartbeat (SET {self.heartbeat_key} EX {self.HEARTBEAT_TTL_SECONDS})")
                    self.redis_client.set(self.heartbeat_key, 'alive', ex=self.HEARTBEAT_TTL_SECONDS)
                except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                    logger.warning(f"[{self.consumer_id}] Connection error sending heartbeat: {e}. Will attempt reconnect.")
                    self.redis_client = None # Signal need for reconnection (read loop will handle reconnect)
                except Exception as e:
                    logger.error(f"[{self.consumer_id}] Unexpected error sending heartbeat: {e}", exc_info=True)
            else:
                # Log less severely if connection is missing, as read loop handles reconnects
                logger.debug(f"[{self.consumer_id}] Skipping heartbeat, client not connected or connection invalid.")

            self._stop_event.wait(self.HEARTBEAT_INTERVAL_SECONDS)
        logger.info(f"[{self.consumer_id}] Heartbeat loop stopped.")

    def _process_single_message(self, message_id: str, message_data: Dict[str, Any]):
        """Processes a single message: parses, calls handler, and acknowledges (ACKs)."""
        if self._stop_event.is_set() or not self.redis_client:
            logger.debug(f"[{self.consumer_id}] Skipping message {message_id} processing (stopping or no client).")
            return

        logger.info(f"[{self.consumer_id}][P{self.partition_index}] Processing message ID: {message_id}")
        payload_str = message_data.get('payload')

        if payload_str is None:
            logger.warning(f"[{self.consumer_id}] Message {message_id} has no 'payload' field. Acknowledging anyway.")
            try:
                # Ensure client is valid before acking
                if self.redis_client and self.redis_client.connection:
                    self.redis_client.xack(self.stream_key, self.group, message_id)
                else:
                    logger.warning(f"[{self.consumer_id}] Cannot ACK message {message_id} without payload, Redis disconnected.")
            except Exception as ack_err:
                logger.error(f"[{self.consumer_id}] Error acknowledging message {message_id} without payload: {ack_err}")
            return

        handler_success = False
        try:
            data = json.loads(payload_str)
            self.handler(data, message_id, self.partition_index)
            handler_success = True
            logger.info(f"[{self.consumer_id}] Handler executed successfully for {message_id}.")

        except json.JSONDecodeError as e:
            logger.error(f"[{self.consumer_id}] Failed to parse JSON for message {message_id}: {e}")
            logger.debug(f"[{self.consumer_id}] Raw payload (first 200 chars): {payload_str[:200]}...")
        except Exception as e:
            logger.error(f"[{self.consumer_id}] Error executing handler for message {message_id}: {e}", exc_info=True)

        if handler_success:
            try:
                # Ensure client is valid before acking
                if self.redis_client and self.redis_client.connection:
                    ack_result = self.redis_client.xack(self.stream_key, self.group, message_id)
                    logger.info(f"[{self.consumer_id}] Message {message_id} acknowledged (ACK: {ack_result})")
                else:
                     logger.warning(f"[{self.consumer_id}] Cannot ACK message {message_id}, Redis disconnected.")
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                 logger.error(f"[{self.consumer_id}] Connection/Timeout error acknowledging message {message_id}: {e}")
            except Exception as e:
                logger.error(f"[{self.consumer_id}] Unexpected error acknowledging message {message_id}: {e}")

    def _claim_and_process_pending(self):
        """Attempts to claim and process pending messages using XAUTOCLAIM."""
        if self._stop_event.is_set() or not self.redis_client:
            return
        logger.info(f"[{self.consumer_id}] Checking for pending messages (min_idle_time={self.MIN_IDLE_TIME_MS}ms)...")
        try:
            start_id = '0-0'
            total_claimed_in_run = 0
            while not self._stop_event.is_set():
                # Ensure client is valid before claiming
                if not self.redis_client or not self.redis_client.connection:
                    logger.warning(f"[{self.consumer_id}] Cannot claim pending messages, Redis disconnected. Waiting for reconnect...")
                    # Wait for the read loop to potentially reconnect
                    time.sleep(self.REDIS_RECONNECT_DELAY_SECONDS)
                    # Re-check connection status in the next iteration
                    continue

                result = self.redis_client.xautoclaim(
                    name=self.stream_key,
                    groupname=self.group,
                    consumername=self.consumer_id,
                    min_idle_time=self.MIN_IDLE_TIME_MS,
                    start_id=start_id,
                    count=self.PENDING_BATCH_SIZE
                )

                next_id = result[0]
                claimed_messages = result[1]

                if not claimed_messages:
                    if start_id == '0-0':
                        logger.debug(f"[{self.consumer_id}] No initial pending messages found to claim.") # Changed to debug
                    else:
                        logger.debug(f"[{self.consumer_id}] No more pending messages found to claim (checked up to {start_id}).") # Changed to debug
                    break

                logger.info(f"[{self.consumer_id}][Pending] Claimed {len(claimed_messages)} pending messages.")
                total_claimed_in_run += len(claimed_messages)
                for message_id, message_data in claimed_messages:
                    if self._stop_event.is_set(): break
                    self._process_single_message(message_id, message_data)

                if not next_id or next_id == '0-0':
                     logger.debug(f"[{self.consumer_id}][Pending] Redis indicated end of pending messages.") # Changed to debug
                     break
                else:
                     start_id = next_id

                time.sleep(0.1)

            if total_claimed_in_run > 0:
                 logger.info(f"[{self.consumer_id}] Pending message check completed. Claimed in this run: {total_claimed_in_run}.")
            else:
                 logger.debug(f"[{self.consumer_id}] Pending message check completed. Nothing to claim.") # Changed to debug


        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            logger.warning(f"[{self.consumer_id}] Connection error during pending message claiming: {e}")
            # Let the read loop handle reconnection
        except Exception as e:
            logger.error(f"[{self.consumer_id}] Error during pending message claiming: {e}", exc_info=True)

    def _schedule_next_claim(self):
        """Schedules the next run of _claim_and_process_pending if enabled."""
        if self._claim_timer:
            self._claim_timer.cancel()
            self._claim_timer = None

        if self.CLAIM_INTERVAL_SECONDS > 0 and not self._stop_event.is_set():
            logger.debug(f"[{self.consumer_id}] Scheduling next pending check in {self.CLAIM_INTERVAL_SECONDS}s.")
            self._claim_timer = threading.Timer(self.CLAIM_INTERVAL_SECONDS, self._run_claim_cycle)
            self._claim_timer.daemon = True
            self._claim_timer.start()

    def _run_claim_cycle(self):
        """Executes one claim cycle and schedules the next one."""
        if self._stop_event.is_set():
            return
        self._claim_and_process_pending()
        self._schedule_next_claim()

    def _read_loop(self):
        """Main loop executed in a thread to read new messages."""
        logger.debug(f"[{self.consumer_id}] Starting read loop...")
        last_error_time = 0
        # --- REFINEMENT: Add initial delay ---
        time.sleep(self.THREAD_INITIAL_DELAY_SECONDS)

        while not self._stop_event.is_set():
            # --- Primary Connection Check and Reconnect Logic ---
            if not self.redis_client or not self.redis_client.connection:
                logger.warning(f"[{self.consumer_id}] Read loop: Redis disconnected. Attempting reconnect...")
                if self._connect_redis():
                    # If reconnected, ensure group exists again
                    self._ensure_group_exists()
                    logger.info(f"[{self.consumer_id}] Read loop: Reconnected successfully.")
                    # Continue to reading immediately after successful reconnect
                else:
                    # Wait before retrying connection in the loop
                    logger.info(f"[{self.consumer_id}] Read loop: Reconnect failed. Retrying in {self.REDIS_RECONNECT_DELAY_SECONDS}s...")
                    self._stop_event.wait(self.REDIS_RECONNECT_DELAY_SECONDS)
                    continue # Go back to start of loop to retry connection

            # --- Proceed with reading if connected ---
            try:
                logger.debug(f"[{self.consumer_id}] Waiting for new messages (BLOCK={self.READ_BLOCK_MILLISECONDS}ms)...")
                response = self.redis_client.xreadgroup(
                    groupname=self.group,
                    consumername=self.consumer_id,
                    streams={self.stream_key: '>'},
                    count=self.READ_BATCH_SIZE,
                    block=self.READ_BLOCK_MILLISECONDS
                )

                if self._stop_event.is_set(): break

                if response:
                    for stream, messages in response:
                        logger.info(f"[{self.consumer_id}][New] Received {len(messages)} new messages from {stream}.")
                        for message_id, message_data in messages:
                            if self._stop_event.is_set(): break
                            self._process_single_message(message_id, message_data)
                        if self._stop_event.is_set(): break
                # else: No new messages

                last_error_time = 0 # Reset error timestamp on success

            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                logger.warning(f"[{self.consumer_id}] Connection/Timeout error in read loop: {e}. Will attempt reconnect on next iteration.")
                self.redis_client = None # Signal need for reconnection
                # No need to sleep here, the check at the top of the loop handles it
            except Exception as e:
                current_time = time.time()
                if current_time - last_error_time > self.READ_LOOP_ERROR_DELAY_SECONDS:
                    logger.error(f"[{self.consumer_id}] Unexpected error in read loop: {e}", exc_info=True)
                    last_error_time = current_time
                # Wait before continuing to avoid fast loop on persistent error
                self._stop_event.wait(self.READ_LOOP_ERROR_DELAY_SECONDS)

        logger.info(f"[{self.consumer_id}] Read loop stopped.")

    def start(self):
        """Starts the worker: connects to Redis, starts heartbeat and read loop threads."""
        if self._read_loop_thread is not None and self._read_loop_thread.is_alive():
             logger.warning(f"[{self.consumer_id}] Worker is already running.")
             return

        logger.info(f"[{self.consumer_id}] Starting worker...")
        self._stop_event.clear()

        if not self._connect_redis() or not self._ensure_group_exists():
            logger.error(f"[{self.consumer_id}] Failed to connect or ensure group. Worker cannot start.")
            if self.redis_client:
                try: self.redis_client.close()
                except: pass
                self.redis_client = None
            return

        self._heartbeat_thread = threading.Thread(target=self._send_heartbeat_loop, name=f"{self.consumer_id}-Heartbeat", daemon=True)
        self._heartbeat_thread.start()
        logger.info(f"[{self.consumer_id}] Heartbeat thread started.")

        if self.CLAIM_INTERVAL_SECONDS > 0:
             logger.info(f"[{self.consumer_id}] Starting pending message claiming cycle (interval: {self.CLAIM_INTERVAL_SECONDS}s).")
             claim_init_thread = threading.Thread(target=self._run_claim_cycle, name=f"{self.consumer_id}-ClaimInit", daemon=True)
             claim_init_thread.start()
        else:
             logger.info(f"[{self.consumer_id}] Pending message claiming cycle disabled.")


        self._read_loop_thread = threading.Thread(target=self._read_loop, name=f"{self.consumer_id}-ReadLoop", daemon=True)
        self._read_loop_thread.start()
        logger.info(f"[{self.consumer_id}] Message read loop thread started.")

        logger.info(f"[{self.consumer_id}] Worker started successfully.")

    def stop(self):
        """Stops the worker gracefully."""
        if self._stop_event.is_set():
            logger.warning(f"[{self.consumer_id}] Worker is already stopping or stopped.")
            return

        logger.info(f"[{self.consumer_id}] Stopping worker...")
        self._stop_event.set()

        if self._claim_timer and self._claim_timer.is_alive():
             logger.debug(f"[{self.consumer_id}] Cancelling pending message claim timer.")
             self._claim_timer.cancel()
             self._claim_timer = None

        wait_timeout = max(self.HEARTBEAT_INTERVAL_SECONDS, self.READ_BLOCK_MILLISECONDS / 1000) + 2
        thread_map = {
            "Heartbeat": self._heartbeat_thread,
            "ReadLoop": self._read_loop_thread
        }

        for name, thread in thread_map.items():
             if thread and thread.is_alive():
                 logger.debug(f"[{self.consumer_id}] Waiting for {name} thread to finish...")
                 thread.join(timeout=wait_timeout)
                 if thread.is_alive():
                      logger.warning(f"[{self.consumer_id}] Timeout waiting for {name} thread.")
        self._heartbeat_thread = None
        self._read_loop_thread = None

        if self.redis_client and self.redis_client.connection:
            try:
                logger.info(f"[{self.consumer_id}] Deleting heartbeat key: {self.heartbeat_key}")
                self.redis_client.delete(self.heartbeat_key)
            except Exception as e:
                logger.warning(f"[{self.consumer_id}] Could not delete heartbeat key on stop: {e}")

        if self.redis_client:
            try:
                logger.info(f"[{self.consumer_id}] Closing Redis connection...")
                self.redis_client.close()
            except Exception as e:
                logger.error(f"[{self.consumer_id}] Error closing Redis connection: {e}")
            finally:
                 self.redis_client = None

        logger.info(f"[{self.consumer_id}] Worker stopped.")

# --- Example Usage (Remains the same as before) ---
if __name__ == "__main__":
    # Configure basic logging for the example
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s %(threadName)s - %(name)s - %(message)s')

    # --- Example Configuration ---
    REDIS_URL_EXAMPLE = "redis://localhost:6379"
    EXAMPLE_TOPIC = "my_event"
    EXAMPLE_GROUP = "event_processors"
    EXAMPLE_PARTITION_INDEX = 0
    EXAMPLE_CONSUMER_ID = f"worker-{EXAMPLE_TOPIC}-{EXAMPLE_GROUP}-p{EXAMPLE_PARTITION_INDEX}-{random.randint(1000,9999)}"

    # --- Example Handler ---
    processed_count = 0
    lock = threading.Lock()

    def simple_handler(data: Dict[str, Any], message_id: str, partition_index: int):
        global processed_count
        with lock:
            processed_count += 1
            current_count = processed_count
        logger.info(f"[Example Handler][P{partition_index}] Message #{current_count} (ID: {message_id}) received: {data}")
        time.sleep(random.uniform(0.1, 0.5))
        logger.info(f"[Example Handler][P{partition_index}] Message #{current_count} (ID: {message_id}) processed.")

    # --- Create and Start Worker ---
    worker = QTaskConsumerWorker(
        redis_url=REDIS_URL_EXAMPLE,
        topic=EXAMPLE_TOPIC,
        group=EXAMPLE_GROUP,
        partition_index=EXAMPLE_PARTITION_INDEX,
        consumer_id=EXAMPLE_CONSUMER_ID,
        handler=simple_handler
    )

    worker.start()

    # Keep the main program alive
    try:
        while True:
            time.sleep(10)
            logger.info(f"[Main] Worker active. Messages processed so far: {processed_count}")

    except KeyboardInterrupt:
        logger.info("[Main] Ctrl+C detected. Stopping worker...")
        worker.stop()
        logger.info("[Main] Program finished.")
    except Exception as e:
        logger.error(f"[Main] Unexpected error: {e}", exc_info=True)
        worker.stop()
