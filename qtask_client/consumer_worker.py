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
    THREAD_INITIAL_DELAY_SECONDS = 0.1
    # --- NEW: Max attempts for ACK after connection error ---
    ACK_RETRY_ATTEMPTS = 1

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
        # --- NEW: Lock for Redis client operations to prevent race conditions during reconnect ---
        self._redis_lock = threading.Lock()


        logger.info(f"[{self.consumer_id}] Worker initialized for stream '{self.stream_key}', group '{self.group}'")

    def _connect_redis(self) -> bool:
        """Attempts to connect (or reconnect) to Redis. MUST be called with _redis_lock held."""
        # This method assumes the lock is already acquired by the caller
        try:
            # Check ping only if client exists, avoid redundant ping if already connected
            if self.redis_client:
                 try:
                     if self.redis_client.ping():
                         # logger.debug(f"[{self.consumer_id}] Redis connection confirmed via ping.")
                         return True
                 except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
                    logger.warning(f"[{self.consumer_id}] Redis ping failed, attempting to reconnect...")
                    # Close potentially broken connection before creating new one
                    try: self.redis_client.close()
                    except: pass
                    self.redis_client = None
                 except Exception as e:
                    logger.error(f"[{self.consumer_id}] Unexpected error during Redis ping: {e}")
                    try: self.redis_client.close()
                    except: pass
                    self.redis_client = None

        except Exception as e: # Catch potential errors accessing self.redis_client if None
             logger.debug(f"[{self.consumer_id}] Error checking existing redis client: {e}")
             self.redis_client = None


        # If client is None or ping failed, attempt new connection
        if self.redis_client is None:
            logger.info(f"[{self.consumer_id}] Attempting to connect to Redis at {self.redis_url}...")
            try:
                # Create and connect
                new_client = redis.from_url(self.redis_url, decode_responses=True)
                new_client.ping() # Verify connection immediately
                self.redis_client = new_client # Assign only after successful ping
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
        
        return False # Should not be reached if logic is correct

    def _ensure_group_exists(self):
        """Ensures the consumer group exists. MUST be called with _redis_lock held."""
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
        time.sleep(self.THREAD_INITIAL_DELAY_SECONDS)

        while not self._stop_event.is_set():
            client_to_use = None
            with self._redis_lock:
                # Get the current client instance under lock
                client_to_use = self.redis_client

            if client_to_use:
                try:
                    # Use the captured client instance
                    logger.debug(f"[{self.consumer_id}] Sending heartbeat (SET {self.heartbeat_key} EX {self.HEARTBEAT_TTL_SECONDS})")
                    client_to_use.set(self.heartbeat_key, 'alive', ex=self.HEARTBEAT_TTL_SECONDS)
                except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                    logger.warning(f"[{self.consumer_id}] Connection error sending heartbeat: {e}. Read loop will handle reconnect.")
                    # Don't set self.redis_client to None here, let read loop manage it
                except Exception as e:
                    logger.error(f"[{self.consumer_id}] Unexpected error sending heartbeat: {e}", exc_info=True)
            else:
                logger.debug(f"[{self.consumer_id}] Skipping heartbeat, client instance is None.")

            self._stop_event.wait(self.HEARTBEAT_INTERVAL_SECONDS)
        logger.info(f"[{self.consumer_id}] Heartbeat loop stopped.")

    def _process_single_message(self, message_id: str, message_data: Dict[str, Any]):
        """Processes a single message: parses, calls handler, and acknowledges (ACKs)."""
        # No lock needed here initially, only for Redis operations

        if self._stop_event.is_set():
             logger.debug(f"[{self.consumer_id}] Skipping message {message_id} processing (stopping).")
             return

        logger.info(f"[{self.consumer_id}][P{self.partition_index}] Processing message ID: {message_id}")
        payload_str = message_data.get('payload')

        if payload_str is None:
            logger.warning(f"[{self.consumer_id}] Message {message_id} has no 'payload' field. Attempting ACK.")
            # Attempt to ACK even without payload
            self._acknowledge_message(message_id)
            return

        # Process payload
        handler_success = False
        try:
            data = json.loads(payload_str)
            # --- Call the user-provided handler ---
            self.handler(data, message_id, self.partition_index)
            # --- Handler finished successfully ---
            handler_success = True
            logger.info(f"[{self.consumer_id}] Handler executed successfully for {message_id}.")

        except json.JSONDecodeError as e:
            logger.error(f"[{self.consumer_id}] Failed to parse JSON for message {message_id}: {e}")
            # Consider moving to a dead-letter queue or logging extensively
            # Do NOT ACK if JSON is invalid, prevents infinite loops if data is malformed
        except Exception as e:
            logger.error(f"[{self.consumer_id}] Error executing handler for message {message_id}: {e}", exc_info=True)
            # Do NOT ACK if handler fails, message will be retried/claimed later

        # --- Acknowledge only if the handler succeeded ---
        if handler_success:
            self._acknowledge_message(message_id)

    def _acknowledge_message(self, message_id: str):
        """Internal helper to acknowledge a message with retry on connection error."""
        if self._stop_event.is_set():
            return # Don't try to ACK if stopping

        attempts = 0
        acknowledged = False
        while attempts <= self.ACK_RETRY_ATTEMPTS and not acknowledged and not self._stop_event.is_set():
            client_to_use = None
            reconnect_needed = False
            with self._redis_lock:
                # Get current client under lock
                client_to_use = self.redis_client

            if client_to_use:
                try:
                    # Attempt ACK using the captured client instance
                    ack_result = client_to_use.xack(self.stream_key, self.group, message_id)
                    logger.info(f"[{self.consumer_id}] Message {message_id} acknowledged (ACK: {ack_result})")
                    acknowledged = True # Success!
                except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                    logger.warning(f"[{self.consumer_id}] Connection/Timeout error acknowledging message {message_id} (Attempt {attempts+1}/{self.ACK_RETRY_ATTEMPTS+1}): {e}")
                    reconnect_needed = True # Signal need to reconnect outside lock
                except Exception as e:
                    logger.error(f"[{self.consumer_id}] Unexpected error acknowledging message {message_id}: {e}")
                    # Break on unexpected errors, don't retry
                    break
            else:
                logger.warning(f"[{self.consumer_id}] Cannot ACK message {message_id}, Redis client is None (Attempt {attempts+1}/{self.ACK_RETRY_ATTEMPTS+1}).")
                reconnect_needed = True # Signal need to reconnect

            # --- Handle Reconnect Outside Lock ---
            if reconnect_needed and attempts < self.ACK_RETRY_ATTEMPTS:
                 logger.info(f"[{self.consumer_id}] Attempting reconnect before retrying ACK for {message_id}...")
                 with self._redis_lock:
                      # Attempt connection while holding lock
                      self._connect_redis()
                 # Brief pause before retrying ACK
                 time.sleep(0.1)

            attempts += 1

        if not acknowledged and not self._stop_event.is_set():
             logger.error(f"[{self.consumer_id}] Failed to acknowledge message {message_id} after {attempts} attempts.")
             # Message will likely be re-processed later


    def _claim_and_process_pending(self):
        """Attempts to claim and process pending messages using XAUTOCLAIM."""
        if self._stop_event.is_set(): return

        logger.info(f"[{self.consumer_id}] Checking for pending messages (min_idle_time={self.MIN_IDLE_TIME_MS}ms)...")
        try:
            start_id = '0-0'
            total_claimed_in_run = 0
            while not self._stop_event.is_set():
                client_to_use = None
                reconnect_needed = False
                with self._redis_lock:
                    client_to_use = self.redis_client

                if not client_to_use:
                    logger.warning(f"[{self.consumer_id}] Cannot claim pending messages, Redis client is None. Waiting...")
                    time.sleep(self.REDIS_RECONNECT_DELAY_SECONDS)
                    continue # Rely on read loop to reconnect

                # --- Perform XAUTOCLAIM ---
                claimed_messages = []
                next_id = start_id
                try:
                    result = client_to_use.xautoclaim(
                        name=self.stream_key,
                        groupname=self.group,
                        consumername=self.consumer_id,
                        min_idle_time=self.MIN_IDLE_TIME_MS,
                        start_id=start_id,
                        count=self.PENDING_BATCH_SIZE
                    )
                    next_id = result[0]
                    claimed_messages = result[1]
                except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                     logger.warning(f"[{self.consumer_id}] Connection error during XAUTOCLAIM: {e}. Read loop will handle reconnect.")
                     # No need to set client to None, read loop handles it
                     time.sleep(self.REDIS_RECONNECT_DELAY_SECONDS) # Wait before next attempt
                     continue # Retry outer while loop
                except Exception as claim_err:
                     logger.error(f"[{self.consumer_id}] Error during XAUTOCLAIM execution: {claim_err}", exc_info=True)
                     break # Exit claim loop on unexpected errors

                # --- Process Claimed Messages ---
                if not claimed_messages:
                    if start_id == '0-0': logger.debug(f"[{self.consumer_id}] No initial pending messages found.")
                    else: logger.debug(f"[{self.consumer_id}] No more pending messages found (checked up to {start_id}).")
                    break # Exit claim loop

                logger.info(f"[{self.consumer_id}][Pending] Claimed {len(claimed_messages)} pending messages.")
                total_claimed_in_run += len(claimed_messages)
                for message_id, message_data in claimed_messages:
                    if self._stop_event.is_set(): break
                    # Process message uses its own connection handling for ACK
                    self._process_single_message(message_id, message_data)

                if self._stop_event.is_set(): break

                if not next_id or next_id == '0-0':
                     logger.debug(f"[{self.consumer_id}][Pending] Redis indicated end of pending messages.")
                     break
                else:
                     start_id = next_id # Continue claiming

                time.sleep(0.1) # Small pause if many messages

            # Log final claim status
            if total_claimed_in_run > 0:
                 logger.info(f"[{self.consumer_id}] Pending message check completed. Claimed in this run: {total_claimed_in_run}.")
            else:
                 logger.debug(f"[{self.consumer_id}] Pending message check completed. Nothing to claim.")

        except Exception as e: # Catch errors outside the inner try/except
            logger.error(f"[{self.consumer_id}] Unexpected error during pending message claiming loop: {e}", exc_info=True)


    def _schedule_next_claim(self):
        """Schedules the next run of _claim_and_process_pending if enabled."""
        if self._claim_timer:
            try: self._claim_timer.cancel()
            except: pass # Ignore errors if timer already cancelled/finished
            self._claim_timer = None

        if self.CLAIM_INTERVAL_SECONDS > 0 and not self._stop_event.is_set():
            logger.debug(f"[{self.consumer_id}] Scheduling next pending check in {self.CLAIM_INTERVAL_SECONDS}s.")
            self._claim_timer = threading.Timer(self.CLAIM_INTERVAL_SECONDS, self._run_claim_cycle)
            self._claim_timer.daemon = True
            self._claim_timer.start()

    def _run_claim_cycle(self):
        """Executes one claim cycle and schedules the next one."""
        if self._stop_event.is_set(): return
        try:
            self._claim_and_process_pending()
        except Exception as e:
             logger.error(f"[{self.consumer_id}] Error in claim cycle execution: {e}", exc_info=True)
        finally:
             # Always reschedule, even if there was an error in this cycle
             if not self._stop_event.is_set():
                 self._schedule_next_claim()


    def _read_loop(self):
        """Main loop executed in a thread to read new messages."""
        logger.debug(f"[{self.consumer_id}] Starting read loop...")
        last_error_time = 0
        time.sleep(self.THREAD_INITIAL_DELAY_SECONDS)

        while not self._stop_event.is_set():
            reconnect_attempted = False
            # --- Primary Connection Check and Reconnect Logic ---
            with self._redis_lock:
                if not self.redis_client: # Check if client is None first
                     reconnect_needed = True
                else:
                     try:
                          # Ping only if client object exists
                          if not self.redis_client.ping():
                               logger.warning(f"[{self.consumer_id}] Read loop: Redis ping failed. Attempting reconnect...")
                               reconnect_needed = True
                          else:
                               reconnect_needed = False # Ping successful
                     except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
                          logger.warning(f"[{self.consumer_id}] Read loop: Redis connection error on ping. Attempting reconnect...")
                          reconnect_needed = True
                     except Exception as ping_err:
                          logger.error(f"[{self.consumer_id}] Read loop: Unexpected error during ping: {ping_err}")
                          reconnect_needed = True # Assume reconnect needed on unexpected error

                if reconnect_needed:
                    logger.info(f"[{self.consumer_id}] Read loop: Attempting reconnect...")
                    reconnect_attempted = True
                    if self._connect_redis(): # Connect holds the lock
                        self._ensure_group_exists() # Ensure group holds the lock
                        logger.info(f"[{self.consumer_id}] Read loop: Reconnected successfully.")
                    else:
                        logger.info(f"[{self.consumer_id}] Read loop: Reconnect failed. Retrying in {self.REDIS_RECONNECT_DELAY_SECONDS}s...")
                        # Release lock before sleeping
                        # Note: _stop_event.wait releases the GIL, allowing other threads
                        self._stop_event.wait(self.REDIS_RECONNECT_DELAY_SECONDS)
                        continue # Go back to start of loop to retry connection

            # If we just tried to reconnect, loop again to re-evaluate state
            if reconnect_attempted:
                 continue

            # --- Proceed with reading if connected ---
            client_to_use = None
            with self._redis_lock:
                client_to_use = self.redis_client # Get client instance under lock

            if not client_to_use:
                 logger.warning(f"[{self.consumer_id}] Read loop: Client became None unexpectedly after check. Retrying loop.")
                 time.sleep(0.1) # Small pause
                 continue

            try:
                logger.debug(f"[{self.consumer_id}] Waiting for new messages (BLOCK={self.READ_BLOCK_MILLISECONDS}ms)...")
                # Use the captured client instance
                response = client_to_use.xreadgroup(
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
                            # Process message handles its own ACK logic/errors
                            self._process_single_message(message_id, message_data)
                        if self._stop_event.is_set(): break
                # else: No new messages

                last_error_time = 0 # Reset error timestamp on success

            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                logger.warning(f"[{self.consumer_id}] Connection/Timeout error in read loop (XREADGROUP): {e}. Will attempt reconnect on next iteration.")
                with self._redis_lock: # Acquire lock to safely set client to None
                    try: client_to_use.close() # Try closing the potentially broken connection
                    except: pass
                    if self.redis_client == client_to_use: # Ensure we nullify the same instance
                         self.redis_client = None
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

        # --- Initial connection attempt ---
        with self._redis_lock:
            if not self._connect_redis() or not self._ensure_group_exists():
                logger.error(f"[{self.consumer_id}] Failed to connect or ensure group during startup. Worker cannot start.")
                # Clean up client if connection failed partially
                if self.redis_client:
                    try: self.redis_client.close()
                    except: pass
                    self.redis_client = None
                return # Do not start threads

        # --- Start Threads ---
        self._heartbeat_thread = threading.Thread(target=self._send_heartbeat_loop, name=f"{self.consumer_id}-Heartbeat", daemon=True)
        self._heartbeat_thread.start()
        logger.info(f"[{self.consumer_id}] Heartbeat thread started.")

        if self.CLAIM_INTERVAL_SECONDS > 0:
             logger.info(f"[{self.consumer_id}] Starting pending message claiming cycle (interval: {self.CLAIM_INTERVAL_SECONDS}s).")
             # Schedule the first claim; subsequent claims are scheduled by _run_claim_cycle
             self._schedule_next_claim()
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
        self._stop_event.set() # Signal threads to stop

        # Cancel the claim timer
        if self._claim_timer:
             try:
                 logger.debug(f"[{self.consumer_id}] Cancelling pending message claim timer.")
                 self._claim_timer.cancel()
             except: pass # Ignore errors
             self._claim_timer = None

        # Wait for threads to finish
        wait_timeout = max(self.HEARTBEAT_INTERVAL_SECONDS, self.READ_BLOCK_MILLISECONDS / 1000) + 2
        thread_map = { "Heartbeat": self._heartbeat_thread, "ReadLoop": self._read_loop_thread }
        for name, thread in thread_map.items():
             if thread and thread.is_alive():
                 logger.debug(f"[{self.consumer_id}] Waiting for {name} thread to finish...")
                 thread.join(timeout=wait_timeout)
                 if thread.is_alive(): logger.warning(f"[{self.consumer_id}] Timeout waiting for {name} thread.")
        self._heartbeat_thread = None
        self._read_loop_thread = None

        # Clean up Redis resources under lock
        with self._redis_lock:
            if self.redis_client:
                # Delete heartbeat key
                try:
                    logger.info(f"[{self.consumer_id}] Deleting heartbeat key: {self.heartbeat_key}")
                    self.redis_client.delete(self.heartbeat_key)
                except Exception as e:
                    logger.warning(f"[{self.consumer_id}] Could not delete heartbeat key on stop: {e}")

                # Close Redis connection
                try:
                    logger.info(f"[{self.consumer_id}] Closing Redis connection...")
                    self.redis_client.close()
                except Exception as e:
                    logger.error(f"[{self.consumer_id}] Error closing Redis connection: {e}")
                finally:
                     self.redis_client = None # Ensure client is None after closing

        logger.info(f"[{self.consumer_id}] Worker stopped.")

# --- Example Usage (Remains the same) ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s %(threadName)s - %(name)s - %(message)s')
    REDIS_URL_EXAMPLE = "redis://localhost:6379"
    EXAMPLE_TOPIC = "my_event"
    EXAMPLE_GROUP = "event_processors"
    EXAMPLE_PARTITION_INDEX = 0
    EXAMPLE_CONSUMER_ID = f"worker-{EXAMPLE_TOPIC}-{EXAMPLE_GROUP}-p{EXAMPLE_PARTITION_INDEX}-{random.randint(1000,9999)}"
    processed_count = 0
    lock = threading.Lock()
    def simple_handler(data: Dict[str, Any], message_id: str, partition_index: int):
        global processed_count
        with lock: processed_count += 1; current_count = processed_count
        logger.info(f"[Example Handler][P{partition_index}] Message #{current_count} (ID: {message_id}) received: {data}")
        time.sleep(random.uniform(0.1, 0.5))
        logger.info(f"[Example Handler][P{partition_index}] Message #{current_count} (ID: {message_id}) processed.")
    worker = QTaskConsumerWorker(
        redis_url=REDIS_URL_EXAMPLE, topic=EXAMPLE_TOPIC, group=EXAMPLE_GROUP,
        partition_index=EXAMPLE_PARTITION_INDEX, consumer_id=EXAMPLE_CONSUMER_ID, handler=simple_handler
    )
    worker.start()
    try:
        while True: time.sleep(10); logger.info(f"[Main] Worker active. Messages processed so far: {processed_count}")
    except KeyboardInterrupt: logger.info("[Main] Ctrl+C detected. Stopping worker...")
    except Exception as e: logger.error(f"[Main] Unexpected error: {e}", exc_info=True)
    finally: worker.stop(); logger.info("[Main] Program finished.")

