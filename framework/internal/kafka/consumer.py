import json
import queue
import threading
from typing import Any

from kafka import KafkaConsumer
from kafka.util import Dict


class Consumer:
    def __init__(self, bootstrap_servers=['185.185.143.231:9092'], topic: str = "register-events"):
        self._bootstrap_servers = bootstrap_servers
        self._consumer: KafkaConsumer | None = None
        self._running = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._messages: queue.Queue() = queue.Queue()
        self._topic = topic

    def start(self):
        self._consumer = KafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            auto_offset_reset="latest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            group_id="test-group"
        )
        self._running.set()
        self._ready.clear()
        self._thread = threading.Thread(target=self.consume, daemon=True)
        self._thread.start()

        if not self._ready.wait(timeout=10):
            raise RuntimeError("Consumer is not ready")

    def consume(self):
        self._ready.set()
        print("Consumer started")
        try:
            while self._running.is_set():
                messages = self._consumer.poll(timeout_ms=1000, max_records=10)
                for topic_partition, records in messages.items():
                    for record in records:
                        print(f"{topic_partition}: {record}")
                        self._messages.put(record.value)
        except Exception as e:
            print(e)

    def get_message(self, timeout: float = 10.0) -> Dict[str, Any]:
        try:
            return self._messages.get(timeout=timeout)
        except queue.Empty:
            raise AssertionError(f"No messages received within timeout {timeout} seconds")

    def stop(self):
        self._running.clear()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
            if self._thread.is_alive():
                print("Thread is still alive")

        if self._consumer:
            try:
                self._consumer.close(timeout_ms=2000)
                print("Stop consuming")
            except Exception as e:
                print(f"Error while closing consumer: {e}")

        del self._consumer
        del self._messages

        print("Consumer stopped")

    def __exit__(self, *args):
        self.stop()

    def __enter__(self):
        self.start()
        return self