import json
import queue
import threading
import time
from collections import defaultdict
from typing import Any

from kafka import KafkaConsumer
from kafka.util import Dict

from framework.internal.kafka.subscriber import Subscriber
from framework.internal.singleton import Singleton
from observer import Observer


class Consumer(Singleton):

    _started: bool = False

    def __init__(self,
                 subscribers: list[Subscriber],
                 bootstrap_servers=['185.185.143.231:9092'],
                 ):
        self._bootstrap_servers = bootstrap_servers
        self._subscribers = subscribers
        self._consumer: KafkaConsumer | None = None
        self._running = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._watchers: list[str, list[Observer] ] = defaultdict(list)

    def register(self):
        if self._subscribers is None:
            raise RuntimeError("Subscribers object is not initialized")

        if self._started:
            raise RuntimeError("Consumer is already started")

        for subscriber in self._subscribers:
            print(f"Registering subsriber {subscriber.topic}")
            self._watchers[subscriber.topic].append(subscriber)

    def start(self):
        self._consumer = KafkaConsumer(
            *self._watchers.keys(),
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

        self._started = True

    def consume(self):
        self._ready.set()
        print("Consumer started")
        try:
            while self._running.is_set():
                messages = self._consumer.poll(timeout_ms=1000, max_records=10)
                for topic_partition, records in messages.items():
                    topic = topic_partition.topic
                    for record in records:
                        for watcher in self._watchers[topic]:
                            print(f"{topic}: {record}")
                            watcher.handle_message(record)
                    time.sleep(0.1)

                if not messages:
                    time.sleep(0.1)
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
                self._consumer.close()
                print("Stop consuming")
            except Exception as e:
                print(f"Error while closing consumer: {e}")

        del self._consumer
        self._watchers.clear()
        self._subscribers.clear()
        self._started = False

        print("Consumer stopped")

    def __enter__(self):
        self.register()
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()