import json
import time
import uuid

import pytest

from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from framework.helpers.kafka.consumers.register_events_error import RegisterEventsErrorSubscriber
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer

@pytest.fixture
def register_message() -> dict[str, str]:
    base = uuid.uuid4().hex
    return {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "1jksdnfjsadnfsa23"
    }

@pytest.fixture
def register_message_wrong() -> dict[str, str]:
    base = "!#_+"
    return {
        "login": base,
        "email": f"{base}@mail",
        "password": "1jksdnfjsadnfsa23"
    }

@decorator
def get_m(func, *args):
    def wrapper(func, *args):
        for i in range(10):
            message_from_kafka = func.get_message()
            if message_from_kafka.value['login'] == args[0]:
                break
        else:
            raise AssertionError("Email not found")

    return wrapper()

def test_success_registration_with_kafka_producer(
        mail: MailApi,
        kafka_producer: Producer,
        register_message: dict[str, str]
) -> None:
    login = register_message['login']
    kafka_producer.send('register-events', register_message)
    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()['total'] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError('Email is not found')

def test_success_registration_with_kafka_consumer_observer(
        register_events_subscriber: RegisterEventsSubscriber,
        kafka_producer: Producer,
        register_message: dict[str, str],
) -> None:
    login = register_message['login']
    kafka_producer.send("register-events", register_message)

    for i in range(10):
        message_from_kafka = register_events_subscriber.get_message()
        if message_from_kafka.value['login'] == login:
            break
    else:
        raise AssertionError("Email not found")

def test_failed_registration_with_kafka_consumer_observer(
        register_events_subscriber: RegisterEventsSubscriber,
        register_events_error_subscriber: RegisterEventsErrorSubscriber,
        kafka_producer: Producer,
        register_message_wrong: dict[str, str],
) -> None:
    login = register_message_wrong['login']
    kafka_producer.send("register-events", register_message_wrong)

    for i in range(10):
        message_from_kafka = register_events_subscriber.get_message()
        if message_from_kafka.value['login'] == login:
            break
    else:
        raise AssertionError("Email not found")

    for i in range(10):
        message_from_kafka = register_events_error_subscriber.get_message()
        if message_from_kafka.value['login'] == login:
            break
    else:
        raise AssertionError("Email not found")