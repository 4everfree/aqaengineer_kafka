import json
import time
import uuid

import pytest

from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from framework.helpers.kafka.consumers.register_events_error import RegisterEventsErrorSubscriber
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer
from tests.conftest import account


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


@pytest.fixture
def register_message_unknown() -> dict[str, dict[str, str]]:
    return {
        "input_data": {
            "login": "string",
            "email": "string@mail.ru",
            "password": "string"
        },
        "error_message": {
            "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "traceId": "00-d957971fc5a90855accb02b59e879f3c-fa1d384d99ca048d-01",
            "errors": {
                "Email": [
                    "Taken"
                ]
            }
        },
        "error_type": "unknown"
    }


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
        account: AccountApi,
        register_events_subscriber: RegisterEventsSubscriber,
        register_events_error_subscriber: RegisterEventsErrorSubscriber,
        kafka_producer: Producer,
        register_message_wrong: dict[str, str],
) -> None:
    login = register_message_wrong['login']
    email = register_message_wrong['email']
    password = register_message_wrong['password']

    account.register_user(login, email, password)

    for i in range(10):
        message_from_kafka = register_events_subscriber.get_message()
        if message_from_kafka.value['login'] == login:
            print("Сообщение в register-events нашлось")
            break
    else:
        raise AssertionError("Email not found")

    for i in range(10):
        message_from_kafka = register_events_error_subscriber.get_message()
        assert message_from_kafka.value['error_message']['title'] == 'Validation failed'
        assert message_from_kafka.value['error_type'] == 'validation'
        break
    else:
        raise AssertionError("Email not found")


def test_failed_registration_with_kafka_wrong_type_error(
        register_events_error_subscriber: RegisterEventsErrorSubscriber,
        kafka_producer: Producer,
        register_message_unknown,
) -> None:
    kafka_producer.send("register-events-errors", register_message_unknown)

    for i in range(10):
        message_from_kafka = register_events_error_subscriber.get_message()
        assert message_from_kafka.value['error_type'] == 'unknown'
        break
    else:
        raise AssertionError("Email not found")

    for i in range(10):
        message_from_kafka = register_events_error_subscriber.get_message()
        assert message_from_kafka.value['error_type'] == 'validation'
        break
    else:
        raise AssertionError("Email not found")
