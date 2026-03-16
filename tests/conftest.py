from typing import Generator, Any

import pytest
import uuid

from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from framework.helpers.kafka.consumers.register_events_error import RegisterEventsErrorSubscriber
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.consumer import Consumer
from framework.internal.kafka.producer import Producer


@pytest.fixture(scope="session")
def account() -> AccountApi:
    return AccountApi()

@pytest.fixture(scope="session")
def mail() -> MailApi:
    return MailApi()

@pytest.fixture(scope="session")
def kafka_producer() -> Generator[Producer, Any, None]:
    with Producer() as producer:
        yield producer

@pytest.fixture(scope="session")
def register_events_subscriber() -> RegisterEventsSubscriber:
    return RegisterEventsSubscriber()

@pytest.fixture(scope="session")
def register_events_error_subscriber() -> RegisterEventsErrorSubscriber:
    return RegisterEventsErrorSubscriber()

@pytest.fixture(scope="session", autouse=True)
def kafka_consumer(
        register_events_subscriber: RegisterEventsSubscriber,
        register_events_error_subscriber: RegisterEventsErrorSubscriber,
) -> Generator[Consumer | Any, Any, None]:
    with Consumer(subscribers=[register_events_subscriber, register_events_error_subscriber]) as consumer:
        yield consumer

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
    login = uuid.uuid4().hex
    return {
        "input_data": {
            "login": f"{login}",
            "email": f"{login}mail.ru",
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

