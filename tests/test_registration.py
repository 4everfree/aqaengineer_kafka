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
