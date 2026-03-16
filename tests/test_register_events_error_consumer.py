import time

from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer

import uuid
import json
from framework.helpers.kafka.consumers.register_events_error import RegisterEventsErrorSubscriber
from framework.internal.http.account import AccountApi
from tests.conftest import account


def test_register_events_error_consumer(account: AccountApi, mail: MailApi, kafka_producer: Producer) -> None:
    base = uuid.uuid4().hex
    login = f"scarface_{base}"

    message = {
    "input_data": {
    "login": login,
            "email": login + "@mail.ru",
            "password": "1jksdnfjsadnfsa23"
        },
          "error_message": {
    "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "traceId": "00-2bd2ede7c3e4dcf40c4b7a62ac23f448-839ff284720ea656-01",
            "errors": {
    "Email": [
                "Invalid"
              ]
            }
          },
          "error_type": "unknown"
        } 

    kafka_producer.send('register-events-errors', message)

    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()['total'] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError('Email is not found')

    token = str(json.loads(response.json()['items'][0]['Content']['Body'])['ConfirmationLinkUrl']).split('/')[-1]
    response = account.user_activate(token)
    response_json = response.json()
    assert response_json["resource"]["login"] == login
    assert "Player" in list(response_json["resource"]["roles"])

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
        if message_from_kafka.value['input_data']['email'] == email:
            assert message_from_kafka.value['error_message']['title'] == 'Validation failed'
            assert message_from_kafka.value['error_type'] == 'validation'
        break
    else:
        raise AssertionError("Email not found")


def test_failed_registration_with_kafka_wrong_type_error(
        register_events_error_subscriber: RegisterEventsErrorSubscriber,
        kafka_producer: Producer,
        register_message_unknown: dict[str, dict[str, str]],
) -> None:
    login = register_message_unknown["input_data"]['login']
    kafka_producer.send("register-events-errors", register_message_unknown)

    for i in range(10):
        message_from_kafka = register_events_error_subscriber.get_message()
        if message_from_kafka.value['input_data']['login'] == login:
            assert message_from_kafka.value['error_type'] == 'unknown'
        break
    else:
        raise AssertionError("Email not found")

    time.sleep(0.5)

    for i in range(10):
        message_from_kafka = register_events_error_subscriber.get_message()

        if message_from_kafka.value['input_data']['login'] == login:
            assert message_from_kafka.value['error_type'] in ['validation']
        break
    else:
        raise AssertionError("Email not found")