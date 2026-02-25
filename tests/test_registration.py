import time
import uuid
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer


def test_success_registration_with_kafka_producer(mail: MailApi, kafka_producer: Producer) -> None:
    base = uuid.uuid4().hex
    login = f"scarface_{base}"
    message = {
        "login": login,
        "email": f"{login}@mail.ru",
        "password": "1jksdnfjsadnfsa23"
    }
    kafka_producer.send('register-events', message)
    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()['total'] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError('Email is not found')