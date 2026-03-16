import json
import time
import uuid

import pika

from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber

from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer


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

def test_rabbit():
    connection = pika.BlockingConnection(
        pika.URLParameters('amqp://guest:guest@185.185.143.231:5672')
    )
    channel = connection.channel()
    address = f"{uuid.uuid4().hex}@mail.ru"
    message = {
        "address": address,
        "subject": "test",
        "body": "to qa or not to qa",
    }

    message = json.dumps(message).encode('utf-8')

    try:
        exchange = channel.exchange_declare(
            exchange_type='topic',
            exchange='dm.mail.sending',
            durable=True
        )

        properties = pika.BasicProperties(
            content_type='application/json',
            correlation_id=str(uuid.uuid4()),
        )

        channel.basic_publish(
            exchange='dm.mail.sending',
            routing_key='',
            body=message,
            properties=properties,
        )
    finally:
        channel.close()
        connection.close()