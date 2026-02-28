import time
import uuid

from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer


def test_register_events_error_consumer(mail: MailApi, kafka_producer: Producer) -> None:
    base = uuid.uuid4().hex
    login = f"scarface_{base}"
    message = {
        "login": login,
        "email": f"{login}@mail.ru",
        "password": "1jksdnfjsadnfsa23"
    }
    kafka_producer.send('register-events-errors', message)

    message = f"""
     {
      "input_data": {
        "login": {login},
        "email": f"{login}@mail.ru",
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
    """

    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()['total'] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError('Email is not found')

