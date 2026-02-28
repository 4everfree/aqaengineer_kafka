import time
import uuid
import json

from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer


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
