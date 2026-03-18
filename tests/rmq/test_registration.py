import time

from framework.helpers.rmq.consumers.dm_mail_sending import DmMailSending
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from tests.conftest import account


def test_success_registration(
        rmq_dm_mail_sending_consumer: DmMailSending,
        account: AccountApi,
        mail: MailApi,
        register_message: dict[str, str]
) -> None:
    login = register_message['login']
    account.register_user(**register_message)
    rmq_dm_mail_sending_consumer.find_message(login=login)
    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()['total'] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError('Email is not found')