from framework.internal.kafka.subscriber import Subscriber


class RegisterEventsErrorSubscriber(Subscriber):

    @property
    def topic(self) -> str:
        return "register-events-errors"

    def handle_message(self, record):
        super().handle_message(record)

