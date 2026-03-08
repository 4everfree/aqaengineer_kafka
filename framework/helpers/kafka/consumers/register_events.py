from framework.internal.kafka.subscriber import Subscriber


class RegisterEventsSubscriber(Subscriber):

    @property
    def topic(self) -> str:
        return "register-events"

    def handle_message(self, record):
        super().handle_message(record)

