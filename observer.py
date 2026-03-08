from typing import Protocol


class Observer(Protocol):
    def update(self, message: str): ...


class Subject:
    def __init__(self):
        self._subscribers: list = []

    def register(self, observer: 'Observer'):
        self._subscribers.append(observer)


    def notify(self, message: str):
        for observer in self._subscribers:
            observer.update(message)

class RegisterEventsSubscriber:
    def __init__(self):
        self.messages = []

    def update(self, message):
        self.messages.append({"register-events": message})

    def get_messages(self):
        return self.messages

class AnotherTopicSubscriber:
    def __init__(self):
        self.messages = []

    def update(self, message):
        self.messages.append({"another-topic": message})

    def get_messages(self):
        return self.messages