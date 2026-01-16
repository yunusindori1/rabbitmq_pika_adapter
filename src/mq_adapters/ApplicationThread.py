import logging
from threading import Thread


class ApplicationThread(Thread):
    """Small Thread base class used by the legacy sync API.

    Notes:
        - This class intentionally preserves the existing public attributes (`running`, `stop_me`, `starting`).
        - Prefer using the higher-level `RabbitMQClient` facade and publisher/listener helpers.
    """

    def __init__(self):
        super(ApplicationThread, self).__init__()
        self.running = False
        self.stop_me = False
        self.starting = True

    def run(self) -> None:
        self.running = True
        logging.getLogger(__name__).debug(f"Thread: {self.name}")

    def stop(self):
        self.stop_me = True
        self.running = False

