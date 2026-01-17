import time
import logging
import traceback
from mqttsink.drop import Drop


class Tap:
    SOURCE = "dummy"

    logger = logging.getLogger("mqttsink.tap")

    def __init__(self, name: str = "core", interval=300):
        self.name: str = name
        self.interval: int = interval
        self._next: float = 0

    @property
    def path(self) -> list[str]:
        return [self.SOURCE, self.name]

    @property
    def fullname(self):
        return ":".join(self.path)

    @property
    def due(self) -> bool:
        return time.time() >= self._next

    def collect(self) -> list["Drop"]:
        if self.due:
            self._next = time.time() + self.interval
            try:
                drops = self.fetch()
                self.logger.info("Collected %s drops for %s", len(drops), self.fullname)
                return drops
            except Exception:  # pylint: disable=broad-exception-caught
                self.logger.error("Could not collect data for %s", self.fullname)
                self.logger.debug(traceback.format_exc())
        return []

    # Cusomizeable methods

    def drop_names(self):
        return [self.name]

    def initalize(self) -> None:
        pass

    def restart(self) -> None:
        self.cleanup()
        self.initalize()

    def reload(self) -> None:
        self.restart()

    def fetch(self) -> list[Drop]:
        return []

    def cleanup(self) -> None:
        pass
