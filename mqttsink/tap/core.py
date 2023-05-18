import time
import logging
import traceback
from typing import Iterable, Optional
from ..drop import Drop


class Tap:
    SOURCE = "dummy"

    logger = logging.getLogger("mqttsink.tap")

    def __init__(self, name: str = "core", interval=300):
        self.name: str = name
        self.interval: int = interval
        self._next: float = 0

    @property
    def fullname(self):
        return f"{self.SOURCE}:{self.name}"

    @property
    def due(self) -> bool:
        return time.time() >= self._next

    def collect(self) -> Iterable["Drop"]:
        if self.due:
            self._next = time.time() + self.interval
            try:
                return self.fetch()
            except Exception:
                self.logger.error(f"Could not collect data ({self.fullname}).")
                self.logger.error(traceback.format_exc())
        return []

    def drop(self, data: dict, name: Optional[str] = None) -> Drop:
        return Drop(
            data={"_tap": self.fullname, **data},
            name=self.name if name is None else name,
        )

    # Cusomizeable methods

    def drop_names(self):
        return [self.name]

    def initalize(self) -> None:
        pass

    def restart(self) -> None:
        self.cleanup()
        self.initalize()

    def reload(self) -> None:
        self.reload()

    def fetch(self) -> Iterable[Drop]:
        return []

    def cleanup(self) -> None:
        pass
