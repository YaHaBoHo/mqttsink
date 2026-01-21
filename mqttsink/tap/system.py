import psutil
from mqttsink.tap.core import Tap
from mqttsink.drop import Drop


class SystemTap(Tap):
    SOURCE = "system"

    def get_load5(self) -> float:
        return round(psutil.getloadavg()[1], 2)

    def get_swap(self) -> float:
        return round(psutil.swap_memory().percent, 2)

    def get_memory(self) -> float:
        return round(psutil.virtual_memory().percent, 2)

    def get_disk(self) -> dict[str, float]:
        disk = {}
        for path in self._paths:
            disk[path] = round(psutil.disk_usage(path).percent, 2)
        return disk

    def __init__(self, paths: list[str], **kwargs):
        super().__init__(**kwargs)
        self._paths = paths

    def fetch(self) -> list[Drop]:
        drops = [
            Drop("host", "load", self.get_load5()),
            Drop("host", "swap", self.get_swap()),
            Drop("host", "memory", self.get_memory()),
        ]
        for path, value in self.get_disk().items():
            if path == "/":
                path = "root"
            else:
                path = path.strip("/").replace("/", "-")
            drops.append(Drop("host", f"disk-{path}", value))
        return drops
