import json
from typing import List


class Drop:
    def __init__(self, name: str, metric: str, data: str) -> None:
        self.data = data
        self.name = name
        self.metric = metric

    def __repr__(self) -> str:
        return f"<Drop({self.name}/{self.metric} : {self.data})>"

    @property
    def path(self) -> List[str]:
        return [self.name, self.metric]

    @property
    def payload(self) -> str:
        return json.dumps(self.data)
