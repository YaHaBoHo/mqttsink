import json


class Drop:
    def __init__(self, data: dict, name: str) -> None:
        self.data = data
        self.name = name

    @property
    def payload(self) -> str:
        return json.dumps(self.data)
