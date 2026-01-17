import random
from mqttsink.tap.core import Tap
from mqttsink.drop import Drop


class RandomTap(Tap):
    SOURCE = "random"

    def __init__(self, blueprint: dict, **kwargs):
        super().__init__(**kwargs)
        self.blueprint = blueprint

    def fetch(self) -> list[Drop]:
        return [
            Drop(
                name=self.name,
                metric=key,
                data=random.randint(*minmax),
            )
            for key, minmax in self.blueprint.items()
        ]
