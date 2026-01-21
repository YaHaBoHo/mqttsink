import logging
import tomllib
from typing import Self, Literal, Generator
from pydantic import BaseModel
from mqttsink.sink import Sink
from mqttsink.tap.core import Tap

# from mqttsink.tap.system import SystemTap
from mqttsink.tap.aranet import AranetTap
from mqttsink.tap.somneo import SomneoTap


# TODO : Error handling
def load(path: str) -> dict:
    with open(path, "rb") as config_file:
        return tomllib.load(config_file)


LogLevel = Literal[10, 20, 30, 40, 50]


class MqttConfig(BaseModel):
    hostname: str = "localhost"
    port: int = 1883
    name: str = "mqttsink"
    username: str
    password: str


class TapConfig(BaseModel):
    _tap: type[Tap]
    name: str | None = None
    interval: int = 300

    def spawn(self) -> Tap:
        return self._tap(**self.model_dump())  # TODO : from_config()


# class SystemTapConfig(TapConfig):
#     _tap = SystemTap
#     paths: list[str] = ["/"]


class AranetTapConfig(TapConfig):
    _tap = AranetTap
    hostname: str
    username: str
    password: str
    sensors: dict[str, str]
    verify: bool = True


class SomneoTapConfig(TapConfig):
    _tap = SomneoTap
    hostname: str


class SinkConfig(BaseModel):
    loglevel: LogLevel
    mqtt: MqttConfig
    # system: list[SystemTapConfig] = []
    aranet: list[AranetTapConfig] = []
    somneo: list[SomneoTapConfig] = []

    @classmethod
    def from_file(cls, path: str) -> Self:
        return cls.model_validate(load(path))

    @property
    def taps(self) -> Generator[TapConfig, None, None]:
        # yield from self.system
        yield from self.aranet
        yield from self.somneo


def configure(path: str) -> Sink:
    # Parse config
    config = SinkConfig.from_file(path)
    # Process config
    logging.basicConfig(level=config.loglevel)
    sink = Sink(**config.mqtt.model_dump())  # TODO : from_config()
    for tap_config in config.taps:
        sink.register(tap_config.spawn())
    return sink
