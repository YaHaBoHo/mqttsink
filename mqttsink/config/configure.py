import logging
from typing import List
from mqttsink.sink import Sink
from mqttsink.tap.core import Tap
from mqttsink.tap.random import RandomTap
from mqttsink.tap.aranet import AranetTap
from mqttsink.tap.somneo import SomneoTap
from mqttsink.config.handler import Parameter, load, extract

MAPPER_LOGGING = [
    Parameter("level", argtype=str, default="INFO"),
]

MAPPER_SINK = [
    Parameter("hostname", argtype=str, default="localhost"),
    Parameter("port", argtype=int, default=1883),
    Parameter("name", argtype=str, default="mqttsink"),
    Parameter("username", argtype=str),
    Parameter("password", argtype=str),
]

MAPPER_TAP = [
    Parameter("interval", argtype=int, default=300),
]

MAPPER_TAP_RANDOM = [
    Parameter("blueprint", argtype=dict, required=True),
]

MAPPER_TAP_ARANET = [
    Parameter("hostname", argtype=str, required=True),
    Parameter("username", argtype=str, required=True),
    Parameter("password", argtype=str, required=True),
    Parameter("sensors", argtype=dict, required=True),
    Parameter("verify", argtype=bool, default=True),
]

MAPPER_TAP_SOMNEO = [
    Parameter("hostname", argtype=str, required=True),
]

MAPPER_TAP_TYPE = {
    "random": (RandomTap, MAPPER_TAP_RANDOM),
    "aranet": (AranetTap, MAPPER_TAP_ARANET),
    "somneo": (SomneoTap, MAPPER_TAP_SOMNEO),
}


def configure_logging(raw) -> None:
    config_logging = extract(raw.get("logging"), MAPPER_LOGGING)
    logging_level = logging.getLevelName(config_logging["level"].upper())
    logging.basicConfig(level=logging_level)


def configure_sink(raw) -> Sink:
    config_sink = extract(raw["sink"], MAPPER_SINK)
    return Sink(**config_sink)


def configure_taps(raw) -> List[Tap]:
    taps = []
    for tap_type, tap_instances in raw["tap"].items():
        for tap_name, tap_raw in tap_instances.items():
            tap_class, tap_mapper = MAPPER_TAP_TYPE[tap_type]
            tap_config = {"name": tap_name}
            tap_config.update(extract(tap_raw, MAPPER_TAP))
            tap_config.update(extract(tap_raw, tap_mapper))
            taps.append(tap_class(**tap_config))
    return taps


def configure(path: str) -> Sink:
    # Parse config
    config_raw = load(path)
    # Process config
    configure_logging(config_raw)
    sink = configure_sink(config_raw)
    for tap in configure_taps(config_raw):
        sink.register(tap)
    return sink
