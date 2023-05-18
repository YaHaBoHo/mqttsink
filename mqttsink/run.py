import os
import logging
from .config.setup import setup

CONFIG_PATH = os.environ.get("MQTTSINK_CONFIG_PATH", "config.toml")


def run():
    logging.basicConfig(level=logging.INFO)
    sink = setup(CONFIG_PATH)
    sink.start()


if __name__ == "__main__":
    run()
