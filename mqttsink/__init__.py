import logging
from .config.setup import setup


def run():
    logging.basicConfig(level=logging.INFO)
    sink = setup("config.toml")
    sink.start()


if __name__ == "__main__":
    run()