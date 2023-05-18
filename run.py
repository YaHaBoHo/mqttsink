import logging
from mqttsink.config.setup import setup


logging.basicConfig(level=logging.INFO)


def main():
    sink = setup("config.toml")
    sink.start()


if __name__ == "__main__":
    main()
