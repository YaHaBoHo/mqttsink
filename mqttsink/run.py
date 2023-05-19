import os
import signal
import logging
from .config.configure import configure

CONFIG_PATH = os.environ.get("MQTTSINK_CONFIG_PATH", "config.toml")


def run():
    # Initialize
    logging.basicConfig(level=logging.INFO)
    sink = configure(CONFIG_PATH)
    # Register termination signals
    for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGQUIT]:
        signal.signal(sig, lambda *x: sink.stop())
    # Start
    sink.start()


if __name__ == "__main__":
    run()
