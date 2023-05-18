from __future__ import annotations
import time
import socket
import logging
from typing import List, Optional
from paho.mqtt.client import Client
from .exceptions import MqttError
from .tap.core import Tap
from .drop import Drop


DISCONNECT_RC = {
    0: "Connection successful",
    1: "Incorrect protocol version",
    2: "Invalid client identifier",
    3: "Server unavailable",
    4: "Bad username or password",
    5: "Not authorised",
}


def disconnect_status(rc: int) -> str:
    return DISCONNECT_RC.get(rc, "Unknown error")


class Sink:
    LOOP: int = 1
    NAME: Optional[str] = None
    DELIMITER: str = "/"
    RECONNECT_ABORT: List[int] = [4, 5]
    RECONNECT_INTERVAL: int = 5
    KEEPALIVE: int = 60

    logger = logging.getLogger("mqttsink.sink")

    def __init__(
        self,
        hostname: str = "localhost",
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        name: str = "mqttsink",
    ):
        # ----- Config ----- #
        self.name = name
        self.hostname = hostname
        self.port = 1883 if port is None else port
        # Credentials
        if (password is not None) and (username is None):
            raise ValueError("You cannot specify a password without a username.")
        self._username = username
        self._password = password
        # ----- Internals ----- #
        self._mqtt = None
        self._running = False
        self._taps: List[Tap] = []

    @property
    def mqtt(self) -> Client:
        if not self._mqtt:
            _mqtt = Client(client_id=self.name)
            _mqtt.enable_logger(logger=self.logger)
            if self._username:
                _mqtt.username_pw_set(
                    username=self._username,
                    password=self._password,
                )
            # >>> TLS will go here <<< #
            _mqtt.on_connect = self._on_connect
            _mqtt.on_disconnect = self._on_disconnect
            self._mqtt = _mqtt
        return self._mqtt

    @property
    def _connected(self):
        return self.mqtt.is_connected()

    # MQTT Callbacks

    def _on_connect(
        self,
        _client: Client,
        _userdata: str,
        _flags: dict,
        _rc: int,
    ) -> None:
        self.logger.info("Connected to %s.", self.hostname)

    def _on_disconnect(
        self,
        _client: Client,
        _userdata: str,
        rc: int,
    ) -> None:
        if rc > 0:
            self.logger.error("MQTT connection error : %s", disconnect_status(rc))
            if rc in self.RECONNECT_ABORT:
                self._running = False
                raise MqttError(
                    f"Unrecoverable MQTT connection error : {disconnect_status(rc)}"
                )
        self.logger.info("Disconnected from %s.", self.hostname)
        # Reconnect if needed
        if self._running:
            self.logger.info("Client still running, will try to reconnect")
            self.connect()

    # MQTT Operation

    def connect(self) -> None:
        self._running = True
        while True:
            self.logger.info("Connecting to %s...", self.hostname)
            try:
                if self._connected:
                    self.mqtt.reconnect()
                else:
                    self.mqtt.connect(
                        host=self.hostname,
                        port=self.port,
                        keepalive=self.KEEPALIVE,
                    )
                return
            except (socket.timeout, ConnectionError) as err:
                self.logger.info(
                    "Could not connect to %s (%s). Will retry in %ss...",
                    self.hostname,
                    err,
                    self.RECONNECT_INTERVAL,
                )
                time.sleep(self.RECONNECT_INTERVAL)

    def publish(self, topic: str, payload: str) -> None:
        try:
            msg = self.mqtt.publish(topic, payload=payload)
            msg.wait_for_publish()
        except TypeError as err:
            # TODO : Custom exception
            self.logger.error("Could not publish message : %s", err)
        except RuntimeError as err:
            self.logger.error("Encoutered MQTT error : %s", err)
            if self._running and not self._connected:
                self.connect()

    def disconnect(self) -> None:
        self._running = False
        self.logger.info("Disonnecting from %s...", self.hostname)
        self.mqtt.disconnect()

    # MQTT Sink

    def _topic(self, *args):
        return self.DELIMITER.join(args)

    def register(self, tap: Tap) -> None:
        self._taps.append(tap)

    def initialize(self):
        for tap in self._taps:
            tap.initalize()
            tap_topics = [
                self._topic(self.name, tap.SOURCE, tap.name, drop_name)
                for drop_name in tap.drop_names()
            ]
            self.logger.info(
                "Tap %s initialized. Will publish in %s",
                tap.fullname,
                tap_topics,
            )

    def submit(self):
        for tap in self._taps:
            for drop in tap.collect():
                self.publish(
                    topic=self._topic(self.name, tap.SOURCE, tap.name, drop.name),
                    payload=drop.payload,
                )

    def cleanup(self):
        for tap in self._taps:
            tap.cleanup()

    def start(self) -> None:
        # TODO : Signals
        # TODO : cleanup @ finally
        self.initialize()
        self.connect()
        self.mqtt.loop_start()
        while self._running or self._connected:
            self.submit()
            time.sleep(self.LOOP)
        self.mqtt.loop_stop()
        self.cleanup()

    def stop(self) -> None:
        self.logger.info("Stop signal recieved")
        self.disconnect()
