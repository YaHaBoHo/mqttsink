from __future__ import annotations
import time
import socket
import logging
from typing import List, Optional
from paho.mqtt.client import Client
from .exceptions import MqttError
from .tap.core import Tap


MQTT_RC = {
    0: "Connection successful",
    1: "Incorrect protocol version",
    2: "Invalid client identifier",
    3: "Server unavailable",
    4: "Bad username or password",
    5: "Not authorised",
}


def mqtt_status(rc: int) -> str:
    return MQTT_RC.get(rc, "Unknown error")


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
        self._next = 0

    @property
    def interval(self) -> int:
        return max(self.KEEPALIVE - 5 * self.LOOP, self.LOOP)

    @property
    def due(self) -> bool:
        return time.time() >= self._next

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
            _mqtt.on_connect = self._on_connect
            _mqtt.on_disconnect = self._on_disconnect
            self._mqtt = _mqtt
        return self._mqtt

    @property
    def _connected(self):
        return self.mqtt.is_connected()

    # MQTT Callbacks

    def _handle_failure(self, rc: int):
        self.logger.error("MQTT connection error : %s", mqtt_status(rc))
        if rc in self.RECONNECT_ABORT:
            self._running = False
            raise MqttError(f"Unrecoverable MQTT connection error : {mqtt_status(rc)}")

    def _on_connect(self, _client: Client, _data: str, _flags: dict, rc: int) -> None:
        if rc == 0:
            self.logger.info("Connected to %s", self.hostname)
        else:
            self._handle_failure(rc)

    def _on_disconnect(
        self,
        _client: Client,
        _userdata: str,
        rc: int,
    ) -> None:
        if rc != 0:
            self._handle_failure(rc)
        self.logger.info("Disconnected from %s", self.hostname)
        self.mqtt.loop_stop()
        # Reconnect if needed
        if self._running:
            self.logger.info("Client still running, will try to reconnect")
            self.connect()

    # MQTT Operation

    def connect(self) -> None:
        while self._running:
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
                    self.mqtt.loop_start()
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
        self.logger.debug("Publishing to %s...", topic)
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
        self.logger.info("Disonnecting from %s...", self.hostname)
        self.mqtt.disconnect()

    # MQTT Sink

    @property
    def path(self) -> List[str]:
        return [self.name]

    def topic(self, *args):
        return self.DELIMITER.join(args)

    def register(self, tap: Tap) -> None:
        self._taps.append(tap)

    def initialize(self) -> None:
        for tap in self._taps:
            tap.initalize()
            self.logger.info("Tap %s initialized.", tap.fullname)

    def submit(self) -> None:
        # Heartbeat due?
        if self.due:
            self._next = time.time() + self.interval
            self.publish(topic=self.topic(*self.path, "heartbeat"), payload="0")
        # Collect taps
        for tap in self._taps:
            for drop in tap.collect():
                self.publish(
                    topic=self.topic(*self.path, *tap.path, *drop.path),
                    payload=drop.payload,
                )

    def cleanup(self) -> None:
        for tap in self._taps:
            tap.cleanup()

    def start(self) -> None:
        # TODO : cleanup @ finally
        self.logger.info("Starting mqttsink...")
        self._running = True
        self.initialize()
        self.connect()
        while self._running or self._connected:
            self.submit()
            time.sleep(self.LOOP)
        self.cleanup()

    def stop(self) -> None:
        self.logger.info("Stopping mqttsink...")
        self._running = False
        self.disconnect()
