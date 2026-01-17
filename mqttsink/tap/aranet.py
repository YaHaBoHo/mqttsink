from binascii import hexlify
from hashlib import sha256
import requests
import urllib3
from mqttsink.tap.core import Tap
from mqttsink.drop import Drop


ENCODING = "utf-8"


def hash_sha256(text, rounds=1):
    # Initialize
    _hash = text
    # Process
    for _ in range(rounds):
        _hash = hexlify(sha256(_hash.encode(ENCODING)).digest()).decode(ENCODING)
    # Return
    return _hash


class AranetTap(Tap):
    SOURCE = "aranet"

    METRICS = {
        "t": "temperature",
        "h": "humidity",
        "co2": "carbon_dioxide",
        "batt": "battery",
    }

    def __init__(
        self,
        hostname: str,
        username: str,
        password: str,
        sensors: dict,
        verify: bool = False,
        timeout: int = 5,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.hostname = hostname
        self.username = username
        self.password = password
        self.sensors = sensors
        self.verify = verify
        self.timeout = timeout
        # --- Initilization ---#
        if not self.verify:
            urllib3.disable_warnings()
        # --- Internals --- #
        self._url = f"https://{self.hostname}/lua/api"

    # Tap methods

    def drop_names(self):
        return list(self.sensors.values())

    def fetch(self) -> list[Drop]:
        raw_data = self.poll()
        output_data = []
        for sensor_id, sensor_name in self.sensors.items():
            # Extract sensor data
            try:
                sensor_data = raw_data[sensor_id]
            except KeyError:
                self.logger.warning("No data for Aranet sensor %s", sensor_name)
                continue
            # Process sensor data
            for metric_id, metric_name in self.METRICS.items():
                try:
                    output_data.append(
                        Drop(
                            name=sensor_name,
                            metric=metric_name,
                            data=sensor_data[metric_id],
                        )
                    )
                except KeyError:
                    # Metric not supported by sensor, skipping...
                    pass
        return output_data

    # Aranet methods

    def hashpass(self, salt_permanent: str, salt_onetime: str) -> str:
        password_hash = hash_sha256(text=self.password, rounds=5)
        permanent_hash = hash_sha256(text=password_hash + salt_permanent)
        return hash_sha256(text=salt_onetime + permanent_hash)

    def post(self, payload: dict) -> dict:
        return requests.post(
            self._url,
            json=payload,
            verify=self.verify,
            timeout=self.timeout,
        ).json()

    def poll(self) -> dict:
        # Fetch salts from API
        preauth = self.post(payload={"auth": {"username": self.username}})
        # Prepare payload
        payload = {
            "currData": 1,
            "auth": {
                "username": self.username,
                "hash": self.hashpass(
                    salt_permanent=preauth["auth"]["permasalt"],
                    salt_onetime=preauth["auth"]["salt"],
                ),
            },
        }
        # Fetch data from API
        data = self.post(payload=payload)
        # Return sensor data
        return data["currData"]
