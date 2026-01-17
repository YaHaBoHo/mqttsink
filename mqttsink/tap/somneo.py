import requests
import urllib3
from mqttsink.tap.core import Tap
from mqttsink.drop import Drop

# https://www.domoticz.com/forum/viewtopic.php?t=33033


class SomneoTap(Tap):
    SOURCE = "somneo"

    METRICS = {
        "mstmp": "temperature",
        "msrhu": "humidity",
        "mslux": "illuminance",
        "mssnd": "sound_pressure",
    }

    def __init__(self, hostname: str, verify: bool = False, timeout: int = 5, **kwargs):
        super().__init__(**kwargs)
        self.hostname = hostname
        self.verify = verify
        self.timeout = timeout
        # --- Initilization ---#
        if not self.verify:
            urllib3.disable_warnings()
        # --- Internals --- #
        self._url = f"https://{self.hostname}/di/v1/products/1"

    def fetch(self) -> list[Drop]:
        return self.fetch_sensors()

    def get(self, path: str) -> dict:
        return requests.get(
            f"{self._url}/{path}",
            verify=self.verify,
            timeout=self.timeout,
        ).json()

    def fetch_sensors(self) -> list[Drop]:
        raw_data = self.get(path="wusrd")
        output_data = []
        # Process sensor data
        for metric_id, metric_name in self.METRICS.items():
            try:
                output_data.append(
                    Drop(
                        name=self.name,
                        metric=metric_name,
                        data=raw_data[metric_id],
                    )
                )
            except KeyError:
                # Metric not supported by sensor, skipping...
                pass
        return output_data
