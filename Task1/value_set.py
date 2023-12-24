from asyncio import Lock
from collections import defaultdict
from datetime import datetime

import pandas as pd


class ValueSet:
    _COLUMNS = ["timestamp", "sensor_name", "sensor_type", "value"]

    _lock: Lock
    _values: dict[str, list]

    def __init__(self):
        self._lock = Lock()
        self._values = defaultdict(list)

    async def add_value(self, timestamp: datetime, sensor_name: str, sensor_type: str, value: float):
        async with self._lock:
            values = self._values
            values["timestamp"].append(timestamp)
            values["sensor_name"].append(sensor_name)
            values["sensor_type"].append(sensor_type)
            values["value"].append(value)

            print(f"Value added: {', '.join([f'{k} = {v[-1]}' for k, v in self._values.items()])}.")

    async def take_dataframe(self) -> pd.DataFrame:
        async with self._lock:
            result = pd.DataFrame(self._values)
            self._values = defaultdict(list)
            print(f"Dataframe with {result.shape[0]} rows was taken.")
            return result
