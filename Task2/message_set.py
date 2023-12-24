from asyncio import Lock
from collections import defaultdict
from datetime import datetime

import pandas as pd


class MessageSet:
    _COLUMNS = ["timestamp", "source", "message", "is_media_presented"]
    _START_TIMESTAMP_PLACEHOLDER = "start"
    _END_TIMESTAMP_PLACEHOLDER = "end"

    _lock: Lock
    _filename_template: str
    _messages: dict[str, list]
    _start_timestamp: datetime | None

    def __init__(self, filename_template: str):
        self._lock = Lock()

        start_placeholder = f"{{{self._START_TIMESTAMP_PLACEHOLDER}}}"
        end_placeholder = f"{{{self._END_TIMESTAMP_PLACEHOLDER}}}"
        if start_placeholder not in filename_template:
            filename_template += f"_{start_placeholder}"
        if end_placeholder not in filename_template:
            filename_template += f"_{end_placeholder}"
        self._filename_template = filename_template
        
        self._clear_data()

    async def add_message(self, timestamp: datetime, source: str, message: str, is_media_presented: bool):
        async with self._lock:
            if not self._start_timestamp:
                self._start_timestamp = datetime.now()

            messages = self._messages
            messages["timestamp"].append(timestamp)
            messages["source"].append(source)
            messages["message"].append(message)
            messages["is_media_presented"].append(is_media_presented)

            print(f"Message added: {', '.join([f'{k} = {v[-1]}' for k, v in self._messages.items()])}.")

    async def save_messages(self):
        if not self._start_timestamp:
            return
        async with self._lock:
            end_timestamp = datetime.now()
            format_args = {
                self._START_TIMESTAMP_PLACEHOLDER: MessageSet._timestamp_to_valid_file_name(self._start_timestamp),
                self._END_TIMESTAMP_PLACEHOLDER: MessageSet._timestamp_to_valid_file_name(end_timestamp)
            }
            filename = str.format(self._filename_template, **format_args)
            pd.DataFrame(self._messages).to_parquet(filename)
            self._clear_data()
            print(f"File saved as {filename}.")

    def _clear_data(self):
        self._start_timestamp = None
        self._messages = defaultdict(list)

    @staticmethod
    def _timestamp_to_valid_file_name(timestamp: datetime) -> str:
        return timestamp.strftime("%Y-%m-%dT%H-%M-%S")