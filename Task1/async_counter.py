from asyncio import Lock

class AsyncCounter:
    _lock: Lock
    _value: int

    def __init__(self):
        self._lock = Lock()
        self._value = 0

    async def increment(self) -> int:
        async with self._lock:
            self._value += 1
            return self._value