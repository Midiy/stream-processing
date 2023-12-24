import asyncio
import json
import math
import random
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum, auto
from typing import Any, Self

from async_timer import Timer
from confluent_kafka import Producer

from async_counter import AsyncCounter


class Distribution(StrEnum):
    UNIFORM = auto()
    GAUSSIAN = auto()
    EXPONENTIAL = auto()
    
    @classmethod
    def _missing_(cls, value: str) -> "Distribution | None":
        value = value.lower()
        for member in cls:
            if member.value == value:
                return member
        return None
    

class SensorType(StrEnum):
    TEMPERATURE = auto()
    PRESSURE = auto()
    HUMIDITY = auto()
    
    @classmethod
    def _missing_(cls, value: str) -> "SensorType | None":
        value = value.lower()
        for member in cls:
            if member.value == value:
                return member
        return None


@dataclass
class SensorArgs:
    sensor_name: str
    sensor_type: SensorType
    distribution: Distribution
    mean: float
    sd: float
    produce_every_seconds: int


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--kafka_server", type=str, default="localhost:9092")
    parser.add_argument("--kafka_topic", type=str, required=True)
    parser.add_argument("--sensor_settings_file", type=str, default="./sensor_settings.json")
    return parser.parse_args()

async def main():
    args = parse_args()
    with open(args.sensor_settings_file) as file:
        sensor_args: list[SensorArgs] = json.load(file, object_hook=dict_to_sensor_args)
    
    counter = AsyncCounter()
    timers = []
    try:
        for sensor_arg in sensor_args:
            timer = Timer(sensor_arg.produce_every_seconds, target=create_sensor(args, sensor_arg, counter))
            timer.start()
            timers.append(timer)
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        for timer in timers:
            timer.stop()
    
def dict_to_sensor_args(args: dict[str, Any]) -> SensorArgs:
    if "sensor_type" in args and isinstance(args["sensor_type"], str):
        args["sensor_type"] = SensorType(args["sensor_type"])
    if "distribution" in args and isinstance(args["distribution"], str):
        args["distribution"] = Distribution(args["distribution"])
    return SensorArgs(**args)

def create_sensor(args: Namespace, sensor_args: SensorArgs, counter: AsyncCounter):
    producer = Producer({
        "bootstrap.servers": args.kafka_server
    })

    async def write_sensor_data():
        key = await counter.increment()
        value = get_random_value(sensor_args.distribution, sensor_args.mean, sensor_args.sd)
        data = create_sensor_data(sensor_args.sensor_name, sensor_args.sensor_type, value)
        producer.produce(args.kafka_topic, key=bytes([key]), value=data)
        producer.flush()
        print(f"Put message {data} with key {key} at {args.kafka_topic}.")

    return write_sensor_data

def get_random_value(distribution: Distribution, mean: float, sd: float) -> float:
    match distribution:
        case Distribution.UNIFORM:
            offset = math.sqrt(3) * sd
            return random.uniform(mean - offset, mean + offset)
        case Distribution.GAUSSIAN:
            return random.gauss(mean, sd)
        case Distribution.EXPONENTIAL:
            assert mean == sd, "For an exponential distribution, the mean and standard deviation must be the same."
            return random.expovariate(1 / mean)

def create_sensor_data(sensor_name: str, sensor_type: SensorType, value: float) -> str:
    timestamp = datetime.now()
    msg = {
        "timestamp": timestamp.isoformat(),
        "sensor_name": sensor_name,
        "sensor_type": sensor_type,
        "value": value
    }
    return json.dumps(msg)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
