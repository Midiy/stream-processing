import asyncio
import json
from argparse import ArgumentParser, Namespace

from async_timer import Timer
from confluent_kafka import Consumer, TopicPartition

from value_set import ValueSet


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--kafka_server", type=str, default="localhost:9092")
    parser.add_argument("--kafka_topic", type=str, required=True)
    parser.add_argument("--kafka_consumer_group", type=str, default="group1")
    parser.add_argument("--aggregation_window_seconds", type=int, default=60)
    return parser.parse_args()

async def main():
    args = parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.kafka_server,
        "group.id": args.kafka_consumer_group,
        "auto.offset.reset": "earliest"
    })
    topic_partition = TopicPartition(topic=args.kafka_topic, partition=0, offset=0)

    value_set = ValueSet()
    try:
        consumer.assign([topic_partition])
        consumer.seek(topic_partition)
        async with Timer(args.aggregation_window_seconds, target=get_aggregator(value_set)):
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    await asyncio.sleep(1)
                    continue
                consumer.commit(message=msg)
                value = json.loads(msg.value())
                if value is not None:
                    await value_set.add_value(**value)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def get_aggregator(value_set: ValueSet):
    async def aggregator():
        df = await value_set.take_dataframe()
        df1 = df.groupby(df.sensor_type).agg({"value": ["mean"]})
        df2 = df.groupby(df.sensor_name).agg({"value": ["mean"]})
        
        print()
        print(df1)
        print()
        print(df2)
        print()

    return aggregator


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
