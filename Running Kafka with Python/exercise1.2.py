import asyncio
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

Broker_URL ="PLAINTEXT://localhost:9092"
topic_name="First-Topic"

async def produce(topic_name):
    # Basically producing data in the given topic_name
    # creating a producer
    p=Producer({"bootstrap.servers":Broker_URL})
    curr_iteration=0
    while True:
        p.produce(topic_name,f"iteration {curr_iteration}".encode('utf-8'))
        curr_iteration+=1
        await asyncio.sleep(1)

async def consume(topic_name):
    # creating a consumer
    c=Consumer({"bootstrap.servers":Broker_URL,"group.id":"My First Consumer Group"})
    c.subscribe([topic_name])

    while True:
        message=c.poll(1.0)

        if message is None:
            print("no Message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer{message.error()}")
        else:
            print(f"consumed message {message.key()} : {message.value()}")
        await asyncio.sleep(1)


async def produce_consume():
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2

def main():
    client=AdminClient({"bootstrap.servers":Broker_URL})
    topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    client.create_topics([topic])

    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
            client.delete_topics([topic])


if __name__ == "__main__":
    main()





    

