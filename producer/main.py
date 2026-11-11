import os, json, time, random, uuid
from confluent_kafka import Producer

KAFKA = os.environ["KAFKA_BOOTSTRAP"]
TOPIC = os.environ.get("TOPIC", "transactions")
RATE  = float(os.environ.get("RATE", "20"))   # tx per second

STATES = ["CA","NY","TX","FL","WA","IL"]
MERCH  = ["amazon","walmart","target","bestbuy","ebay"]
CATS   = [1,2,3,4,5,6]

p = Producer({"bootstrap.servers": KAFKA})

while True:
    tx = {
        "id": str(uuid.uuid4()),
        "amount": round(random.expovariate(1/80), 2),
        "hour": random.randint(0, 23),
        "us_state": random.choice(STATES),
        "merch": random.choice(MERCH),
        "cat_id": random.choice(CATS),
    }
    p.produce(TOPIC, json.dumps(tx).encode())
    p.poll(0)
    time.sleep(1.0 / RATE)
