from confluent_kafka import Producer
import requests
import uuid
import json
import time
import random

def generate_random_time_series_data(count = 0):
    new_message = {
        "hits":random.randint(10,100),
        "timestamp":time.time(),
        "userId":random.randint(15,20)
    }
    return count+1, new_message

def fetching_exchange_rate_data(count = 0):
    res = requests.get('https://open.er-api.com/v6/latest/USD')
    response = json.loads(res.text)
    new_message = {
        "time_last_update": response['time_last_update_utc'],
        "rates": response['rates']
    }
    return count+1, new_message

def kafka_producer(topic = 'hit_count', total = 10):
    bootstrap_servers = "localhost:29092"
    p = Producer({'bootstrap.servers': bootstrap_servers})
    count = 0 
    while total:
        # count, base_message = generate_random_time_series_data(count)
        count, base_message = fetching_exchange_rate_data(count)
        total-=1

        record_key = str(uuid.uuid4())
        record_value = json.dumps(base_message)

        p.produce(topic, key=record_key, value=record_value)
        p.poll(0)   

    p.flush()
    print('we\'ve sent {count} messages to {brokers}'.format(count=count, brokers=bootstrap_servers))

if __name__ == "__main__":
  kafka_producer(topic = 'exchange_rate', total = 10)
#   fetching_exchange_rate_data()