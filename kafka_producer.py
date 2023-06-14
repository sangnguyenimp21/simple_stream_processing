from confluent_kafka import Producer
import requests
import uuid
import json
import time
import random
import configparser


config = configparser.RawConfigParser()
config.read('config.ini')
api_key = config['api_ninjas']['api_key']

def fetch_sentence(count = 0):
    category_list = ['happiness',
        'success',
        'life',
        'love',
        'failure',
        'faith',
        'family',
        'famous',
        'fear',
        'fitness',
        'food',
        'forgiveness',
        'freedom',
        'friendship',
        'funny',
        'future',
        'god',
        'good',
        'government',
        'graduation',
        'great',
        'happiness',
        'health',
        'history',
        'home',
        'hope',
        'humor',
        'imagination',
        'inspirational',
        'intelligence',
        'jealousy',
        'knowledge',
        'leadership',
    ]
    category = random.choice(category_list)
    api_url = 'https://api.api-ninjas.com/v1/quotes?category={}'.format(category)
    response = requests.get(api_url, headers={'X-Api-Key': api_key})
    if response.status_code == requests.codes.ok:
        response = json.loads(response.text)[0]
        new_message = {
            "quote": response['quote'],
            "author": response['author'],
            "category": response['category']
        }
        return count+1, new_message
    else:
        print("Error:", response.status_code, response.text)

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
        if topic == 'hit_count':
            count, base_message = generate_random_time_series_data(count)
        
        if topic == 'rate_exchange':
            count, base_message = fetching_exchange_rate_data(count)

        if topic == 'fetch_quote':
            count, base_message = fetch_sentence(count)
        
        total-=1

        record_key = str(uuid.uuid4())
        record_value = json.dumps(base_message)

        p.produce(topic, key=record_key, value=record_value)
        p.poll(0)   

    p.flush()
    print('we\'ve sent {count} messages to {brokers}'.format(count=count, brokers=bootstrap_servers))

if __name__ == "__main__":
    kafka_producer(topic = 'fetch_quote', total = 10)