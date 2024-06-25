import requests
from confluent_kafka import Producer
import socket
import time


KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':54872'


def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s' % err)
    else:
        print('%% Message delivered to %s [%d]' % (msg.topic(), msg.partition()))


def get_joke():
    headers = {"Accept": "application/json"}
    response = requests.get('https://icanhazdadjoke.com/', headers=headers)
    joke_data = response.json()
    return joke_data['joke']


def main():
    # Kafka producer configuration
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': socket.gethostname()
    }

    # Create Kafka producer instance
    producer = Producer(conf)

    while True:
        joke = get_joke()
        producer.produce(KAFKA_TOPIC, value=joke, callback=delivery_callback)

        # Flush messages to Kafka to ensure they are sent immediately
        producer.flush()
        # Wait 5 seconds to loop again
        time.sleep(5)

    # Close Kafka producer
    producer.close()


if __name__ == '__main__':
    main()
