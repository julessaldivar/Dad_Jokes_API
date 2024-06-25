from confluent_kafka import Consumer, KafkaError, KafkaException
import sys

KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':54872'

total_words = 0
joke_count = 0


def update_word_count(message):
    global total_words, joke_count
    joke = message.value().decode('utf-8')
    words = len(joke.split())
    joke_count += 1
    total_words += words

    if joke_count % 5 == 0:
        average_word_count = total_words / joke_count
        print(f'Average word count every 5 jokes: {average_word_count:.2f}')

    print(f'Total jokes: {joke_count}, Total words: {total_words}')


def main():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'word_count_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)

    try:
        consumer.subscribe([KAFKA_TOPIC])
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                update_word_count(msg)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close connection to consumer
        consumer.close()


if __name__ == '__main__':
    main()
