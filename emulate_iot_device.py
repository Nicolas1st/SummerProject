from kafka import KafkaConsumer, KafkaProducer


if __name__ == '__main__':

    """ Test script """

    api_version = (0, 10)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=api_version)
    topic_name = 'test'

    key = input("Key: ")
    key_bytes = bytes(key, encoding='utf-8')

    value = input("Value: ")
    value_bytes = bytes(value, encoding='utf-8')

    producer.send(topic_name, key=key_bytes, value=value_bytes)
    producer.flush()

    print('Message published successfully.')
