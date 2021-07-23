import json
from time import sleep
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import requests



def get_needed_method(entity_name):

    r = requests.get(f'http://localhost:1026/v2/entities/{entity_name}')
    hashmap = json.loads(r.content.decode())
    desc = hashmap.get('description', None)
    if desc != 'The requested entity has not been found. Check type and id':
        return 'patch'
    return 'post'



if __name__ == '__main__':

    #iot_device_type = "device"
    headers = {'Content-Type': 'application/json'}
    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    topic_partition = TopicPartition(topic='test', partition=0)
    consumer.assign([topic_partition])

    offset = 0

    while True:

        consumer.seek(topic_partition, offset)

        if consumer:

            for msg in consumer:

                key = msg.key.decode()
                value = msg.value.decode()
                method = get_needed_method(key)

                if method == 'post':
                    post_message = { "id": key, "type": "iot_device", key: {"metadata": {}, "type": "Integer", "value": value }}
                    r = requests.post("http://localhost:1026/v2/entities", data=json.dumps(post_message), headers=headers)
                    print(f"N{offset} Post for the enitity {key} request was successfully completed")

                else:
                    patch_message = {key: {'value': value, 'type': 'Integer'}} 
                    r = requests.patch(f"http://localhost:1026/v2/entities/{key}/attrs", data=json.dumps(patch_message), headers=headers)
                    print(f"N{offset} Patch for the enitity {key} request was successfully completed")

                offset += 1

        sleep(2)

    consumer.close()
