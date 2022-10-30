import json 
from kafka import KafkaProducer
import env
from kafka import KafkaClient
from kafka.admin import KafkaAdminClient, NewTopic


def create_topic():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=env.bootstrap_servers.split(",")
        )
        topic_list = [NewTopic(name=env.kafka_topic, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except:
        pass

def add_topic():
    try:
        client = KafkaClient(bootstrap_servers=env.bootstrap_servers.split(","))
        client.add_topic(env.kafka_topic)
    except:
        pass

def show_topics():
    try:
        client = KafkaClient(bootstrap_servers=env.bootstrap_servers.split(","))
        future = client.cluster.request_update()
        client.poll(future=future)
        metadata = client.cluster
        print(metadata.topics())
    except:
        pass

def send_data_to_kp():
    try:
        producer =KafkaProducer(bootstrap_servers=env.bootstrap_servers.split(","))
        processed_path = r"{}/{}/{}".format(os.getcwd(), env.processed_input_path, env.processed_input_file)
        with open(processed_path) as f:
            for index, line in enumerate(f):
                producer.send(env.kafka_topic, line.encode('utf-8'))
                print("{} lines loaded".format(index))
    except:
        raise

if __name__=="__main__":
    create_topic()
    add_topic()
    show_topics()
    send_data_to_kp()