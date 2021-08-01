from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer


class TestKafka:

    def __init__(self, host="localhost:9092"):
        self.admin_client = KafkaAdminClient(bootstrap_servers=host, client_id='test')

    def create_topic(self, topic_name):
        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        self.admin_client.create_topics(new_topics=topic_list, validate_only=False)

    def delete_topic(self, topic_name):
        self.admin_client.delete_topics(topic_name)

    def list_all_topics(self, host="localhost:9092"):
        self.admin_client = KafkaAdminClient(bootstrap_servers=host, client_id='test')
        print(self.admin_client.list_topics())

    def send_file_to_kafka_topic(self, topic, input_file, host="localhost:9092"):
        producer = KafkaProducer(bootstrap_servers=host)
        file = open(input_file, "r")
        value = file.read()
        producer.send(topic, value.encode("utf-8"))
        return

    def kafka_consumer(self, topic, output_file, host="localhost:9092"):
        consumer = KafkaConsumer(topic, bootstrap_servers=host, auto_offset_reset='earliest', consumer_timeout_ms=1000)
        file = open(output_file, "w+")
        output = ""
        for message in consumer:
            if message.value is not None:
                output += message.value.decode('utf-8')
            else:
                output += "\n"
        file.write(output)


x = TestKafka()
x.list_all_topics()
x.create_topic("topic2")
x.list_all_topics()
x.send_file_to_kafka_topic("topic2", "./Resources/input.txt")
x.kafka_consumer("topic2", "output.txt")
# x.delete_topic(["testing1212"])