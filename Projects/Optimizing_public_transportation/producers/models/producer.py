"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "schema.registry.url": SCHEMA_REGISTRY_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=key_schema,
            default_value_schema=value_schema,
            
        )

    def topic_exists(self, client):
        """Checks if the given topic exists"""
        topic_metadata = client.list_topics(timeout=5)
        return topic_metadata.topics.get(self.topic_name) is not None

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        client = AdminClient({'bootstrap.servers':BROKER_URL})
        exists = self.topic_exists(client)
        logger.info(f"Topic {self.topic_name} exists: {exists}")
        
        if not topic_exists:
            futures = client.create_topic(
                [NewTopic(topic=self.topic_name,  num_partitions=self.num_partitions, replication_factor=self.num_replicas)])

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Topic {self.topic_name} created")
                except Exception as e:
                    logger.error(f"failed to create topic {self.topic_name}: {e}")
                    raise


    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # TODO: Write cleanup code for the Producer here
        if self.producer:
            self.producer.flush()
            logger.info("producer close")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
