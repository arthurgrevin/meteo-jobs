from kafka import KafkaConsumer
import json

class MeteoKafkaConsumer:
    def __init__(self,
                 kafka_broker,
                 topic,
                 group_id="meteo_consumer_group",
                 auto_offset_reset="earliest"):
        """
        Initialise le consumer Kafka pour les données météo.

        :param kafka_broker: adresse du broker Kafka (ex: 'localhost:9092')
        :param topic: topic Kafka à consommer
        :param group_id: ID du groupe de consommateurs Kafka
        :param auto_offset_reset: 'earliest'
        pour lire depuis le début ou 'latest' pour ne lire que les nouveaux messages
        """
        self.kafka_broker = kafka_broker
        self.topic = topic
        self.group_id = group_id

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=[self.kafka_broker],
            group_id=self.group_id,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

    def process_message(self, message):
        """
        Méthode à surcharger pour traiter les messages.
        """
        print("Received message:", message)

    def run(self):
        """
        Boucle infinie pour consommer les messages du topic.
        """
        print(f"Starting MeteoKafkaConsumer on topic '{self.topic}'...")
        try:
            for message in self.consumer:
                self.process_message(message.value)
        except Exception as e:
            print("Error:", e)
        finally:
            self.consumer.close()
            print("Consumer closed.")
