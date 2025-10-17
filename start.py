from extract import MeteoKafkaProducer

print("start")

if __name__ == "__main__":
    KAFKA_BROKER = 'localhost:9092'
    TOPIC = 'meteo_toulouse'
    API_URL = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/00-station-meteo-toulouse-valade/records?limit=20"
    INTERVAL = 60  # en secondes

    producer = MeteoKafkaProducer(KAFKA_BROKER, TOPIC, API_URL, INTERVAL)
    producer.run()