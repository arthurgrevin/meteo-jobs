from consume import MeteoKafkaConsumer

if __name__ == "__main__":
    KAFKA_BROKER = 'localhost:9092'
    TOPIC = 'meteo_toulouse'

    consumer = MeteoKafkaConsumer(KAFKA_BROKER, TOPIC)
    consumer.run()