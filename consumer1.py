
from kafka import KafkaConsumer
import json
from apriori import apriori_algorithm
import logging

# Setup logging for better debug visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def consume_data(min_support, window_size):
    """ Consume data from a Kafka topic, process using Apriori algorithm. """
    consumer = KafkaConsumer(
        'amazon_data_topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        stream_data = message.value  # Extract the message value
        if 'also_buy' in stream_data and isinstance(stream_data['also_buy'], list):
            frequent_itemsets = apriori_algorithm(stream_data['also_buy'], min_support, window_size)
            logging.info("Frequent Itemsets Apriori Algorithm:")
            logging.info(frequent_itemsets)
        else:
            logging.warning("Missing 'also_buy' or incorrect data format in message.")

if __name__ == "__main__":
    consume_data(min_support=0.01, window_size=100)  # Adjust min_support as needed for your dataset size
