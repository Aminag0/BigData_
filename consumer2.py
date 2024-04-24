from kafka import KafkaConsumer
import json
import logging
from pcy import pcy_algorithm, sliding_window

# Setup logging for better debug visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def consume_data(min_support, window_size, hash_table_size):
    """ Consume data from a Kafka topic, process using PCY algorithm. """
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
            frequent_itemsets, frequent_pairs = pcy_algorithm(stream_data['also_buy'], min_support, window_size, hash_table_size)
            logging.info("Frequent Itemsets using PCY Algorithm:")
            logging.info("Frequent Itemsets: %s", frequent_itemsets)
            logging.info("Frequent Pairs: %s", frequent_pairs)
        else:
            logging.warning("Missing 'also_buy' or incorrect data format in message.")

if __name__ == "__main__":
    min_support = 0.01  # Adjust min_support as needed for your dataset size
    window_size = 100
    hash_table_size = 10000  # Adjust hash_table_size as needed
    consume_data(min_support, window_size, hash_table_size)

