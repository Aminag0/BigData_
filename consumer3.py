# consumer3.py

from kafka import KafkaConsumer
from textblob import TextBlob
import json

def consume_data():
    """ Consume data from a Kafka topic. """
    consumer = KafkaConsumer(
        'amazon_data_topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    for message in consumer:
        perform_sentiment_analysis(message.value)

def perform_sentiment_analysis(data):
    """ Perform sentiment analysis on product reviews and print the results. """
    if 'description' in data:  # Assuming 'description' field contains the review text
        review_text = data['description']
        blob = TextBlob(review_text)
        sentiment_score = blob.sentiment.polarity
        sentiment = 'Positive' if sentiment_score > 0 else 'Negative' if sentiment_score < 0 else 'Neutral'
        
        print("Review: ", review_text)
        print("Sentiment: ", sentiment, "(Polarity Score: ", sentiment_score, ")")
        print("="*50)
    else:
        print("No review found in the data:", data)

if __name__ == "__main__":
    consume_data()

