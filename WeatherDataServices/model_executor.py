from flask import Flask
from flask_cors import CORS
from time import sleep
from kafka import KafkaConsumer, KafkaProducer
import pickle

app = Flask(__name__)
CORS(app, support_credentials=True)

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

@app.route('/fetchKafkaData', methods=['POST'])
def fetch_data_from_topic():
    print('Running Consumer..')
    topic_name = 'topic2'

    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer:
        data = pickle.loads(msg.value)
        print(data)
    consumer.close()
    sleep(5)
