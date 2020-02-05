from flask import Flask
from flask_cors import CORS
from kafka import KafkaProducer
import pyart
import numpy as np
import pickle


app = Flask(__name__)
CORS(app, support_credentials=True)

def read_weather_data():
    filename = 'KTLX19910605_162126/KTLX19910605_162126'

    radar = pyart.io.read_nexrad_archive(filename)

    radar.fields['reflectivity']['data'][:, -10:] = np.ma.masked

    gatefilter = pyart.filters.GateFilter(radar)
    gatefilter.exclude_transition()
    gatefilter.exclude_masked('reflectivity')

    grid = pyart.map.grid_from_radars(
        (radar,), gatefilters=(gatefilter,),
        grid_shape=(1, 241, 241),
        grid_limits=((2000, 2000), (-123000.0, 123000.0), (-123000.0, 123000.0)),
        fields=['reflectivity'])

    return grid.fields['reflectivity']['data'][0]

def publish_message(producer, topic, value):
    try:
        producer.send(topic, value=value)
        producer.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

@app.route('/fetchWeatherData', methods=['POST'])
def fetch_weather_data():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }
    weather_data = read_weather_data()
    if len(weather_data) > 0:
        kafka_producer = connect_kafka_producer()
        serialize_weather_data = pickle.dumps(weather_data)
        publish_message(kafka_producer, 'topic2', serialize_weather_data)
        if kafka_producer is not None:
            kafka_producer.close()