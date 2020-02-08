from flask import Flask
from flask_cors import CORS
from kafka import KafkaProducer
import pyart
import numpy as np
import pickle
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError, ConnectionLossException
import json
from flask_script import Manager

app = Flask(__name__)
manager = Manager(app)
CORS(app, support_credentials=True)

def registerFetchWeatherDataService(host, port):
    print("in zookeeper")
    zk = KazooClient(hosts='localhost', read_only=True)
    zk.start()
    path = '/WeatherData'
    pass_data=json.dumps({"host":host, "port":port}).encode('utf-8')
    print(pass_data)
    try:
        zk.create(path,value=pass_data,ephemeral=True,makepath=True)
        print("Auth Service is running '"+path+"' here.")
    except NodeExistsError:
        print("Node already exists in Zookeeper")

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
        return True
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))
        return False


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


@app.route('/getWeatherData', methods=['POST'])
def get_weather_data():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }
    weather_data = read_weather_data()
    if len(weather_data) > 0:
        kafka_producer = connect_kafka_producer()
        serialize_weather_data = pickle.dumps(weather_data)
        status = publish_message(kafka_producer, 'WeatherData', serialize_weather_data)
        if kafka_producer is not None:
            kafka_producer.close()
        if status:
            return "success"
        else:
            return "failure"

if __name__ == "__main__":
    registerFetchWeatherDataService('127.10.10.7', 3000)
    app.run(host='127.10.10.7', port=3000)
