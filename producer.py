import requests, json
from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    response = requests.get("https://disease.sh/v3/covid-19/countries")
    if response.ok:
        data = response.json()
        producer.send('covid-data', value=data)
    time.sleep(600)  # every 10 mins
