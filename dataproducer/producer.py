import time
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
import json

fake = Faker()

class DataProducer:
    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers = self.broker, \
                                      value_serializer = lambda v: json.dumps(v).encode('utf-8'))
        

    def sendata(self, data):
        try:
            self.producer.send(self.topic, data)
            self.producer.flush()
            print(f'data sent : {data}')
        except Exception as e:
            print(f'error sending data : {e}')

    def data(self):
        while True:
            patient = {
                    'name': fake.name(),
                    'age': fake.pyint(min_value=18, max_value=80),
                    'disease': fake.random_element(elements=('Diabetes', 'Hypertension', 'Asthma', 'Arthritis', 'Depression')),
                    'measurement_type': fake.random_element(elements=('Temperature', 'Blood Pressure', 'Glucose', 'Cholesterol', 'Weight')),
                    'measurement_value': fake.pyfloat(min_value=50, max_value=200),
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            self.sendata(patient)
            time.sleep(fake.pyint(min_value = 5, max_value = 10))

if __name__ == '__main__':
    dataprod = DataProducer(broker='localhost:9092', topic='arhvdata')
    dataprod.data()
        


