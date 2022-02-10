import os
import json

from kafka import KafkaConsumer

from vivarium.core.engine import Engine, pf
from vivarium.core.process import (
    Process,
)

from vivarium.composites.toys import ToyTransport

class ConsumerProcess(Process):
    defaults = {
        'receiver': -1, # pass in an id
        'topic': 'vivarium-consumer',
        'group_id': 'vivarium-simulation',
        'bootstrap_servers': 'localhost:9092'}

    def __init__(self, parameters=None):
        super().__init__(parameters)
        self.consumer = KafkaConsumer(
            self.parameters['topic'],
            group_id=self.parameters['group_id'],
            bootstrap_servers=self.parameters['bootstrap_servers'])

    def ports_schema(self):
        return {
            'top': {'_output': True}}

    def update_condition(self, timestep, state):
        return True

    def next_update(self, timestep, state):
        receiver = False
        while not receiver:
            from_kafka = next(self.consumer)
            value = from_kafka.value.decode('utf-8')
            print(f'we received {value}')
            if value is not None:
                message = json.loads(value)
                print(f'consumer received: {message}')
                receiver = message['receiver'] == self.parameters['receiver']

        if message.get('update'):
            return {'top': message['update']}
        else:
            return {}


def test_consumer():
    # kafka_consumer = KafkaConsumer(
    #     'test-consumer',
    #     group_id='test-group',
    #     bootstrap_servers='localhost:9092')

    # print('starting loop')
    # for message in kafka_consumer:
    #     print('waiting for message')
    #     print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
    #                                           message.offset, message.key,
    #                                           message.value))

    consumer = ConsumerProcess({
        'topic': 'test-consumer',
        'group_id': 'test-group'})

    transport = ToyTransport()

    sim = Engine(
        processes={
            'consumer': consumer,
            'transport': transport},
        topology={
            'consumer': {'top': ()},
            'transport': {
                'internal': ('internal',),
                'external': ('external',)}},
        initial_state={
            'external': {
                'GLC': 10.0}})

    sim.update(10)

    data = sim.emitter.get_data()
    print(pf(data))

if __name__ == '__main__':
    test_consumer()

    
