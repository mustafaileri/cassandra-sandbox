import os
import json
import uuid
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.cqlengine import (
    columns,
    ValidationError,
)
from cassandra.cqlengine.connection import register_connection, set_default_connection
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table, create_keyspace_simple, drop_keyspace
from cassandra.auth import PlainTextAuthProvider

__all__ = ['Cassandra']


class SampleModel(Model):
    __keyspace__ = 'test_keyspace'
    __table_name__ = 'sample_table'

    id = columns.UUID(primary_key=True, default=uuid.uuid4)
    name = columns.Text(required=True)
    surname = columns.Text(required=True)
    title = columns.Text(required=True)
    skills = columns.Map(key_type=columns.Text, value_type=columns.Text, required=False, )
    created_at = columns.DateTime(default=datetime.utcnow)
    city_name = columns.Text(required=False)


class Cassandra:
    session = None
    cluster = None

    def __init__(self):
        self.connect()
        self.key_space = os.getenv('CASSANDRA_KEY_SPACE')

    def connect(self):
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

        self.cluster = Cluster(
            ['node_0', 'node_1'],
            auth_provider=auth_provider,
            # executor_threads=int(os.getenv('CASSANDRA_EXECUTOR_THREADS')),
            # protocol_version=int(os.getenv('CASSANDRA_PROTOCOL_VERSION')),
        )

        self.session = self.cluster.connect()

        register_connection(str(self.session), session=self.session)
        set_default_connection(str(self.session))

    def sync_table(self):
        return sync_table(SampleModel)

    def write(self, data):
        try:
            sample_model_data = SampleModel.create(
                name=str(data['name']),
                surname=str(data['surname']),
                title=str(data['title']),
                skills=data['skills'],
                city_name=str(data['city'])
            )
            print('Cassandra Data: {}'.format(dict(sample_model_data)))
        except ValidationError as e:
            print(e)

        return True

    def disconnect(self):
        self.cluster.shutdown()
