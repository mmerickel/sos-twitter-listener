from cached_property import cached_property
from datetime import datetime
import json
import logging
import os
import pika

from . import zstd

log = logging.getLogger(__name__)

class CompositeOutputStream:
    def __init__(self, streams=()):
        self.streams = list(streams)

    def add_stream(self, stream):
        self.streams.append(stream)

    def close(self):
        for stream in self.streams:
            stream.close()

    def rotate(self):
        for stream in self.streams:
            stream.rotate()

    def on_status(self, status):
        for stream in self.streams:
            stream.on_status(status)

class StdoutOutputStream:
    def close(self):
        pass

    def rotate(self):
        pass

    def on_status(self, status):
        print(json.dumps(status))

class FileOutputStream:
    path = None
    fp = None

    def __init__(self, path_prefix):
        self.path_prefix = path_prefix

    @cached_property
    def writer(self):
        now = datetime.utcnow()
        path = f'{self.path_prefix}.{now:%Y%m%d.%H%M%S}.zstd'
        log.info(f'opening path={path}')
        root_path = os.path.dirname(path)
        if root_path:
            os.makedirs(root_path, exist_ok=True)
        self.fp = open(path, mode='ab')
        self.path = path
        return zstd.writer(self.fp)

    def close(self):
        if self.path is not None:
            log.info(f'closing path={self.path}')
            try:
                self.writer.close()
            except Exception:
                log.exception('failed to close zstd writer')
            try:
                self.fp.close()
            except Exception:
                log.exception('failed to close file')
            self.path = self.fp = None
            del self.__dict__['writer']

    def rotate(self):
        self.close()

    def on_status(self, status):
        self.writer.write(json.dumps(status).encode('utf8'))

class RabbitMqOutputStream:
    def __init__(self, parameters, exchange, routing_key):
        self.parameters = parameters
        self.exchange = exchange
        self.routing_key = routing_key
        self.connection = None

    @classmethod
    def from_profile(cls, profile, exchange, routing_key):
        return cls(
            pika.URLParameters(profile['url']),
            exchange,
            routing_key,
        )

    @cached_property
    def channel(self):
        log.info(f'opening new rabbitmq connection={self.parameters}')
        connection = pika.BlockingConnection(self.parameters)
        connection.add_on_connection_blocked_callback(self.on_connection_blocked)
        connection.add_on_connection_unblocked_callback(self.on_connection_unblocked)
        channel = connection.channel()
        self.connection = connection
        return channel

    def on_connection_blocked(self, connection, method):
        log.warning('rabbit connection blocked')

    def on_connection_unblocked(self, connection, method):
        log.info('rabbit connection unblocked')

    def close(self):
        if self.connection is not None:
            try:
                self.channel.close()
            except Exception:
                log.exception('failed to close rabbitmq channel')
            try:
                self.connection.close()
            except Exception:
                log.exception('failed to close rabbitmq connection')
            self.connection = None
            del self.__dict__['channel']

    def rotate(self):
        self.close()

    def on_status(self, status):
        self.channel.basic_publish(
            self.exchange,
            self.routing_key,
            json.dumps(status).encode('utf8'),
            properties=pika.BasicProperties(
                content_type='application/json',
            ),
        )
