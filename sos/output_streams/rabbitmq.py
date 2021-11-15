from cached_property import cached_property
import json
import logging
import pika

log = logging.getLogger(__name__)

class RabbitMqOutputStream:
    def __init__(self, parameters, exchange, routing_key):
        self.parameters = parameters
        self.exchange = exchange or ''
        self.routing_key = routing_key
        self.connection = None

    @classmethod
    def from_config(cls, profile, exchange, routing_key):
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
        channel.add_on_close_callback(self.on_channel_closed)
        self.connection = connection
        return channel

    def on_connection_blocked(self, connection, method):
        log.warning('rabbit connection blocked')

    def on_connection_unblocked(self, connection, method):
        log.info('rabbit connection unblocked')

    def on_channel_closed(self, channel, e):
        log.error(f'channel closed, error={e}')
        self.rotate()

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

    def on_status(self, status, *, retry=True):
        try:
            self.channel.basic_publish(
                self.exchange,
                self.routing_key,
                json.dumps(status).encode('utf8'),
                properties=pika.BasicProperties(
                    content_type='application/json',
                ),
            )
        except Exception:
            if not retry:
                raise

            # try to do a quick retry
            self.rotate()
            self.on_status(status, retry=False)
