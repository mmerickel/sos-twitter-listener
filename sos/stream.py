from cached_property import cached_property
from datetime import datetime, timedelta
import json
import logging
import os
import pika
import signal
import tweepy
import yaml

from . import zstd
from .settings import asduration

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
            os.makedirs(root_path)
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
    def __init__(self, parameters, exchange, queue):
        self.parameters = parameters
        self.exchange = exchange
        self.queue = queue
        self.connection = None

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
            self.queue,
            json.dumps(status).encode('utf8'),
            properties=pika.BasicProperties(
                content_type='application/json',
            ),
        )

class TweetStream(tweepy.Stream):
    last_report_at = None
    num_records_since_report = 0
    report_interval = timedelta(seconds=1)

    def __init__(self, *args, output_stream, report_interval=None, **kw):
        super().__init__(*args, **kw)
        self.output_stream = output_stream
        if report_interval is not None:
            self.report_interval = report_interval

    def on_connect(self):
        super().on_connect()
        self.last_report_at = datetime.utcnow()
        self.num_records_since_report = 0

    def on_disconnect(self):
        super().on_disconnect()
        self.report()

    # explitly overriding tweepy.Stream.on_data here to avoid inefficiencies
    # in extra parsing of the tweets - just want to grab them and shoot them
    # into the output stream as quickly as possible without parsing into
    # a full Status object
    def on_data(self, raw_data):
        now = datetime.utcnow()
        self.num_records_since_report += 1

        data = json.loads(raw_data)

        if 'in_reply_to_status_id' in data:
            try:
                self.output_stream.on_status(data)
            except Exception:
                log.exception('received exception writing status to output stream')
        elif 'warning' in data:
            self.on_warning(data['warning'])
        elif 'limit' in data:
            self.on_limit(data['limit']['track'])
        elif 'disconnect' in data:
            self.on_disconnect_message(data['disconnect'])
        else:
            log.debug(f'ignoring unknown message={raw_data}')

        if now - self.last_report_at >= self.report_interval:
            self.report(now=now)

    def report(self, now=None):
        if now is None:
            now = datetime.utcnow()
        dt = now - self.last_report_at

        log.info(
            f'received {self.num_records_since_report} records since '
            f'{dt.total_seconds():.2f} seconds ago'
        )
        self.last_report_at = now
        self.num_records_since_report = 0

def main(cli, args):
    profile = cli.profile

    with open(args.filter_file, 'r', encoding='utf8') as fp:
        filters = yaml.safe_load(fp)

    stream_profile = profile.get('stream', {})
    report_interval = stream_profile.get('report_interval')
    if report_interval is not None:
        report_interval = asduration(report_interval)

    output_stream = CompositeOutputStream()
    if args.output_path_prefix:
        output_stream.add_stream(FileOutputStream(args.output_path_prefix))
    if args.rabbitmq_queue:
        output_stream.add_stream(RabbitMqOutputStream(
            pika.URLParameters(profile['rabbitmq']['url']),
            exchange=args.rabbitmq_exchange,
            queue=args.rabbitmq_queue,
        ))

    tweet_stream = TweetStream(
        profile['twitter']['consumer_key'],
        profile['twitter']['consumer_secret'],
        profile['twitter']['access_token'],
        profile['twitter']['access_token_secret'],
        output_stream=output_stream,
        report_interval=report_interval,
    )

    def on_sighup(*args):
        log.info('received SIGHUP, rotating')
        output_stream.rotate()

    def on_sigterm(*args):
        log.info('received SIGTERM, stopping')
        tweet_stream.disconnect()

    signal.signal(signal.SIGTERM, on_sigterm)
    signal.signal(signal.SIGHUP, on_sighup)
    try:
        tweet_stream.filter(**filters, stall_warnings=True)
    except KeyboardInterrupt:
        log.info('received SIGINT, stopping')
        tweet_stream.disconnect()

    signal.signal(signal.SIGHUP, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    output_stream.close()
