from datetime import datetime, timedelta
import json
import logging
import signal
import tweepy
import yaml

from .output_streams import output_stream_from_config

log = logging.getLogger(__name__)

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

    output_stream = output_stream_from_config(
        profile,
        output_path_prefix=args.output_path_prefix,
        rabbitmq_exchange=args.rabbitmq_exchange,
        rabbitmq_routing_key=args.rabbitmq_routing_key,
    )

    tweet_stream = TweetStream(
        profile['twitter']['consumer_key'],
        profile['twitter']['consumer_secret'],
        profile['twitter']['access_token'],
        profile['twitter']['access_token_secret'],
        output_stream=output_stream,
        report_interval=args.report_interval,
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
