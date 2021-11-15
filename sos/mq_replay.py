import io
import json
import logging
import zstandard as zstd

from .output_streams import output_stream_from_config

log = logging.getLogger(__name__)

def parse_tweet_stream(path):
    with open(path, 'rb') as fp:
        dctx = zstd.ZstdDecompressor()
        stream_reader = dctx.stream_reader(fp)
        stream = io.TextIOWrapper(stream_reader, encoding='utf8')
        for line in stream:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except Exception as ex:
                    log.exception(ex)

def main(cli, args):
    profile = cli.profile

    output_stream = output_stream_from_config(
        profile,
        rabbitmq_exchange=args.exchange,
        rabbitmq_routing_key=args.routing_key,
    )

    for file in args.files:
        tweets = parse_tweet_stream(file)
        for tweet in tweets:
            output_stream.on_status(tweet)
