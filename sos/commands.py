from datetime import timedelta
from subparse import command

from .settings import asduration

default_report_interval = timedelta(seconds=5)

def generic_options(parser):
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('--profile', default='profile.yml')

@command('.tweet_stream', 'twitter:stream')
def tweet_stream(parser):
    """
    Listen to the twitter firehouse.

    A new file is created when the stream is interrupted due to an issue or
    when a SIGHUP is received locally. The resulting files can then be
    concatenated together and/or ingested into the database for querying.

    """
    parser.add_argument('filter_file')
    parser.add_argument(
        '--report-interval',
        type=asduration,
        default=default_report_interval,
    )
    parser.add_argument(
        '--output-path-prefix',
        help=(
            'Store the data compressed to disk. '
            'Helpful for debugging or redundancy.'
        ),
    )
    parser.add_argument('--rabbitmq-exchange', default='')
    parser.add_argument('--rabbitmq-routing-key')

@command('.mq_archiver', 'mq:archive')
def mq_archiver(parser):
    """
    Listen for messages in the queue and save them.

    """
    parser.add_argument('--queue', required=True)
    parser.add_argument(
        '--report-interval',
        type=asduration,
        default=default_report_interval,
    )
    parser.add_argument(
        '--output-path-prefix',
        help=(
            'Store the data compressed to disk. '
            'Helpful for debugging or redundancy.'
        ),
    )
    parser.add_argument('--gcp-image-bucket')
    parser.add_argument('--gcp-firestore-collection')
