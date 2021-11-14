from subparse import command

def generic_options(parser):
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('--profile', default='profile.yml')

@command('.stream', 'api:stream')
def stream(parser):
    """
    Listen to the twitter firehouse.

    A new file is created when the stream is interrupted due to an issue or
    when a SIGHUP is received locally. The resulting files can then be
    concatenated together and/or ingested into the database for querying.

    """
    parser.add_argument('filter_file')
    parser.add_argument(
        '--output-path-prefix',
        help=(
            'Store the data compressed to disk. '
            'Helpful for debugging or redundancy.'
        ),
    )
    parser.add_argument('--rabbitmq-exchange', default='')
    parser.add_argument('--rabbitmq-queue')

@command('.mq_watch', 'mq:watch')
def mq_watch(parser):
    """
    Listen for messages in the queue.

    """
    parser.add_argument(
        '--output-path-prefix',
        help=(
            'Store the data compressed to disk. '
            'Helpful for debugging or redundancy.'
        ),
    )
    parser.add_argument('--queue', required=True)
