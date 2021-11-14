import json
import logging
import pika
import signal

from .stream import (
    CompositeOutputStream,
    FileOutputStream,
    StdoutOutputStream,
)

log = logging.getLogger(__name__)

def main(cli, args):
    profile = cli.profile
    rabbitmq_profile = profile.get('rabbitmq', {})
    rabbitmq_parameters = pika.URLParameters(rabbitmq_profile['url'])

    connection = pika.BlockingConnection(rabbitmq_parameters)
    channel = connection.channel()
    channel.queue_declare(args.queue)

    output_stream = CompositeOutputStream()
    if args.output_path_prefix:
        output_stream.add_stream(FileOutputStream(args.output_path_prefix))
    else:
        output_stream.add_stream(StdoutOutputStream())

    def on_sighup(*args):
        log.info('received SIGHUP, rotating')
        output_stream.rotate()

    def on_sigterm(*args):
        log.info('received SIGTERM, stopping')
        channel.stop_consuming()

    def on_message(channel, method_frame, header_frame, body):
        try:
            status = json.loads(body.decode('utf8'))
            output_stream.on_status(status)
        except Exception:
            log.exception(f'failed to handle message={body}')
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    channel.basic_consume(args.queue, on_message)
    signal.signal(signal.SIGTERM, on_sigterm)
    signal.signal(signal.SIGHUP, on_sighup)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info('received SIGINT, stopping')
        channel.stop_consuming()
    signal.signal(signal.SIGHUP, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    output_stream.close()
    connection.close()
