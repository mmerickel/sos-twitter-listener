from datetime import datetime, timedelta
import json
import logging
import pika
import signal

from .output_streams import output_stream_from_config

log = logging.getLogger(__name__)

def main(cli, args):
    profile = cli.profile

    connection = pika.BlockingConnection(pika.URLParameters(profile['rabbitmq']['url']))
    channel = connection.channel()
    channel.queue_declare(args.queue)

    output_stream = output_stream_from_config(
        profile,
        output_path_prefix=args.output_path_prefix,
        gcp_firestore_collection=args.gcp_firestore_collection,
        gcp_image_bucket=args.gcp_image_bucket,
    )

    def on_sighup(*args):
        log.info('received SIGHUP, rotating')
        output_stream.rotate()

    def on_sigterm(*args):
        log.info('received SIGTERM, stopping')
        channel.stop_consuming()

    def report(now=None):
        nonlocal last_report_at, num_records_since_report

        if now is None:
            now = datetime.utcnow()
        dt = now - last_report_at

        log.info(
            f'received {num_records_since_report} records since '
            f'{dt.total_seconds():.2f} seconds ago'
        )
        last_report_at = now
        num_records_since_report = 0

    def on_message(channel, method_frame, header_frame, body):
        nonlocal num_records_since_report
        now = datetime.utcnow()
        num_records_since_report += 1

        try:
            status = json.loads(body.decode('utf8'))
            output_stream.on_status(status)
        except Exception:
            log.exception(f'failed to handle message={body}')

        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        if now - last_report_at >= args.report_interval:
            report(now=now)

    last_report_at = datetime.utcnow()
    num_records_since_report = 0

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
