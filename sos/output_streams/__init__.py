from .composite import CompositeOutputStream
from .file import FileOutputStream
from .gcp_firestore import GCPFirestoreOutputStream
from .gcp_image_storage import GCPImageStorageOutputStream
from .rabbitmq import RabbitMqOutputStream
from .stdout import StdoutOutputStream

def output_stream_from_config(
    profile,
    *,
    rabbitmq_exchange=None,
    rabbitmq_routing_key=None,
    output_path_prefix=None,
    gcp_firestore_collection=None,
    gcp_image_bucket=None,
):
    streams = []

    if output_path_prefix:
        streams.append(FileOutputStream(output_path_prefix))

    if rabbitmq_routing_key:
        streams.append(RabbitMqOutputStream.from_config(
            profile['rabbitmq'],
            rabbitmq_exchange,
            rabbitmq_routing_key,
        ))

    if gcp_firestore_collection:
        streams.append(GCPFirestoreOutputStream.from_config(
            profile,
            gcp_firestore_collection,
        ))

    if gcp_image_bucket:
        streams.append(GCPImageStorageOutputStream.from_config(
            profile,
            gcp_image_bucket,
        ))

    if len(streams) > 1:
        return CompositeOutputStream(streams)
    if len(streams) > 0:
        return streams[0]
    return StdoutOutputStream()
