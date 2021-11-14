from google.cloud import storage
import httpx
import posixpath

from ..media import media_info_from_status

class GCPImageStorageOutputStream:
    def __init__(self, *, bucket, prefix, client=None):
        if client is None:
            client = storage.Client()
        self.bucket = client.bucket(bucket)
        self.prefix = prefix

    @classmethod
    def from_config(cls, profile, bucket):
        parts = bucket.split(':', 1)
        prefix = ''
        if len(parts) > 1:
            bucket, prefix = parts

        return cls(bucket=bucket, prefix=prefix)

    def close(self):
        pass

    def rotate(self):
        pass

    def on_status(self, status):
        for info in media_info_from_status(status):
            r = httpx.get(info['url'])
            data = r.content
            content_type = info['content_type']
            blob = self.bucket.blob(posixpath.join(self.prefix, info['name']))
            blob.upload_from_string(
                data,
                content_type=content_type,
            )
