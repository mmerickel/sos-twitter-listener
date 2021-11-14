from cached_property import cached_property
from datetime import datetime
import json
import logging
import os

from .. import zstd

log = logging.getLogger(__name__)

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
            os.makedirs(root_path, exist_ok=True)
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
        write = self.writer.write
        write(json.dumps(status).encode('utf8'))
        write(b'\n')
