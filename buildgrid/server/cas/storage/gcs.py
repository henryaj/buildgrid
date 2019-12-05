import io
import logging

from google.cloud import storage
from google.cloud.exceptions import NotFound

from .storage_abc import StorageABC


class GCSStorage(StorageABC):
    def __init__(self, bucket, **kwargs):
        self.__logger.info("Initialised GCS backend")
        self.__logger = logging.getLogger(__name__)

        self._client = storage.Client()
        self._bucket = self._client.get_bucket(bucket)

    def has_blob(self, digest):
        name = _name_from_digest(digest)
        blobs = list(self._bucket.list_blobs(prefix=name))
        if len(blobs) == 0:
            return False
        return True

    def get_blob(self, digest):
        self.__logger.debug("Getting blob: [{}]".format(digest))
        try:
            name = _name_from_digest(digest)
            blob = self._bucket.get_blob(name)
            return io.BytesIO(blob.download_as_string())
        except NotFound:
            return None

    def delete_blob(self, digest):
        self.__logger.debug("Deleting blob: [{}]".format(digest))
        try:
            name = _name_from_digest(digest)
            self._bucket.delete_blob(name)
        except NotFound:
            return None

    def begin_write(self, _digest):
        return io.BytesIO()

    def commit_write(self, digest, write_session):
        write_session.seek(0)
        name = _name_from_digest(digest)
        blob = self._bucket.blob(name)
        blob.upload_from_file(write_session)
        write_session.close()


def _name_from_digest(digest):
    return digest.hash + '_' + str(digest.size_bytes)
