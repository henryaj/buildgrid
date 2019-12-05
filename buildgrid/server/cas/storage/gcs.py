import io
import logging

from google.cloud import storage
from google.cloud.exceptions import NotFound

from .storage_abc import StorageABC


class GCSStorage(StorageABC):
    def __init__(self, bucket, **kwargs):
        self.__logger = logging.getLogger(__name__)
        self._client = storage.Client()
        self._bucket = self._client.get_bucket(bucket)

        self.__logger.info("Initialised GCS backend")

    def has_blob(self, digest):
        self.__logger.debug("Checking for blob: [{}]".format(digest))
        name = _name_from_digest(digest)
        blobs = list(self._bucket.list_blobs(prefix=name))
        if len(blobs) == 0:
            return False
        self.__logger.debug("Found blob {}".format(digest))
        return True

    def get_blob(self, digest):
        self.__logger.debug("Getting blob: [{}]".format(digest))
        try:
            name = _name_from_digest(digest)
            blob = self._bucket.get_blob(name)
            self.__logger.debug("Got blob {}".format(digest))
            return io.BytesIO(blob.download_as_string())
        except NotFound:
            return None

    def delete_blob(self, digest):
        self.__logger.debug("Deleting blob: [{}]".format(digest))
        try:
            name = _name_from_digest(digest)
            self._bucket.delete_blob(name)
            self.__logger.debug("Deleted blob {}".format(digest))
        except NotFound:
            return None

    def begin_write(self, _digest):
        return io.BytesIO()

    def commit_write(self, digest, write_session):
        self.__logger.debug("Writing blob: [{}]".format(digest))
        write_session.seek(0)
        name = _name_from_digest(digest)
        blob = self._bucket.blob(name)
        blob.upload_from_file(write_session)
        write_session.close()
        self.__logger.debug("Wrote blob {}".format(digest))


def _name_from_digest(digest):
    return digest.hash + '_' + str(digest.size_bytes)
