from .._exceptions import BgdError, ErrorDomain


class InvalidArgumentError(BgdError):
    """A bad argument was passed, such as a name which doesn't exist.
    """

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class NotFoundError(BgdError):
    """Requested resource not found.
    """

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class OutofSyncError(BgdError):
    """The worker is out of sync with the server, such as having a differing number of leases.
    """

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)
