from ..._exceptions import BgdError, ErrorDomain

""" A bad argument was passed, such as a name which doesn't exist
"""
class InvalidArgumentError(BgdError):
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER_EXECUTION, reason=reason)
