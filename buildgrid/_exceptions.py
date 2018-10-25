# Copyright (C) 2018 Bloomberg LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  <http://www.apache.org/licenses/LICENSE-2.0>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Exceptions
===========
"""

from enum import Enum


class BgdError(Exception):
    """
    Base BuildGrid Error class for internal exceptions.
    """

    def __init__(self, message, *, detail=None, domain=None, reason=None):
        super().__init__(message)

        # Additional detail and extra information
        self.detail = detail

        # Domand and reason
        self.domain = domain
        self.reason = reason


class ErrorDomain(Enum):
    SERVER = 1
    BOT = 2


class ServerError(BgdError):
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class BotError(BgdError):
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.BOT, reason=reason)


class CancelledError(BgdError):
    """The job was cancelled and any callers should be notified"""
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class InvalidArgumentError(BgdError):
    """A bad argument was passed, such as a name which doesn't exist."""
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class NotFoundError(BgdError):
    """Requested resource not found."""
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class OutOfSyncError(BgdError):
    """The worker is out of sync with the server, such as having a differing
    number of leases."""
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class OutOfRangeError(BgdError):
    """ByteStream service read data out of range."""
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class FailedPreconditionError(BgdError):
    """One or more errors occurred in setting up the action requested, such as
    a missing input or command or no worker being available. The client may be
    able to fix the errors and retry."""
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)
