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


class OutOfRangeError(BgdError):
    """ ByteStream service read data out of range
    """

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)
