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
    EXECUTION = 2
    WORKER = 3
    BOT = 4


class ServerError(BgdError):
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class ExecutionError(BgdError):
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.EXECUTION, reason=reason)


class WorkerError(BgdError):
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.WORKER, reason=reason)


class BotError(BgdError):
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.BOT, reason=reason)
