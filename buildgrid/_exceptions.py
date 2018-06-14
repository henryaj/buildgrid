# Copyright (C) 2018 Codethink Limited
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
#
# Authors:
#        Finn Ball <finn.ball@codethink.co.uk>

"""
Exceptions
===========
"""

from enum import Enum

""" Base BuildGrid Error class for internal exceptions.
"""
class BgdError(Exception):
    def __init__(self, message, *, detail=None, domain=None, reason=None):
        super().__init__(message)

        """ Any additional detail and extra information
        """
        self.detail = detail

        """ Domand and reason.
        """
        self.domain = domain
        self.reason = reason

class ErrorDomain(Enum):
    SERVER_EXECUTION = 1
    SERVER_WORKER    = 2
    WORKER           = 3
    APP              = 4

class ServerError(BgdError):
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)
    
class AppError(BgdError):
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.APP, reason=reason)