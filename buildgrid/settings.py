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


import hashlib


# Latest REAPI version supported:
HIGH_REAPI_VERSION = '2.0.0'

# Earliest non-deprecated REAPI version supported:
LOW_REAPI_VERSION = '2.0.0'

# Hash function used for computing digests:
HASH = hashlib.sha256

# Lenght in bytes of a hash string returned by HASH:
HASH_LENGTH = HASH().digest_size * 2

# Period, in seconds, for the monitoring cycle:
MONITORING_PERIOD = 5.0

# Maximum size for a single gRPC request:
MAX_REQUEST_SIZE = 2 * 1024 * 1024

# Maximum number of elements per gRPC request:
MAX_REQUEST_COUNT = 500

# String format for log records:
LOG_RECORD_FORMAT = '%(asctime)s:[%(name)36.36s][%(levelname)5.5s]: %(message)s'
# The different log record attributes are documented here:
# https://docs.python.org/3/library/logging.html#logrecord-attributes

# URL scheme for the CAS content browser:
BROWSER_URL_FORMAT = '%(type)s/%(instance)s/%(hash)s/%(sizebytes)s/'
# The string markers that are substituted are:
#  instance   - CAS instance's name.
#  type       - Type of CAS object, eg. 'action_result', 'command'...
#  hash       - Object's digest hash.
#  sizebytes  - Object's digest size in bytes.
