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


import random
import time

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2


def work_dummy(context, lease):
    """ Just returns lease after some random time
    """
    lease.result.Clear()

    time.sleep(random.randint(1, 5))

    action_result = remote_execution_pb2.ActionResult()

    lease.result.Pack(action_result)

    return lease
