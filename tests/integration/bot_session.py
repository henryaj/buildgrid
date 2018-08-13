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

import grpc
import pytest
import uuid

from unittest import mock

from buildgrid.bot import bot_session, bot_interface


@pytest.mark.parametrize("docker_value", ["True", "False"])
@pytest.mark.parametrize("os_value", ["nexus7", "nexus8"])
def test_create_device(docker_value, os_value):
    properties = {'docker': docker_value, 'os': os_value}
    device = bot_session.Device(properties)

    assert uuid.UUID(device.name, version=4)
    assert properties == device.properties


def test_create_device_key_fail():
    properties = {'voight': 'kampff'}

    with pytest.raises(KeyError):
        device = bot_session.Device(properties)


def test_create_device_value_fail():
    properties = {'docker': True}

    with pytest.raises(ValueError):
        device = bot_session.Device(properties)


def test_create_worker():
    properties = {'pool': 'swim'}
    configs = {'DockerImage': 'Windows'}
    worker = bot_session.Worker(properties, configs)

    assert properties == worker.properties
    assert configs == worker.configs

    device = bot_session.Device()
    worker.add_device(device)

    assert worker._devices[0] == device


def test_create_worker_key_fail():
    properties = {'voight': 'kampff'}
    configs = {'voight': 'kampff'}

    with pytest.raises(KeyError):
        bot_session.Worker(properties)
    with pytest.raises(KeyError):
        bot_session.Worker(configs)
