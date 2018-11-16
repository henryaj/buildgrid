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

# pylint: disable=redefined-outer-name


import pytest

from buildgrid._exceptions import FailedPreconditionError
from buildgrid.bot.hardware.interface import HardwareInterface
from buildgrid.bot.hardware.device import Device
from buildgrid.bot.hardware.worker import Worker


CONFIGURATIONS_WORKER = [{'DockerImage': ['Blade']}, {'DockerImage': ['Sheep', 'Snake']}]
PROPERTIES_WORKER = [{'pool': ['Blade']}, {'pool': ['Sheep', 'Snake']}]
PROPERTIES_DEVICE = [{'os': ['Blade']}, {'has-docker': ['Sheep', 'Snake']}]


def test_create_hardware():
    worker = Worker()
    interface = HardwareInterface(worker)

    device0 = Device()
    worker.add_device(device0)
    protobuf_worker = interface.get_worker_pb2()
    assert len(protobuf_worker.devices) == 1

    worker.add_device(Device())
    protobuf_worker = interface.get_worker_pb2()
    assert len(protobuf_worker.devices) == 2

    assert protobuf_worker.devices[0].handle != protobuf_worker.devices[1].handle
    assert device0.name == protobuf_worker.devices[0].handle


@pytest.mark.parametrize('config', CONFIGURATIONS_WORKER)
def test_worker_config(config):
    worker = Worker(configs=config)
    protobuf_worker = worker.get_pb2()

    proto_cfg = {}
    for cfg in protobuf_worker.configs:
        k = cfg.key
        v = cfg.value

        proto_cfg_values = proto_cfg.get(k)
        if not proto_cfg_values:
            proto_cfg[k] = [v]
        else:
            proto_cfg_values.append(v)

    assert config == proto_cfg
    assert worker.configs == config


@pytest.mark.parametrize('prop', PROPERTIES_WORKER)
def test_worker_property(prop):
    worker = Worker(properties=prop)
    protobuf_worker = worker.get_pb2()

    proto_prop = {}
    for p in protobuf_worker.properties:
        k = p.key
        v = p.value

        proto_prop_values = proto_prop.get(k)
        if not proto_prop_values:
            proto_prop[k] = [v]
        else:
            proto_prop_values.append(v)

    assert prop == proto_prop
    assert worker.properties == prop


@pytest.mark.parametrize('prop', PROPERTIES_DEVICE)
def test_device_property(prop):
    device = Device(properties=prop)
    protobuf_device = device.get_pb2()

    proto_prop = {}
    for p in protobuf_device.properties:
        k = p.key
        v = p.value

        proto_prop_values = proto_prop.get(k)
        if not proto_prop_values:
            proto_prop[k] = [v]
        else:
            proto_prop_values.append(v)

    assert prop == proto_prop
    assert device.properties == prop


@pytest.mark.parametrize('config', [{'piano': ['Blade']}])
def test_worker_config_fail(config):
    with pytest.raises(KeyError):
        Worker(configs=config)


@pytest.mark.parametrize('prop', [{'piano': ['Blade']}])
def test_worker_property_fail(prop):
    with pytest.raises(KeyError):
        Worker(properties=prop)


@pytest.mark.parametrize('prop', [{'piano': ['Blade']}])
def test_device_property_fail(prop):
    with pytest.raises(KeyError):
        Device(properties=prop)


@pytest.mark.parametrize('requirements', CONFIGURATIONS_WORKER)
def test_configure_hardware(requirements):
    hardware_interface = HardwareInterface(Worker(configs=requirements))

    worker_requirements = Worker(configs=requirements)

    hardware_interface.configure_hardware(worker_requirements.get_pb2())


@pytest.mark.parametrize('requirements', CONFIGURATIONS_WORKER)
def test_configure_hardware_fail(requirements):
    hardware_interface = HardwareInterface(Worker())

    worker_requirements = Worker(configs=requirements)

    with pytest.raises(FailedPreconditionError):
        hardware_interface.configure_hardware(worker_requirements.get_pb2())
