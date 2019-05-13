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


import asyncio
import ctypes
from enum import Enum
import sys

from google.protobuf import json_format

from buildgrid._exceptions import InvalidArgumentError
from buildgrid._protos.buildgrid.v2 import monitoring_pb2


class MonitoringOutputType(Enum):
    # Standard output stream.
    STDOUT = 'stdout'
    # On-disk file.
    FILE = 'file'
    # UNIX domain socket.
    SOCKET = 'socket'


class MonitoringOutputFormat(Enum):
    # Protobuf binary format.
    BINARY = 'binary'
    # JSON format.
    JSON = 'json'
    # StatsD format. Only metrics are kept - logs are dropped.
    STATSD = 'statsd'


class MonitoringBus:

    def __init__(self, event_loop,
                 endpoint_type=MonitoringOutputType.SOCKET, endpoint_location=None,
                 serialisation_format=MonitoringOutputFormat.BINARY):
        self.__event_loop = event_loop
        self.__streaming_task = None

        self.__message_queue = asyncio.Queue(loop=self.__event_loop)
        self.__sequence_number = 1

        self.__output_location = None
        self.__async_output = False
        self.__json_output = False
        self.__statsd_output = False
        self.__print_output = False

        if endpoint_type == MonitoringOutputType.FILE:
            self.__output_location = endpoint_location

        elif endpoint_type == MonitoringOutputType.SOCKET:
            self.__output_location = endpoint_location
            self.__async_output = True

        elif endpoint_type == MonitoringOutputType.STDOUT:
            self.__print_output = True

        else:
            raise InvalidArgumentError("Invalid endpoint output type: [{}]"
                                       .format(endpoint_type))

        if serialisation_format == MonitoringOutputFormat.JSON:
            self.__json_output = True
        elif serialisation_format == MonitoringOutputFormat.STATSD:
            self.__statsd_output = True

    # --- Public API ---

    @property
    def prints_records(self):
        """Whether or not messages are printed to standard output."""
        return self.__print_output

    def start(self):
        """Starts the monitoring bus worker task."""
        if self.__streaming_task is not None:
            return

        self.__streaming_task = asyncio.ensure_future(
            self._streaming_worker(), loop=self.__event_loop)

    def stop(self):
        """Cancels the monitoring bus worker task."""
        if self.__streaming_task is None:
            return

        self.__streaming_task.cancel()

    async def send_record(self, record):
        """Publishes a record onto the bus asynchronously.

        Args:
            record (Message): The
        """
        await self.__message_queue.put(record)

    def send_record_nowait(self, record):
        """Publishes a record onto the bus.

        Args:
            record (Message): The
        """
        self.__message_queue.put_nowait(record)

    # --- Private API ---

    @staticmethod
    def _convert_metric_to_statsd(record):
        if record.type == monitoring_pb2.MetricRecord.COUNTER:
            if record.count is None:
                raise ValueError(
                    "COUNTER record {} is missing a count".format(record.name))
            return "{}:{}|c\n".format(record.name, record.count)
        elif record.type is monitoring_pb2.MetricRecord.TIMER:
            if record.duration is None:
                raise ValueError(
                    "TIMER record {} is missing a duration".format(record.name))
            return "{}:{}|ms\n".format(record.name, record.duration.ToMilliseconds())
        elif record.type is monitoring_pb2.MetricRecord.GAUGE:
            if record.value is None:
                raise ValueError(
                    "GAUGE record {} is missing a value".format(record.name))
            return "{}:{}|g\n".format(record.name, record.value)
        raise ValueError("Unknown record type.")

    async def _streaming_worker(self):
        """Handles bus messages streaming work."""
        async def __streaming_worker(end_points):
            record = await self.__message_queue.get()

            message = monitoring_pb2.BusMessage()
            message.sequence_number = self.__sequence_number

            if record.DESCRIPTOR is monitoring_pb2.LogRecord.DESCRIPTOR:
                message.log_record.CopyFrom(record)

            elif record.DESCRIPTOR is monitoring_pb2.MetricRecord.DESCRIPTOR:
                message.metric_record.CopyFrom(record)

            else:
                return False

            if self.__json_output:
                blob_message = json_format.MessageToJson(message).encode()

                for end_point in end_points:
                    end_point.write(blob_message)

            elif self.__statsd_output:
                if record.DESCRIPTOR is monitoring_pb2.MetricRecord.DESCRIPTOR:
                    statsd_message = MonitoringBus._convert_metric_to_statsd(
                        record)
                    for end_point in end_points:
                        end_point.write(statsd_message.encode())

            else:
                blob_size = ctypes.c_uint32(message.ByteSize())
                blob_message = message.SerializeToString()

                for end_point in end_points:
                    end_point.write(bytes(blob_size))
                    end_point.write(blob_message)

            return True

        output_writers, output_file = [], None

        async def __client_connected_callback(reader, writer):
            output_writers.append(writer)

        try:
            if self.__async_output and self.__output_location:
                await asyncio.start_unix_server(
                    __client_connected_callback, path=self.__output_location,
                    loop=self.__event_loop)

                while True:
                    if await __streaming_worker(output_writers):
                        self.__sequence_number += 1

                        for writer in output_writers:
                            await writer.drain()

            elif self.__output_location:
                output_file = open(self.__output_location, mode='wb')

                output_writers.append(output_file)

                while True:
                    if await __streaming_worker([output_file]):
                        self.__sequence_number += 1

                        output_file.flush()

            elif self.__print_output:
                output_writers.append(sys.stdout.buffer)

                while True:
                    if await __streaming_worker(output_writers):
                        self.__sequence_number += 1

        except asyncio.CancelledError:
            if output_file is not None:
                output_file.close()

            elif output_writers:
                for writer in output_writers:
                    writer.close()
                    await writer.wait_closed()
