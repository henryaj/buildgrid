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
from concurrent import futures
from datetime import datetime, timedelta
import logging
import logging.handlers
import os
import signal
import sys
import time

import grpc
import janus

from buildgrid._enums import BotStatus, LogRecordLevel, MetricRecordDomain, MetricRecordType
from buildgrid._protos.buildgrid.v2 import monitoring_pb2
from buildgrid.server.actioncache.service import ActionCacheService
from buildgrid.server._authentication import AuthMetadataMethod, AuthMetadataAlgorithm, AuthMetadataServerInterceptor
from buildgrid.server.bots.service import BotsService
from buildgrid.server.capabilities.instance import CapabilitiesInstance
from buildgrid.server.capabilities.service import CapabilitiesService
from buildgrid.server.cas.service import ByteStreamService, ContentAddressableStorageService
from buildgrid.server.execution.service import ExecutionService
from buildgrid.server._monitoring import MonitoringBus, MonitoringOutputType, MonitoringOutputFormat
from buildgrid.server.operations.service import OperationsService
from buildgrid.server.referencestorage.service import ReferenceStorageService
from buildgrid.settings import LOG_RECORD_FORMAT, MONITORING_PERIOD


class BuildGridServer:
    """Creates a BuildGrid server.

    The :class:`BuildGridServer` class binds together all the
    requisite services.
    """

    def __init__(self,
                 max_workers=None, monitor=False,
                 mon_endpoint_type=MonitoringOutputType.STDOUT,
                 mon_endpoint_location=None,
                 mon_serialisation_format=MonitoringOutputFormat.JSON,
                 auth_method=AuthMetadataMethod.NONE,
                 auth_secret=None,
                 auth_algorithm=AuthMetadataAlgorithm.UNSPECIFIED):
        """Initializes a new :class:`BuildGridServer` instance.

        Args:
            max_workers (int, optional): A pool of max worker threads.
            monitor (bool, optional): Whether or not to globally activate server
                monitoring. Defaults to ``False``.
            auth_method (AuthMetadataMethod, optional): Authentication method to
                be used for request authorization. Defaults to ``NONE``.
            auth_secret (str, optional): The secret or key to be used for
                authorizing request using `auth_method`. Defaults to ``None``.
            auth_algorithm (AuthMetadataAlgorithm, optional): The crytographic
                algorithm to be uses in combination with `auth_secret` for
                authorizing request using `auth_method`. Defaults to
                ``UNSPECIFIED``.
        """
        self.__logger = logging.getLogger(__name__)

        if max_workers is None:
            # Use max_workers default from Python 3.5+
            max_workers = (os.cpu_count() or 1) * 5

        self.__grpc_auth_interceptor = None
        if auth_method != AuthMetadataMethod.NONE:
            self.__grpc_auth_interceptor = AuthMetadataServerInterceptor(
                method=auth_method, secret=auth_secret, algorithm=auth_algorithm)
        self.__grpc_executor = futures.ThreadPoolExecutor(max_workers)

        if self.__grpc_auth_interceptor is not None:
            self.__grpc_server = grpc.server(
                self.__grpc_executor, interceptors=(self.__grpc_auth_interceptor,))
        else:
            self.__grpc_server = grpc.server(self.__grpc_executor)

        self.__main_loop = asyncio.get_event_loop()

        self.__monitoring_bus = None

        self.__logging_queue = janus.Queue(loop=self.__main_loop)
        self.__logging_handler = logging.handlers.QueueHandler(self.__logging_queue.sync_q)
        self.__logging_formatter = logging.Formatter(fmt=LOG_RECORD_FORMAT)
        self.__print_log_records = True

        self.__build_metadata_queues = None

        self.__state_monitoring_task = None
        self.__build_monitoring_tasks = None
        self.__logging_task = None

        # We always want a capabilities service
        self._capabilities_service = CapabilitiesService(self.__grpc_server)

        self._execution_service = None
        self._bots_service = None
        self._operations_service = None
        self._reference_storage_service = None
        self._action_cache_service = None
        self._cas_service = None
        self._bytestream_service = None

        self._schedulers = {}
        self._instances = set()

        self._is_instrumented = monitor

        if self._is_instrumented:
            self.__monitoring_bus = MonitoringBus(
                self.__main_loop, endpoint_type=mon_endpoint_type,
                endpoint_location=mon_endpoint_location,
                serialisation_format=mon_serialisation_format)

            self.__build_monitoring_tasks = []

        # Setup the main logging handler:
        root_logger = logging.getLogger()

        for log_filter in root_logger.filters[:]:
            self.__logging_handler.addFilter(log_filter)
            root_logger.removeFilter(log_filter)

        for log_handler in root_logger.handlers[:]:
            root_logger.removeHandler(log_handler)
        root_logger.addHandler(self.__logging_handler)

        if self._is_instrumented and self.__monitoring_bus.prints_records:
            self.__print_log_records = False

    # --- Public API ---

    def start(self):
        """Starts the BuildGrid server."""
        self.__grpc_server.start()

        if self._is_instrumented:
            self.__monitoring_bus.start()

            self.__state_monitoring_task = asyncio.ensure_future(
                self._state_monitoring_worker(period=MONITORING_PERIOD),
                loop=self.__main_loop)

            self.__build_monitoring_tasks.clear()
            for instance_name, scheduler in self._schedulers.items():
                if not scheduler.is_instrumented:
                    continue

                message_queue = janus.Queue(loop=self.__main_loop)
                scheduler.register_build_metadata_watcher(message_queue.sync_q)

                self.__build_monitoring_tasks.append(asyncio.ensure_future(
                    self._build_monitoring_worker(instance_name, message_queue),
                    loop=self.__main_loop))

        self.__logging_task = asyncio.ensure_future(
            self._logging_worker(), loop=self.__main_loop)

        self.__main_loop.add_signal_handler(signal.SIGTERM, self.stop)

        self.__main_loop.run_forever()

    def stop(self):
        """Stops the BuildGrid server."""
        if self._is_instrumented:
            if self.__state_monitoring_task is not None:
                self.__state_monitoring_task.cancel()

            for build_monitoring_task in self.__build_monitoring_tasks:
                build_monitoring_task.cancel()
            self.__build_monitoring_tasks.clear()

            self.__monitoring_bus.stop()

        if self.__logging_task is not None:
            self.__logging_task.cancel()

        self.__main_loop.stop()

        self.__grpc_server.stop(None)

    def add_port(self, address, credentials):
        """Adds a port to the server.

        Must be called before the server starts. If a credentials object exists,
        it will make a secure port.

        Args:
            address (str): The address with port number.
            credentials (:obj:`grpc.ChannelCredentials`): Credentials object.

        Returns:
            int: Number of the bound port.
        """
        if credentials is not None:
            self.__logger.info("Adding secure connection on: [%s]", address)
            port_number = self.__grpc_server.add_secure_port(address, credentials)

        else:
            self.__logger.info("Adding insecure connection on [%s]", address)
            port_number = self.__grpc_server.add_insecure_port(address)

        return port_number

    def add_execution_instance(self, instance, instance_name):
        """Adds an :obj:`ExecutionInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ExecutionInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._execution_service is None:
            self._execution_service = ExecutionService(
                self.__grpc_server, monitor=self._is_instrumented)

        self._execution_service.add_instance(instance_name, instance)
        self._add_capabilities_instance(instance_name, execution_instance=instance)

        self._schedulers[instance_name] = instance.scheduler
        self._instances.add(instance_name)

        if self._is_instrumented:
            instance.scheduler.activate_monitoring()

    def add_bots_interface(self, instance, instance_name):
        """Adds a :obj:`BotsInterface` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`BotsInterface`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._bots_service is None:
            self._bots_service = BotsService(
                self.__grpc_server, monitor=self._is_instrumented)

        self._bots_service.add_instance(instance_name, instance)

        self._instances.add(instance_name)

    def add_operations_instance(self, instance, instance_name):
        """Adds an :obj:`OperationsInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`OperationsInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._operations_service is None:
            self._operations_service = OperationsService(self.__grpc_server)

        self._operations_service.add_instance(instance_name, instance)

    def add_reference_storage_instance(self, instance, instance_name):
        """Adds a :obj:`ReferenceCache` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ReferenceCache`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._reference_storage_service is None:
            self._reference_storage_service = ReferenceStorageService(self.__grpc_server)

        self._reference_storage_service.add_instance(instance_name, instance)

    def add_action_cache_instance(self, instance, instance_name):
        """Adds a :obj:`ReferenceCache` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ReferenceCache`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._action_cache_service is None:
            self._action_cache_service = ActionCacheService(self.__grpc_server)

        self._action_cache_service.add_instance(instance_name, instance)
        self._add_capabilities_instance(instance_name, action_cache_instance=instance)

    def add_cas_instance(self, instance, instance_name):
        """Adds a :obj:`ContentAddressableStorageInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ReferenceCache`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._cas_service is None:
            self._cas_service = ContentAddressableStorageService(self.__grpc_server)

        self._cas_service.add_instance(instance_name, instance)
        self._add_capabilities_instance(instance_name, cas_instance=instance)

    def add_bytestream_instance(self, instance, instance_name):
        """Adds a :obj:`ByteStreamInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ByteStreamInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._bytestream_service is None:
            self._bytestream_service = ByteStreamService(self.__grpc_server)

        self._bytestream_service.add_instance(instance_name, instance)

    # --- Public API: Monitoring ---

    @property
    def is_instrumented(self):
        return self._is_instrumented

    # --- Private API ---

    def _add_capabilities_instance(self, instance_name,
                                   cas_instance=None,
                                   action_cache_instance=None,
                                   execution_instance=None):
        """Adds a :obj:`CapabilitiesInstance` to the service.

        Args:
            instance (:obj:`CapabilitiesInstance`): Instance to add.
            instance_name (str): Instance name.
        """

        try:
            if cas_instance:
                self._capabilities_service.add_cas_instance(instance_name, cas_instance)
            if action_cache_instance:
                self._capabilities_service.add_action_cache_instance(instance_name, action_cache_instance)
            if execution_instance:
                self._capabilities_service.add_execution_instance(instance_name, execution_instance)

        except KeyError:
            capabilities_instance = CapabilitiesInstance(cas_instance,
                                                         action_cache_instance,
                                                         execution_instance)
            self._capabilities_service.add_instance(instance_name, capabilities_instance)

    async def _logging_worker(self):
        """Publishes log records to the monitoring bus."""
        async def __logging_worker():
            log_record = await self.__logging_queue.async_q.get()

            # Print log records to stdout, if required:
            if self.__print_log_records:
                record = self.__logging_formatter.format(log_record)

                # TODO: Investigate if async write would be worth here.
                sys.stdout.write('{}\n'.format(record))
                sys.stdout.flush()

            # Emit a log record if server is instrumented:
            if self._is_instrumented:
                log_record_level = LogRecordLevel(int(log_record.levelno / 10))
                log_record_creation_time = datetime.fromtimestamp(log_record.created)
                # logging.LogRecord.extra must be a str to str dict:
                if 'extra' in log_record.__dict__ and log_record.extra:
                    log_record_metadata = log_record.extra
                else:
                    log_record_metadata = None
                record = self._forge_log_record(
                    log_record.name, log_record_level, log_record.message,
                    log_record_creation_time, metadata=log_record_metadata)

                await self.__monitoring_bus.send_record(record)

        try:
            while True:
                await __logging_worker()

        except asyncio.CancelledError:
            pass

    def _forge_log_record(self, domain, level, message, creation_time, metadata=None):
        log_record = monitoring_pb2.LogRecord()

        log_record.creation_timestamp.FromDatetime(creation_time)
        log_record.domain = domain
        log_record.level = level.value
        log_record.message = message
        if metadata is not None:
            log_record.metadata.update(metadata)

        return log_record

    async def _build_monitoring_worker(self, instance_name, message_queue):
        """Publishes builds metadata to the monitoring bus."""
        async def __build_monitoring_worker():
            metadata, context = await message_queue.async_q.get()

            context.update({'instance-name': instance_name or ''})

            # Emit build inputs fetching time record:
            fetch_start = metadata.input_fetch_start_timestamp.ToDatetime()
            fetch_completed = metadata.input_fetch_completed_timestamp.ToDatetime()
            input_fetch_time = fetch_completed - fetch_start
            timer_record = self._forge_timer_metric_record(
                MetricRecordDomain.BUILD, 'inputs-fetching-time', input_fetch_time,
                metadata=context)

            await self.__monitoring_bus.send_record(timer_record)

            # Emit build execution time record:
            execution_start = metadata.execution_start_timestamp.ToDatetime()
            execution_completed = metadata.execution_completed_timestamp.ToDatetime()
            execution_time = execution_completed - execution_start
            timer_record = self._forge_timer_metric_record(
                MetricRecordDomain.BUILD, 'execution-time', execution_time,
                metadata=context)

            await self.__monitoring_bus.send_record(timer_record)

            # Emit build outputs uploading time record:
            upload_start = metadata.output_upload_start_timestamp.ToDatetime()
            upload_completed = metadata.output_upload_completed_timestamp.ToDatetime()
            output_upload_time = upload_completed - upload_start
            timer_record = self._forge_timer_metric_record(
                MetricRecordDomain.BUILD, 'outputs-uploading-time', output_upload_time,
                metadata=context)

            await self.__monitoring_bus.send_record(timer_record)

            # Emit total build handling time record:
            queued = metadata.queued_timestamp.ToDatetime()
            worker_completed = metadata.worker_completed_timestamp.ToDatetime()
            total_handling_time = worker_completed - queued
            timer_record = self._forge_timer_metric_record(
                MetricRecordDomain.BUILD, 'total-handling-time', total_handling_time,
                metadata=context)

            await self.__monitoring_bus.send_record(timer_record)

        try:
            while True:
                await __build_monitoring_worker()

        except asyncio.CancelledError:
            pass

    async def _state_monitoring_worker(self, period=1.0):
        """Periodically publishes state metrics to the monitoring bus."""
        async def __state_monitoring_worker():
            # Emit total clients count record:
            _, record = self._query_n_clients()
            await self.__monitoring_bus.send_record(record)

            # Emit total bots count record:
            _, record = self._query_n_bots()
            await self.__monitoring_bus.send_record(record)

            queue_times = []
            # Emits records by instance:
            for instance_name in self._instances:
                # Emit instance clients count record:
                _, record = self._query_n_clients_for_instance(instance_name)
                await self.__monitoring_bus.send_record(record)

                # Emit instance bots count record:
                _, record = self._query_n_bots_for_instance(instance_name)
                await self.__monitoring_bus.send_record(record)

                # Emit instance average queue time record:
                queue_time, record = self._query_am_queue_time_for_instance(instance_name)
                await self.__monitoring_bus.send_record(record)
                if queue_time:
                    queue_times.append(queue_time)

            # Emits records by bot status:
            for bot_status in [BotStatus.OK, BotStatus.UNHEALTHY]:
                # Emit status bots count record:
                _, record = self._query_n_bots_for_status(bot_status)
                await self.__monitoring_bus.send_record(record)

            # Emit overall average queue time record:
            if queue_times:
                am_queue_time = sum(queue_times, timedelta()) / len(queue_times)
            else:
                am_queue_time = timedelta()
            record = self._forge_timer_metric_record(
                MetricRecordDomain.STATE,
                'average-queue-time',
                am_queue_time)

            await self.__monitoring_bus.send_record(record)

        try:
            while True:
                start = time.time()
                await __state_monitoring_worker()

                end = time.time()
                await asyncio.sleep(period - (end - start))

        except asyncio.CancelledError:
            pass

    def _forge_counter_metric_record(self, domain, name, count, metadata=None):
        counter_record = monitoring_pb2.MetricRecord()

        counter_record.creation_timestamp.GetCurrentTime()
        counter_record.domain = domain.value
        counter_record.type = MetricRecordType.COUNTER.value
        counter_record.name = name
        counter_record.count = count
        if metadata is not None:
            counter_record.metadata.update(metadata)

        return counter_record

    def _forge_timer_metric_record(self, domain, name, duration, metadata=None):
        timer_record = monitoring_pb2.MetricRecord()

        timer_record.creation_timestamp.GetCurrentTime()
        timer_record.domain = domain.value
        timer_record.type = MetricRecordType.TIMER.value
        timer_record.name = name
        timer_record.duration.FromTimedelta(duration)
        if metadata is not None:
            timer_record.metadata.update(metadata)

        return timer_record

    def _forge_gauge_metric_record(self, domain, name, value, metadata=None):
        gauge_record = monitoring_pb2.MetricRecord()

        gauge_record.creation_timestamp.GetCurrentTime()
        gauge_record.domain = domain.value
        gauge_record.type = MetricRecordType.GAUGE.value
        gauge_record.name = name
        gauge_record.value = value
        if metadata is not None:
            gauge_record.metadata.update(metadata)

        return gauge_record

    # --- Private API: Monitoring ---

    def _query_n_clients(self):
        """Queries the number of clients connected."""
        n_clients = self._execution_service.query_n_clients()
        gauge_record = self._forge_gauge_metric_record(
            MetricRecordDomain.STATE, 'clients-count', n_clients)

        return n_clients, gauge_record

    def _query_n_clients_for_instance(self, instance_name):
        """Queries the number of clients connected for a given instance"""
        n_clients = self._execution_service.query_n_clients_for_instance(instance_name)
        gauge_record = self._forge_gauge_metric_record(
            MetricRecordDomain.STATE, 'clients-count', n_clients,
            metadata={'instance-name': instance_name or ''})

        return n_clients, gauge_record

    def _query_n_bots(self):
        """Queries the number of bots connected."""
        n_bots = self._bots_service.query_n_bots()
        gauge_record = self._forge_gauge_metric_record(
            MetricRecordDomain.STATE, 'bots-count', n_bots)

        return n_bots, gauge_record

    def _query_n_bots_for_instance(self, instance_name):
        """Queries the number of bots connected for a given instance."""
        n_bots = self._bots_service.query_n_bots_for_instance(instance_name)
        gauge_record = self._forge_gauge_metric_record(
            MetricRecordDomain.STATE, 'bots-count', n_bots,
            metadata={'instance-name': instance_name or ''})

        return n_bots, gauge_record

    def _query_n_bots_for_status(self, bot_status):
        """Queries the number of bots connected for a given health status."""
        n_bots = self._bots_service.query_n_bots_for_status(bot_status)
        gauge_record = self._forge_gauge_metric_record(
            MetricRecordDomain.STATE, 'bots-count', n_bots,
            metadata={'bot-status': bot_status.name})

        return n_bots, gauge_record

    def _query_am_queue_time_for_instance(self, instance_name):
        """Queries the average job's queue time for a given instance."""
        am_queue_time = self._schedulers[instance_name].query_am_queue_time()
        timer_record = self._forge_timer_metric_record(
            MetricRecordDomain.STATE, 'average-queue-time', am_queue_time,
            metadata={'instance-name': instance_name or ''})

        return am_queue_time, timer_record
