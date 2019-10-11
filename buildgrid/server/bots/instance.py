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
BotsInterface
=================

Instance of the Remote Workers interface.
"""
from datetime import datetime, timedelta
from collections import OrderedDict
import asyncio
import logging
import math
import uuid

from buildgrid._exceptions import InvalidArgumentError, NotFoundError
from buildgrid.settings import NETWORK_TIMEOUT

from ..job import LeaseState, BotStatus


class BotsInterface:

    def __init__(self, scheduler, *, bot_session_keepalive_timeout=None):
        self.__logger = logging.getLogger(__name__)
        # Turn on debug mode based on log verbosity level:
        self.__debug = self.__logger.getEffectiveLevel() <= logging.DEBUG

        self._scheduler = scheduler
        self._instance_name = None

        self._bot_ids = {}
        self._assigned_leases = {}

        self._bot_session_keepalive_timeout = bot_session_keepalive_timeout
        self._setup_bot_session_reaper_loop()

        # Ordered mapping of bot_session_name: string -> last_expire_time_we_assigned: datetime
        # This works because the bot_session_keepalive_timeout is the same for all bots
        # and thus always increases with time (e.g. inserting at the end keeps them sorted because
        # of this property, otherwise we may have had to insert 'in the middle')
        self._ordered_expire_times_by_botsession = OrderedDict()
        # The "minimum" expire_time we have coming up
        self._next_expire_time = None
        # The Event to set when we learn about a new expire time that is closer in the
        # future than what we knew (e.g. whenever we reset the value of self._next_expire_time)
        self._deadline_event = asyncio.Event()

        # Remembering the last n evicted_bot_sessions so that we can present the appropriate
        # messages if they ever get back
        self._remember_last_n_evicted_bot_sessions = 1000
        # maps bot_session_name: string to (eviction_time: datetime, reason: string)
        self._evicted_bot_sessions = OrderedDict()

    # --- Public API ---

    @property
    def instance_name(self):
        return self._instance_name

    @property
    def scheduler(self):
        return self._scheduler

    def register_instance_with_server(self, instance_name, server):
        """Names and registers the bots interface with a given server."""
        if self._instance_name is None:
            server.add_bots_interface(self, instance_name)

            self._instance_name = instance_name

        else:
            raise AssertionError("Instance already registered")

    def create_bot_session(self, parent, bot_session):
        """ Creates a new bot session. Server should assign a unique
        name to the session. If a bot with the same bot id tries to
        register with the service, the old one should be closed along
        with all its jobs.
        """
        if not bot_session.bot_id:
            raise InvalidArgumentError("Bot's id must be set by client.")

        try:
            self._check_bot_ids(bot_session.bot_id)
        except InvalidArgumentError:
            pass

        # Bot session name, selected by the server
        name = "{}/{}".format(parent, str(uuid.uuid4()))
        bot_session.name = name

        self._bot_ids[name] = bot_session.bot_id

        # We want to keep a copy of lease ids we have assigned
        self._assigned_leases[name] = set()

        self._request_leases(bot_session, name=name)
        self._assign_deadline_for_botsession(bot_session, name)

        if self.__debug:
            self.__logger.info("Opened session bot_name=[%s] for bot_id=[%s], leases=[%s]",
                               bot_session.name, bot_session.bot_id,
                               ",".join([lease.id[:8] for lease in bot_session.leases]))
        else:
            self.__logger.info("Opened session, bot_name=[%s] for bot_id=[%s]",
                               bot_session.name, bot_session.bot_id)

        return bot_session

    def update_bot_session(self, name, bot_session, deadline=None):
        """ Client updates the server. Any changes in state to the Lease should be
        registered server side. Assigns available leases with work.
        """
        self._check_bot_ids(bot_session.bot_id, name)
        self._check_assigned_leases(bot_session)

        # Stop tracking the prior deadline since we have heard back
        # by the deadline we had announced, now we're going to prepare
        # a new BotSession for the bot and once done assign a new deadline.
        self._untrack_deadline_for_botsession(bot_session.name)

        for lease in list(bot_session.leases):
            checked_lease = self._check_lease_state(lease)
            if not checked_lease:
                # TODO: Make sure we don't need this
                try:
                    self._assigned_leases[name].remove(lease.id)
                except KeyError:
                    pass
                try:
                    self._scheduler.delete_job_lease(lease.id)
                except NotFoundError:
                    # Job already dropped from scheduler
                    pass

                bot_session.leases.remove(lease)

        self._request_leases(bot_session, deadline, name)
        # Assign a new deadline to the BotSession
        self._assign_deadline_for_botsession(bot_session, name)

        self.__logger.debug("Sending session update, name=[%s], for bot=[%s], leases=[%s]",
                            bot_session.name, bot_session.bot_id,
                            ",".join([lease.id[:8] for lease in bot_session.leases]))

        return bot_session

    # --- Private API ---
    def _request_leases(self, bot_session, deadline=None, name=None):
        # Only send one lease at a time currently.
        if bot_session.status == BotStatus.OK.value and not bot_session.leases:
            worker_capabilities = {}

            # TODO? Fail if there are no devices in the worker?
            if bot_session.worker.devices:
                # According to the spec:
                #   "The first device in the worker is the "primary device" -
                #   that is, the device running a bot and which is
                #   responsible for actually executing commands."
                primary_device = bot_session.worker.devices[0]

                for device_property in primary_device.properties:
                    if device_property.key not in worker_capabilities:
                        worker_capabilities[device_property.key] = set()
                    worker_capabilities[device_property.key].add(device_property.value)

            # If the client specified deadline is less than NETWORK_TIMEOUT,
            # the response shouldn't long poll for work.
            if deadline and (deadline > NETWORK_TIMEOUT):
                deadline = deadline - NETWORK_TIMEOUT
            else:
                deadline = None

            leases = self._scheduler.request_job_leases(
                worker_capabilities,
                timeout=deadline,
                worker_name=name)

            if leases:
                for lease in leases:
                    self._assigned_leases[bot_session.name].add(lease.id)
                bot_session.leases.extend(leases)

    def _check_lease_state(self, lease):
        # careful here
        # should store bot name in scheduler
        lease_state = LeaseState(lease.state)

        # Lease has replied with cancelled, remove
        if lease_state == LeaseState.CANCELLED:
            return None

        try:
            if self._scheduler.get_job_lease_cancelled(lease.id):
                lease.state = LeaseState.CANCELLED.value
                return lease
        except KeyError:
            # Job does not exist, remove from bot.
            return None

        self._scheduler.update_job_lease_state(lease.id, lease)

        if lease_state == LeaseState.COMPLETED:
            return None

        return lease

    def _check_bot_ids(self, bot_id, name=None):
        """ Checks whether the ID and the name of the bot match,
        otherwise closes the bot sessions with that name or ID
        """
        if name is not None:
            _bot_id = self._bot_ids.get(name)
            if _bot_id is None:
                eviction_record = self._evicted_bot_sessions.get(name)
                if eviction_record:
                    raise InvalidArgumentError("Server has recently evicted the bot_name=[{}] at "
                                               "timestamp=[{}], reason=[{}]".format(name, eviction_record[0],
                                                                                    eviction_record[1]))
                raise InvalidArgumentError('Name not registered on server: bot_name=[{}]'.format(name))
            elif _bot_id != bot_id:
                self._close_bot_session(name, reason="bot_id mismatch between worker and bgd")
                raise InvalidArgumentError(
                    'Bot id invalid. ID sent: bot_id=[{}] with name: bot_name[{}].'
                    'ID registered: bgd_bot_id[{}] for that name'.format(bot_id, name, _bot_id))
        else:
            for _name, _bot_id in self._bot_ids.items():
                if bot_id == _bot_id:
                    self._close_bot_session(_name, reason="bot already registered and given name")
                    raise InvalidArgumentError(
                        'Bot id already registered. ID sent: bot_id=[{}].'
                        'Id registered: bgd_bot_id=[{}] with bgd_bot_name=[{}]'.format(bot_id, _bot_id, _name))

    def _assign_deadline_for_botsession(self, bot_session, bot_session_name):
        """ Assigns a deadline to the BotSession if bgd was configured to do so
        """
        # Specify bot keepalive expiry time if timeout is set
        if self._bot_session_keepalive_timeout:
            # Calculate expire time
            expire_time_python = datetime.utcnow() + timedelta(seconds=self._bot_session_keepalive_timeout)

            # Set it in the bot_session
            bot_session.expire_time.FromDatetime(expire_time_python)

            # Keep track internally for the botsession reaper
            self._track_deadline_for_bot_session(bot_session_name, expire_time_python)

    def _untrack_deadline_for_botsession(self, bot_session_name):
        """ Un-assigns the session reaper tracked deadline of the BotSession
        if bgd was configured to do so
        """
        # Specify bot keepalive expiry time if timeout is set
        if self._bot_session_keepalive_timeout:
            self._track_deadline_for_bot_session(bot_session_name, None)

    def _track_deadline_for_bot_session(self, bot_session_name, new_deadline):
        """ Updates the data structures keeping track of the last deadline
        we had assigned to this BotSession by name.
        When `new_deadline` is set to None, the deadline is unassigned.
        """
        # Keep track of the next expire time to inform the watcher
        updated_next_expire_time = False

        if new_deadline:
            # Since we're re-setting the update time for this bot, make sure to move it
            # to the end of the OrderedDict
            try:
                self._ordered_expire_times_by_botsession.move_to_end(bot_session_name)
            except KeyError:
                pass

            self._ordered_expire_times_by_botsession[bot_session_name] = new_deadline
            updated_next_expire_time = True
        else:
            try:
                if self._ordered_expire_times_by_botsession.pop(bot_session_name):
                    updated_next_expire_time = True
            except KeyError:
                self.__logger.debug("Tried to un-assign deadline for bot_session_name=[%s] "
                                    "but it had no deadline to begin with.", bot_session_name)
                pass

        # Make the botsession reaper thread look at the current new_deadline
        if updated_next_expire_time:
            self._update_next_expire_time()
            self._deadline_event.set()

    def _check_assigned_leases(self, bot_session):
        session_lease_ids = []

        for lease in bot_session.leases:
            session_lease_ids.append(lease.id)

        for lease_id in self._assigned_leases[bot_session.name]:
            if lease_id not in session_lease_ids:
                self.__logger.error("Assigned lease id=[%s],"
                                    " not found on bot with name=[%s] and id=[%s]."
                                    " Retrying job", lease_id, bot_session.name, bot_session.bot_id)
                try:
                    self._scheduler.retry_job_lease(lease_id)
                except NotFoundError:
                    pass

    def _close_bot_session(self, name, *, reason=None):
        """ Before removing the session, close any leases and
        requeue with high priority.
        """
        bot_id = self._bot_ids.get(name)

        if bot_id is None:
            raise InvalidArgumentError("Bot id does not exist: [{}]".format(name))

        self.__logger.debug("Attempting to close [%s] with name: [%s]", bot_id, name)
        for lease_id in self._assigned_leases[name]:
            try:
                self._scheduler.retry_job_lease(lease_id)
            except NotFoundError:
                pass
        self._assigned_leases.pop(name)

        # If we had assigned an expire_time for this botsession, make sure to
        # clean up, regardless of the reason we end up closing this BotSession
        self._untrack_deadline_for_botsession(name)

        # Make sure we're only keeping the last N evicted sessions
        if len(self._evicted_bot_sessions) > self._remember_last_n_evicted_bot_sessions:
            self._evicted_bot_sessions.popitem()
        # Record this eviction
        self._evicted_bot_sessions[name] = (datetime.utcnow(), reason)

        self.__logger.debug("Closing bot session: [%s]", name)
        self._bot_ids.pop(name)
        self.__logger.info("Closed bot [%s] with name: [%s]", bot_id, name)

    def _update_next_expire_time(self):
        # If we don't have any more bot_session deadlines, clear out this variable
        # to avoid busy-waiting. Otherwise, populate it with the next known expiry time
        _, next_expire_time = self._get_next_botsession_expiry()
        self._next_expire_time = next_expire_time

    def _next_expire_time_occurs_in(self):
        if self._next_expire_time:
            next_expire_time = math.ceil((self._next_expire_time -
                                          datetime.utcnow()).total_seconds())
            # Pad this with 1 second so that the expiry actually happens
            return max(0, next_expire_time + 1)

        return None

    def _get_next_botsession_expiry(self):
        botsession_name = None
        expire_time = None
        if self._ordered_expire_times_by_botsession:
            botsession_name = next(iter(self._ordered_expire_times_by_botsession.keys()))
            expire_time = self._ordered_expire_times_by_botsession[botsession_name]

        return (botsession_name, expire_time)

    def _reap_next_expired_session(self):
        self.__logger.debug("Checking for next session to reap...")
        now = datetime.utcnow()

        if self._ordered_expire_times_by_botsession:
            next_botsession_name_to_expire, next_botsession_expire_time = self._get_next_botsession_expiry()

            if next_botsession_expire_time <= now:
                # Pop the next in-order expiring bot session
                self._ordered_expire_times_by_botsession.popitem()

                # This is the last deadline we have communicated with this bot...
                # It has expired.
                self.__logger.warning("BotSession name=[%s] for bot=[%s] with deadline=[%s] "
                                      "has expired.", next_botsession_name_to_expire,
                                      self._bot_ids.get(next_botsession_name_to_expire),
                                      next_botsession_expire_time)
                try:
                    self._close_bot_session(next_botsession_name_to_expire, reason="expired")
                except InvalidArgumentError:
                    self.__logger.warning("Expired BotSession name=[%s] for bot=[%s] with deadline=[%s] "
                                          "was already closed.", next_botsession_name_to_expire,
                                          self._bot_ids.get(next_botsession_name_to_expire),
                                          next_botsession_expire_time)
                    pass

                self._update_next_expire_time()

    async def _reap_expired_sessions_loop(self):
        try:
            self.__logger.info("Starting BotSession reaper, bot_session_keepalive_timeout=[%s].",
                               self._bot_session_keepalive_timeout)
            while True:
                try:
                    # for <= 0, assume something expired already
                    expires_in = self._next_expire_time_occurs_in()
                    if expires_in:
                        self.__logger.debug("Waiting for an event indicating earlier expiry or wait=[%s]"
                                            " for a the next BotSession to expire.", expires_in)
                    else:
                        self.__logger.debug("No more BotSessions to watch for expiry, waiting for new BotSessions.")
                    await asyncio.wait_for(self._deadline_event.wait(), timeout=expires_in)
                    self._deadline_event.clear()
                except asyncio.TimeoutError:
                    pass

                self._reap_next_expired_session()
        except asyncio.CancelledError:
            self.__logger.info("Cancelled reaper task.")
            pass
        except Exception as exception:
            self.__logger.exception(exception)
            raise

    def _setup_bot_session_reaper_loop(self):
        if self._bot_session_keepalive_timeout:
            if self._bot_session_keepalive_timeout <= 0:
                raise InvalidArgumentError("[bot_session_keepalive_timeout] set to [%s], "
                                           "must be > 0, in seconds", self._bot_session_keepalive_timeout)

            # Add the expired session reaper in the event loop
            main_loop = asyncio.get_event_loop()
            main_loop.create_task(self._reap_expired_sessions_loop())
