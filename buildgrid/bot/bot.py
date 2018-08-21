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
Bot
====

Creates a bot session.
"""

import asyncio
import logging


class Bot:
    """
    Creates a local BotSession.
    """

    def __init__(self, bot_session, update_period=1):
        self.logger = logging.getLogger(__name__)

        self._bot_session = bot_session
        self._update_period = update_period

    def session(self, work, context):
        loop = asyncio.get_event_loop()

        self._bot_session.create_bot_session(work, context)

        try:
            task = asyncio.ensure_future(self._update_bot_session())
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            task.cancel()
            loop.close()

    async def _update_bot_session(self):
        """
        Calls the server periodically to inform the server the client has not died.
        """
        while True:
            self._bot_session.update_bot_session()
            await asyncio.sleep(self._update_period)
