# Copyright (C) 2019 Bloomberg LP
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
SQLIndex
==================

A SQL index implementation. This can be pointed to either a remote SQL server
or a local SQLite database.

"""

from contextlib import contextmanager
from datetime import datetime
import logging
import os
import time
import io
import itertools

from typing import Any, BinaryIO, ContextManager, Dict, Iterable, List, Optional, Sequence, Union

from alembic import command
from alembic.config import Config
from sqlalchemy import and_, create_engine, func, text, Column
from sqlalchemy.sql import select
from sqlalchemy.sql.elements import BinaryExpression as SQLExpression
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.session import sessionmaker, Session as SessionType
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.pool import StaticPool

from ..storage_abc import StorageABC
from .index_abc import IndexABC
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Digest
from buildgrid._protos.google.rpc import code_pb2
from buildgrid._protos.google.rpc.status_pb2 import Status
import buildgrid.server.persistence.sql as sql_module
from buildgrid.server.persistence.sql.models import IndexEntry


# Each dialect has a limit on the number of bind parameters allowed. This
# matters because it determines how large we can allow our IN clauses to get.
#
# SQLite: 1000 https://www.sqlite.org/limits.html#max_variable_number
# PostgreSQL: 32767 (Int16.MAX_VALUE) https://www.postgresql.org/docs/9.4/protocol-message-formats.html
#
# We'll refer to this as the "inlimit" in the code. The inlimits are
# set to 75% of the bind parameter limit of the implementation.
DIALECT_INLIMIT_MAP = {
    "sqlite": 750,
    "postgresql": 24000
}
DEFAULT_INLIMIT = 100


class SQLIndex(IndexABC):

    def _get_default_inlimit_for_current_dialect(self) -> int:
        """ Map a connection string to its inlimit. """
        dialect = self._engine.dialect.name
        if dialect not in DIALECT_INLIMIT_MAP:
            self.__logger.warning("The SQL dialect [%s] is unsupported, and "
                                  "errors may occur. Supported dialects are %s. "
                                  "Using default inclause limit of %s.",
                                  dialect,
                                  list(DIALECT_INLIMIT_MAP.keys()),
                                  DEFAULT_INLIMIT)
            return DEFAULT_INLIMIT

        return DIALECT_INLIMIT_MAP[dialect]

    def __init__(self, storage: StorageABC, connection_string: str,
                 automigrate: bool=False, window_size: int=1000,
                 inclause_limit: int=-1, **kwargs):
        base_argnames = ['fallback_on_get']
        base_args = {}
        for arg in base_argnames:
            if arg in kwargs:
                base_args[arg] = kwargs.pop(arg)
        super().__init__(**base_args)
        self.__logger = logging.getLogger(__name__)

        self._storage = storage

        # Max # of rows to fetch while iterating over all blobs
        # (e.g. in least_recent_digests)
        self._all_blobs_window_size = window_size

        # Only pass known kwargs to db session
        available_options = {'pool_size', 'max_overflow', 'pool_timeout'}
        kwargs_keys = kwargs.keys()
        if kwargs_keys > available_options:
            unknown_args = kwargs_keys - available_options
            raise TypeError("Unknown keyword arguments: [%s]" % unknown_args)

        self._create_sqlalchemy_engine(connection_string, automigrate, **kwargs)

        if inclause_limit > 0:
            if inclause_limit > window_size:
                self.__logger.warning(
                    "Configured inclause_limit [%s] "
                    "is greater than window_size [%s]",
                    inclause_limit,
                    window_size)
            self._inclause_limit = inclause_limit
        else:
            # If the inlimit isn't explicitly set, we use a default that
            # respects both the window size and the db implementation's
            # inlimit.
            self._inclause_limit = min(
                window_size,
                self._get_default_inlimit_for_current_dialect())
            self.__logger.debug("SQL index: using default inclause limit "
                                "of %s", self._inclause_limit)

        session_factory = sessionmaker()
        self.Session = scoped_session(session_factory)
        self.Session.configure(bind=self._engine)

    def _create_or_migrate_db(self, connection_string: str) -> None:
        self.__logger.warn(
            "Will attempt migration to latest version if needed.")

        config = Config()

        config.set_main_option("script_location",
                               os.path.join(
                                   os.path.dirname(__file__),
                                   "../../../persistence/sql/alembic"))

        with self._engine.begin() as connection:
            config.attributes['connection'] = connection
            command.upgrade(config, "head")

    def _create_sqlalchemy_engine(self, connection_string, automigrate, **kwargs):
        self.automigrate = automigrate

        if self._is_sqlite_inmemory_connection_string(connection_string):
            raise ValueError("Cannot use SQLite in-memory with BuildGrid (connection_string=[%s]). "
                             "Use a file or leave the connection_string empty for a tempfile." %
                             connection_string)

        # When using SQLite, make sure to set the appropriate options
        # to make this work with multiple threads
        # ref: https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#threading-pooling-behavior
        if self._is_sqlite_connection_string(connection_string):
            # Disallow sqlite in-memory because multi-threaded access to it
            # could cause problems
            # Disable 'check_same_thread' to share this among all threads
            if 'connect_args' not in kwargs:
                kwargs['connect_args'] = dict()
            try:
                kwargs['connect_args']['check_same_thread'] = False
            except:
                raise TypeError("Expected SQLDataStore 'connect_args' to be a dict, got "
                                "[connect_args=%s]" % kwargs['connect_args'])

            # Use the 'StaticPool' to make sure SQLite usage is always exclusive
            kwargs['poolclass'] = StaticPool
            self.__logger.debug("Automatically setting up SQLAlchemy with SQLite specific options.")

        # Only pass the (known) kwargs that have been explicitly set by the user
        available_options = set(['pool_size', 'max_overflow', 'pool_timeout',
                                'poolclass', 'connect_args'])
        kwargs_keys = set(kwargs.keys())
        if not kwargs_keys.issubset(available_options):
            unknown_options = kwargs_keys - available_options
            raise TypeError("Unknown keyword arguments: [%s]" % unknown_options)

        self.__logger.debug("SQLAlchemy additional kwargs: [%s]", kwargs)

        self._engine = create_engine(connection_string, echo=False, **kwargs)

        self.__logger.info("Using SQL backend for index at connection [%s] "
                           "using additional SQL options %s",
                           repr(self._engine.url), kwargs)

        if self.automigrate:
            self._create_or_migrate_db(connection_string)

    def _is_sqlite_connection_string(self, connection_string: str) -> bool:
        if connection_string:
            return connection_string.startswith("sqlite")
        return False

    def _is_sqlite_inmemory_connection_string(self, full_connection_string: str) -> bool:
        if self._is_sqlite_connection_string(full_connection_string):
            # Valid connection_strings for in-memory SQLite which we don't support could look like:
            # "sqlite:///file:memdb1?option=value&cache=shared&mode=memory",
            # "sqlite:///file:memdb1?mode=memory&cache=shared",
            # "sqlite:///file:memdb1?cache=shared&mode=memory",
            # "sqlite:///file::memory:?cache=shared",
            # "sqlite:///file::memory:",
            # "sqlite:///:memory:",
            # "sqlite:///",
            # "sqlite://"
            # ref: https://www.sqlite.org/inmemorydb.html
            # Note that a user can also specify drivers, so prefix could become 'sqlite+driver:///'
            connection_string = full_connection_string

            uri_split_index = connection_string.find("?")
            if uri_split_index != -1:
                connection_string = connection_string[0:uri_split_index]

            if connection_string.endswith((":memory:", ":///", "://")):
                return True
            elif uri_split_index != -1:
                opts = full_connection_string[uri_split_index + 1:].split("&")
                if "mode=memory" in opts:
                    return True

        return False

    @contextmanager
    def session(self, reraise: bool=False) -> ContextManager[SessionType]:
        """ Context manager for convenience use of sessions. Automatically
        commits when the context ends and rolls back failed transactions.

        Setting reraise to True causes this to reraise any exceptions
        after rollback so they can be handled directly by client logic.
        """
        session = self.Session()
        try:
            yield session
            session.commit()
        except:
            self.__logger.exception(
                "Error in index database session. Rolling back.")
            session.rollback()
            if reraise:
                raise
        finally:
            session.close()

    def _query_for_entry_by_digest(self, digest: Digest,
                                   session: SessionType) -> Query:
        """ Helper method to query for a blob """
        return session.query(IndexEntry).filter(
            IndexEntry.digest_hash == digest.hash
        )

    def has_blob(self, digest: Digest) -> bool:
        with self.session() as session:
            num_entries = self._query_for_entry_by_digest(
                digest, session).count()
            if num_entries == 1:
                return True
            elif num_entries < 1:
                return False
            else:
                raise RuntimeError(
                    "Multiple results found for blob [%s]. "
                    "The index is in a bad state")

    def _update_blob_timestamp(self, digest: Digest, session: SessionType,
                               sync_mode: Union[bool, str]=False) -> int:
        """ Refresh a blob's timestamp """
        entry = self._query_for_entry_by_digest(digest, session)
        num_rows_updated = entry.update({
            "accessed_timestamp": datetime.utcnow()
        }, synchronize_session=sync_mode)
        return num_rows_updated

    def _add_index_entry(self, digest: Digest, session: SessionType) -> None:
        """ Helper method to add an index entry """
        session.add(IndexEntry(
            digest_hash=digest.hash,
            digest_size_bytes=digest.size_bytes,
            accessed_timestamp=datetime.utcnow()
        ))

    def get_blob(self, digest: Digest) -> Optional[BinaryIO]:
        with self.session() as session:
            num_rows_updated = self._update_blob_timestamp(digest, session)
            if num_rows_updated > 1:
                raise RuntimeError(
                    "Multiple rows found for blob [%s]. "
                    "The index is in a bad state." % digest.hash)
            elif num_rows_updated == 0:
                # If fallback is enabled, try to fetch the blob and add it
                # to the index
                if self._fallback_on_get:
                    blob = self._storage.get_blob(digest)
                    if blob:
                        self._add_index_entry(digest, session)
                    return blob
                else:
                    # Otherwise just skip the storage entirely
                    return None
            else:
                # If exactly one row in the index was updated,
                # grab the blob
                blob = self._storage.get_blob(digest)
                if not blob:
                    self.__logger.warning("Unable to find blob [%s] in storage",
                                          digest.hash)
                return blob

    def delete_blob(self, digest: Digest) -> None:
        with self.session() as session:
            entry = self._query_for_entry_by_digest(digest, session)
            entry.delete(synchronize_session=False)
        self._storage.delete_blob(digest)

    def begin_write(self, digest: Digest) -> BinaryIO:
        return self._storage.begin_write(digest)

    def commit_write(self, digest: Digest, write_session: BinaryIO) -> None:
        try:
            with self.session(reraise=True) as session:
                self._add_index_entry(digest, session)
                self._storage.commit_write(digest, write_session)
        except IntegrityError:
            with self.session() as session:
                num_rows_updated = self._update_blob_timestamp(digest, session)
                if num_rows_updated > 1:
                    raise RuntimeError(
                        "Multiple rows found for blob [%s]. "
                        "The index is in a bad state." % digest.hash)
                self._storage.commit_write(digest, write_session)

    def _partitioned_hashes(self, digests: Sequence[Digest]) -> Iterable[List[str]]:
        """ Given a long list of digests, split it into parts no larger than
        _inclause_limit and yield the hashes in each part.
        """
        for part_start in range(0, len(digests), self._inclause_limit):
            part_end = min(len(digests), part_start + self._inclause_limit)
            part_digests = itertools.islice(digests, part_start, part_end)
            yield map(lambda digest: digest.hash, part_digests)

    def _bulk_select_digests(self, digests: Sequence[Digest]) -> Iterable[IndexEntry]:
        """ Generator that selects all rows matching a digest list.

        SQLAlchemy Core is used for this because the ORM has problems with
        large numbers of bind variables for WHERE IN clauses.

        We only select on the digest hash (not hash and size) to allow for
        index-only queries on db backends that support them.
        """
        index_table = IndexEntry.__table__
        with self.session() as session:
            for part in self._partitioned_hashes(digests):
                session.query(IndexEntry)
                stmt = select(
                    [index_table.c.digest_hash]
                ).where(
                    index_table.c.digest_hash.in_(part)
                )
                entries = session.execute(stmt)
                yield from entries

    def _bulk_refresh_timestamps(self, digests: Sequence[Digest], update_time: Optional[datetime]=None):
        """ Refresh all timestamps of the input digests.

        SQLAlchemy Core is used for this because the ORM is not suitable for
        bulk inserts and updates.

        https://docs.sqlalchemy.org/en/13/faq/performance.html#i-m-inserting-400-000-rows-with-the-orm-and-it-s-really-slow
        """
        index_table = IndexEntry.__table__
        # If a timestamp was passed in, use it
        if update_time:
            timestamp = update_time
        else:
            timestamp = datetime.utcnow()
        with self.session() as session:
            for part in self._partitioned_hashes(digests):
                stmt = index_table.update().where(
                    index_table.c.digest_hash.in_(part)
                ).values(
                    accessed_timestamp=timestamp
                )
                session.execute(stmt)

    def missing_blobs(self, digests: List[Digest]) -> List[Digest]:
        entries = self._bulk_select_digests(digests)

        found_hashes = {entry.digest_hash for entry in entries}

        # Update all timestamps
        self._bulk_refresh_timestamps(digests)

        return [digest for digest in digests if digest.hash not in found_hashes]

    def _save_digests_to_index(self, digests: List[Digest]) -> None:
        """ Persist a list of digests to the index.

        Any digests present are updated, and new digests are inserted.
        """
        update_time = datetime.utcnow()

        # Update existing digests
        self._bulk_refresh_timestamps(digests, update_time)

        # Figure out which digests need to be inserted
        entries = self._bulk_select_digests(digests)

        # Map digests to new entries
        entries_not_present = {
            digest.hash: IndexEntry(
                digest_hash=digest.hash,
                digest_size_bytes=digest.size_bytes,
                accessed_timestamp=update_time
            )
            for digest in digests
        }
        for entry in entries:
            del entries_not_present[entry.digest_hash]

        # Add new digests
        with self.session() as session:
            session.bulk_save_objects(entries_not_present.values())

    def bulk_update_blobs(self, blobs: List[bytes]) -> List[Status]:
        results = self._storage.bulk_update_blobs(blobs)
        digests_to_save = [
            digest for ((digest, data), result) in zip(blobs, results)
            if result.code == code_pb2.OK
        ]
        self._save_digests_to_index(digests_to_save)
        return results

    def bulk_read_blobs(self, digests: List[Digest]) -> Dict[str, BinaryIO]:
        hash_to_digest = {digest.hash: digest for digest in digests}
        if self._fallback_on_get:
            # If fallback is enabled, query the backend first and update
            # the index which each blob found there
            results = self._storage.bulk_read_blobs(digests)
            # Save only the results that were fetched
            digests_to_save = [
                hash_to_digest[digest_hash] for digest_hash in results
                if results[digest_hash] is not None
            ]
            self._save_digests_to_index(digests_to_save)
            return results
        else:
            # If fallback is disabled, query the index first and only
            # query the storage for blobs found there
            entries = self._bulk_select_digests(digests)
            digests_to_fetch = [hash_to_digest[entry.digest_hash]
                                for entry in entries]
            # Update timestamps
            self._bulk_refresh_timestamps(digests_to_fetch)
            return self._storage.bulk_read_blobs(digests_to_fetch)

    def _column_windows(self, session: SessionType, column: Column) -> SQLExpression:
        """ Adapted from the sqlalchemy WindowedRangeQuery recipe.
        https://github.com/sqlalchemy/sqlalchemy/wiki/WindowedRangeQuery

        This method breaks the timestamp range into windows and yields
        the borders of these windows to the callee. For example, the borders
        yielded by this might look something like
        ('2019-10-08 18:25:03.699863', '2019-10-08 18:25:03.751018')
        ('2019-10-08 18:25:03.751018', '2019-10-08 18:25:03.807867')
        ('2019-10-08 18:25:03.807867', '2019-10-08 18:25:03.862192')
        ('2019-10-08 18:25:03.862192',)

        _windowed_lru_digests uses these borders to form WHERE clauses for its
        SELECTs. In doing so, we make sure to repeatedly query the database for
        live updates, striking a balance between loading the entire resultset
        into memory and querying each row individually, both of which are
        inefficient in the context of a large index.

        The window size is a parameter and can be configured. A larger window
        size will yield better performance (fewer SQL queries) at the cost of
        memory (holding on to the results of the query) and accuracy (blobs
        may get updated while you're working on them), and vice versa for a
        smaller window size.
        """

        def int_for_range(start_id: Any, end_id: Any) -> SQLExpression:
            if end_id:
                return and_(
                    column >= start_id,
                    column < end_id
                )
            else:
                return column >= start_id

        q = session.query(
            column,
            func.row_number()
                .over(order_by=column)
                .label('rownum')
        ).from_self(column)

        if self._all_blobs_window_size > 1:
            q = q.filter(text("rownum %% %d=1" % self._all_blobs_window_size))

        intervals = [id for id, in q]

        while intervals:
            start = intervals.pop(0)
            if intervals:
                end = intervals[0]
            else:
                end = None
            yield int_for_range(start, end)

    def _windowed_lru_digests(self, q: Query, column: Column) -> Iterable[IndexEntry]:
        """ Generate a query for each window produced by _column_windows
        and yield the results one by one.
        """
        for whereclause in self._column_windows(q.session, column):
            window = q.filter(whereclause).order_by(column.asc())
            yield from window

    def least_recent_digests(self) -> Iterable[Digest]:
        with self.session() as session:
            q = session.query(IndexEntry)
            for index_entry in self._windowed_lru_digests(q, IndexEntry.accessed_timestamp):
                yield Digest(hash=index_entry.digest_hash, size_bytes=index_entry.digest_size_bytes)
