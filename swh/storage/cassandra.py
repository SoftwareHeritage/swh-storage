# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import json
import logging

from cassandra import WriteFailure, ReadFailure
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

from swh.model.model import (
    TimestampWithTimezone, Timestamp, Person, RevisionType,
    Revision,
)

from .journal_writer import get_journal_writer
from . import converters


logger = logging.getLogger(__name__)


def revision_to_db(revision):
    metadata = revision.get('metadata')
    if metadata and 'extra_headers' in metadata:
        extra_headers = converters.git_headers_to_db(
            metadata['extra_headers'])
        revision = {
            **revision,
            'metadata': {
                **metadata,
                'extra_headers': extra_headers
            }
        }

    revision = Revision.from_dict(revision)
    revision.type = revision.type.value
    revision.metadata = json.dumps(revision.metadata)

    return revision


def revision_from_db(rev):
    rev.type = RevisionType(rev.type)
    metadata = json.loads(rev.metadata)
    if metadata and 'extra_headers' in metadata:
        extra_headers = converters.db_to_git_headers(
            metadata['extra_headers'])
        metadata['extra_headers'] = extra_headers
    rev.metadata = metadata

    return rev


def prepared_statement(query):
    def decorator(f):
        @functools.wraps(f)
        def newf(self, *args, **kwargs):
            if f.__name__ not in self._prepared_statements:
                self._prepared_statements[f.__name__] = \
                    self._session.prepare(query)
            return f(self, *args, **kwargs,
                     statement=self._prepared_statements[f.__name__])
        return newf
    return decorator


def prepared_insert_statement(table_name, keys):
    return prepared_statement(
        'INSERT INTO %s (%s) VALUES (%s)' %
        (table_name, ', '.join(keys), ', '.join('?' for _ in keys))
    )


class CassandraProxy:
    def __init__(self, hosts, keyspace, port):
        self._cluster = Cluster(
            hosts, port=port,
            load_balancing_policy=RoundRobinPolicy())
        self._session = self._cluster.connect(keyspace)
        self._cluster.register_user_type(
            keyspace, 'microtimestamp_with_timezone', TimestampWithTimezone)
        self._cluster.register_user_type(
            keyspace, 'microtimestamp', Timestamp)
        self._cluster.register_user_type(
            keyspace, 'person', Person)

        self._prepared_statements = {}

    MAX_RETRIES = 3

    _revision_keys = [
        'id', 'date', 'committer_date', 'type', 'directory', 'message',
        'author', 'committer', 'parents',
        'synthetic', 'metadata']

    def execute_and_retry(self, statement, *args):
        for nb_retries in range(self.MAX_RETRIES):
            try:
                return self._session.execute(statement, *args)
            except WriteFailure as e:
                logger.error('Failed to write object to cassandra: %r', e)
            except ReadFailure as e:
                logger.error('Failed to read object(s) to cassandra: %r', e)
            if nb_retries == self.MAX_RETRIES-1:
                raise e  # noqa

    def _add_one(self, statement, obj, keys):
        self.execute_and_retry(
            statement, [getattr(obj, key) for key in keys])

    @prepared_insert_statement('revision', _revision_keys)
    def revision_add_one(self, revision, *, statement):
        self._add_one(statement, revision, self._revision_keys)


class CassandraStorage:
    def __init__(self, hosts, keyspace, port=9042, journal_writer=None):
        self._proxy = CassandraProxy(hosts, keyspace, port)

        if journal_writer:
            self.journal_writer = get_journal_writer(**journal_writer)
        else:
            self.journal_writer = None

    def check_config(self, check_write=False):
        return True

    def revision_add(self, revisions, check_missing=True):
        if self.journal_writer:
            self.journal_writer.write_additions('revision', revisions)

        if check_missing:
            missing = self.revision_missing([rev['id'] for rev in revisions])

        for revision in revisions:
            if check_missing and revision['id'] not in missing:
                continue

            revision = revision_to_db(revision)

            if revision:
                self._proxy.revision_add_one(revision)

        if check_missing:
            return {'revision:add': len(missing)}
        else:
            return {'revision:add': len(revisions)}

    def revision_missing(self, revision_ids):
        res = self._proxy.execute_and_retry(
            'SELECT id FROM revision WHERE id IN (%s)' %
            ', '.join('%s' for _ in revision_ids),
            revision_ids)
        found_ids = {id_ for (id_,) in res}
        return set(revision_ids) - found_ids

    def revision_get(self, revision_ids):
        rows = self._proxy.execute_and_retry(
            'SELECT * FROM revision WHERE id IN ({})'.format(
                ', '.join('%s' for _ in revision_ids)),
            revision_ids)
        revs = {}
        for row in rows:
            rev = Revision(**row._asdict())
            rev = revision_from_db(rev)
            revs[rev.id] = rev.to_dict()

        for rev_id in revision_ids:
            yield revs.get(rev_id)

    def _get_parent_revs(self, rev_ids, seen, limit, short):
        if limit and len(seen) >= limit:
            return
        rev_ids = [id_ for id_ in rev_ids if id_ not in seen]
        if not rev_ids:
            return
        seen |= set(rev_ids)
        rows = self._proxy.execute_and_retry(
            'SELECT {} FROM revision WHERE id IN ({})'.format(
                'id, parents' if short else '*',
                ', '.join('%s' for _ in rev_ids)),
            rev_ids)
        for row in rows:
            if short:
                (id_, parents) = row
                yield (id_, parents)
            else:
                rev = revision_from_db(Revision(**row._asdict()))
                parents = rev.parents
                yield rev.to_dict()
            yield from self._get_parent_revs(parents, seen, limit, short)

    def revision_log(self, revision_ids, limit=None):
        """Fetch revision entry from the given root revisions.

        Args:
            revisions: array of root revision to lookup
            limit: limitation on the output result. Default to None.

        Yields:
            List of revision log from such revisions root.

        """
        seen = set()
        yield from self._get_parent_revs(revision_ids, seen, limit, False)

    def revision_shortlog(self, revisions, limit=None):
        """Fetch the shortlog for the given revisions

        Args:
            revisions: list of root revisions to lookup
            limit: depth limitation for the output

        Yields:
            a list of (id, parents) tuples.

        """
        seen = set()
        yield from self._get_parent_revs(revisions, seen, limit, True)
