# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from copy import deepcopy
import functools
import json

from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

from swh.model.model import (
    TimestampWithTimezone, Timestamp, Person, RevisionType,
    Revision,
)

from .journal_writer import get_journal_writer
from . import converters


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


class CassandraStorage:
    def __init__(self, hosts, keyspace, port=9042, journal_writer=None):
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

        if journal_writer:
            self.journal_writer = get_journal_writer(**journal_writer)
        else:
            self.journal_writer = None

    def check_config(self, check_write=False):
        return True

    _revision_keys = [
        'id', 'date', 'committer_date', 'type', 'directory', 'message',
        'author', 'committer', 'parents',
        'synthetic', 'metadata']

    @prepared_statement(
        '''INSERT INTO revision (%s)
           VALUES (%s)''' % (
            ', '.join(_revision_keys),
            ', '.join('?' for _ in _revision_keys)))
    def revision_add(self, revisions, statement):
        if self.journal_writer:
            self.journal_writer.write_additions('revision', revisions)

        missing = self.revision_missing([rev['id'] for rev in revisions])

        for revision in revisions:
            revision = deepcopy(revision)
            if revision['id'] not in missing:
                continue
            metadata = revision.get('metadata')
            if metadata and 'extra_headers' in metadata:
                metadata = metadata.copy()
                extra_headers = converters.git_headers_to_db(
                    metadata['extra_headers'])
                metadata['extra_headers'] = extra_headers
                revision['metadata'] = metadata

            revision = Revision.from_dict(revision)
            revision.type = revision.type.value
            revision.metadata = json.dumps(revision.metadata)
            self._session.execute(
                statement,
                [getattr(revision, key) for key in self._revision_keys])
            print('insert: %r' % revision)

        return {'revision:add': len(missing)}

    def revision_missing(self, revision_ids):
        res = self._session.execute(
            'SELECT id FROM revision WHERE id IN (%s)' %
            ', '.join('%s' for _ in revision_ids),
            revision_ids)
        found_ids = {id_ for (id_,) in res}
        return set(revision_ids) - found_ids

    def revision_get(self, revision_ids):
        rows = self._session.execute(
            'SELECT * FROM revision WHERE id IN (%s)' %
            ', '.join('%s' for _ in revision_ids),
            revision_ids)
        revs = {}
        for row in rows:
            rev = Revision(**row._asdict())
            rev.type = RevisionType(rev.type)
            metadata = json.loads(rev.metadata)
            if metadata and 'extra_headers' in metadata:
                extra_headers = converters.db_to_git_headers(
                    metadata['extra_headers'])
                metadata['extra_headers'] = extra_headers
            rev.metadata = metadata
            print('select: %r' % rev)
            revs[rev.id] = rev.to_dict()

        for rev_id in revision_ids:
            yield revs.get(rev_id)
