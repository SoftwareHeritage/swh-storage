# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import json
import logging

from cassandra import WriteFailure, WriteTimeout, ReadFailure, ReadTimeout
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy

from swh.model.model import (
    TimestampWithTimezone, Timestamp, Person, RevisionType,
    Revision, Directory, DirectoryEntry,
)

from .journal_writer import get_journal_writer
from . import converters


logger = logging.getLogger(__name__)


CREATE_TABLES_QUERIES = [
    '''
CREATE TYPE IF NOT EXISTS microtimestamp (
    seconds             bigint,
    microseconds        int
)
''',
    '''
CREATE TYPE IF NOT EXISTS microtimestamp_with_timezone (
    timestamp           frozen<microtimestamp>,
    offset              smallint,
    negative_utc        boolean
);
''',
    '''
CREATE TYPE IF NOT EXISTS person (
    fullname    blob,
    name        blob,
    email       blob
);
''',
    '''
CREATE TYPE IF NOT EXISTS dir_entry (
    target  blob,  -- id of target revision
    name    blob,  -- path name, relative to containing dir
    perms   int,   -- unix-like permissions
    type    ascii
);
''',
    '''
CREATE TABLE IF NOT EXISTS revision (
    id                              blob PRIMARY KEY,
    date                            microtimestamp_with_timezone,
    committer_date                  microtimestamp_with_timezone,
    type                            ascii,
    directory                       blob,  -- source code "root" directory
    message                         blob,
    author                          person,
    committer                       person,
    parents                         frozen<list<blob>>,
    synthetic                       boolean,
        -- true iff revision has been created by Software Heritage
    metadata                        text
        -- extra metadata as JSON(tarball checksums,
        -- extra commit information, etc...)
);
''',
    '''
CREATE TABLE IF NOT EXISTS directory (
    id        blob PRIMARY KEY,
    entries_  frozen<list<dir_entry>>
);
''',
]


def create_keyspace(hosts, keyspace, port=9042):
    cluster = Cluster(
        hosts, port=port,
        load_balancing_policy=RoundRobinPolicy())
    session = cluster.connect()
    session.execute('''CREATE KEYSPACE IF NOT EXISTS "%s"
                       WITH REPLICATION = {
                           'class' : 'SimpleStrategy',
                           'replication_factor' : 1
                       };
                    ''' % keyspace)
    session.execute('USE "%s"' % keyspace)
    for query in CREATE_TABLES_QUERIES:
        session.execute(query)


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
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
        self._session = self._cluster.connect(keyspace)
        self._cluster.register_user_type(
            keyspace, 'microtimestamp_with_timezone', TimestampWithTimezone)
        self._cluster.register_user_type(
            keyspace, 'microtimestamp', Timestamp)
        self._cluster.register_user_type(
            keyspace, 'person', Person)
        self._cluster.register_user_type(
            keyspace, 'dir_entry', DirectoryEntry)

        self._prepared_statements = {}

    MAX_RETRIES = 3

    _revision_keys = [
        'id', 'date', 'committer_date', 'type', 'directory', 'message',
        'author', 'committer', 'parents',
        'synthetic', 'metadata']

    _directory_keys = ['id', 'entries_']
    _directory_attributes = ['id', 'entries']

    def execute_and_retry(self, statement, *args):
        for nb_retries in range(self.MAX_RETRIES):
            try:
                return self._session.execute(statement, *args, timeout=100.)
            except (WriteFailure, WriteTimeout) as e:
                logger.error('Failed to write object to cassandra: %r', e)
                if nb_retries == self.MAX_RETRIES-1:
                    raise e
            except (ReadFailure, ReadTimeout) as e:
                logger.error('Failed to read object(s) to cassandra: %r', e)
                if nb_retries == self.MAX_RETRIES-1:
                    raise e

    def _add_one(self, statement, obj, keys):
        self.execute_and_retry(
            statement, [getattr(obj, key) for key in keys])

    @prepared_insert_statement('revision', _revision_keys)
    def revision_add_one(self, revision, *, statement):
        self._add_one(statement, revision, self._revision_keys)

    @prepared_insert_statement('directory', _directory_keys)
    def directory_add_one(self, directory, *, statement):
        self._add_one(statement, directory, self._directory_attributes)


class CassandraStorage:
    def __init__(self, hosts, keyspace, port=9042, journal_writer=None):
        self._proxy = CassandraProxy(hosts, keyspace, port)

        if journal_writer:
            self.journal_writer = get_journal_writer(**journal_writer)
        else:
            self.journal_writer = None

    def check_config(self, check_write=False):
        self._proxy.execute_and_retry('SELECT uuid() FROM revision LIMIT 1;')

        return True

    def _missing(self, table, ids):
        res = self._proxy.execute_and_retry(
            'SELECT id FROM %s WHERE id IN (%s)' %
            (table, ', '.join('%s' for _ in ids)),
            ids
        )
        found_ids = {id_ for (id_,) in res}
        return set(ids) - found_ids

    def content_find(self, content):
        return None

    def directory_add(self, directories):
        if self.journal_writer:
            self.journal_writer.write_additions('directory', directories)

        missing = self.directory_missing([dir_['id'] for dir_ in directories])

        for directory in directories:
            if directory['id'] in missing:
                self._proxy.directory_add_one(
                    Directory.from_dict(directory))

        return {'directory:add': len(missing)}

    def directory_missing(self, directory_ids):
        return self._missing('directory', directory_ids)

    def _join_dentry_to_content(self, dentry):
        keys = (
            'status',
            'sha1',
            'sha1_git',
            'sha256',
            'length',
        )
        ret = dict.fromkeys(keys)
        ret.update(dentry.to_dict())
        if ret['type'] == 'file':
            content = self.content_find({'sha1_git': ret['target']})
            if content:
                for key in keys:
                    ret[key] = content[key]
        return ret

    def _directory_ls(self, directory_id, recursive, prefix=b''):
        rows = list(self._proxy.execute_and_retry(
            'SELECT * FROM directory WHERE id = %s',
            (directory_id,)))
        if not rows:
            return
        assert len(rows) == 1

        dir_ = rows[0]._asdict()
        dir_['entries'] = dir_.pop('entries_')
        dir_ = Directory(**dir_)
        for entry in dir_.entries:
            ret = self._join_dentry_to_content(entry)
            ret['name'] = prefix + ret['name']
            ret['dir_id'] = directory_id
            yield ret
            if recursive and ret['type'] == 'dir':
                yield from self._directory_ls(
                    ret['target'], True, prefix + ret['name'] + b'/')

    def directory_entry_get_by_path(self, directory, paths):
        if not paths:
            return

        contents = list(self.directory_ls(directory))

        if not contents:
            return

        def _get_entry(entries, name):
            for entry in entries:
                if entry['name'] == name:
                    return entry

        first_item = _get_entry(contents, paths[0])

        if len(paths) == 1:
            return first_item

        if not first_item or first_item['type'] != 'dir':
            return

        return self.directory_entry_get_by_path(
                first_item['target'], paths[1:])

    def directory_ls(self, directory_id, recursive=False):
        yield from self._directory_ls(directory_id, recursive)

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
        return self._missing('revision', revision_ids)

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
