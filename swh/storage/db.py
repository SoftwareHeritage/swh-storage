# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import binascii
import datetime
import enum
import functools
import json
import psycopg2
import psycopg2.extras
import select
import tempfile

from contextlib import contextmanager

from swh.model import hashutil

TMP_CONTENT_TABLE = 'tmp_content'


psycopg2.extras.register_uuid()


def stored_procedure(stored_proc):
    """decorator to execute remote stored procedure, specified as argument

    Generally, the body of the decorated function should be empty. If it is
    not, the stored procedure will be executed first; the function body then.

    """
    def wrap(meth):
        @functools.wraps(meth)
        def _meth(self, *args, **kwargs):
            cur = kwargs.get('cur', None)
            self._cursor(cur).execute('SELECT %s()' % stored_proc)
            meth(self, *args, **kwargs)
        return _meth
    return wrap


def jsonize(value):
    """Convert a value to a psycopg2 JSON object if necessary"""
    if isinstance(value, dict):
        return psycopg2.extras.Json(value)

    return value


def entry_to_bytes(entry):
    """Convert an entry coming from the database to bytes"""
    if isinstance(entry, memoryview):
        return entry.tobytes()
    if isinstance(entry, list):
        return [entry_to_bytes(value) for value in entry]
    return entry


def line_to_bytes(line):
    """Convert a line coming from the database to bytes"""
    if not line:
        return line
    if isinstance(line, dict):
        return {k: entry_to_bytes(v) for k, v in line.items()}
    return line.__class__(entry_to_bytes(entry) for entry in line)


def cursor_to_bytes(cursor):
    """Yield all the data from a cursor as bytes"""
    yield from (line_to_bytes(line) for line in cursor)


class BaseDb:
    """Base class for swh.storage.*Db.

    cf. swh.storage.db.Db, swh.archiver.db.ArchiverDb

    """

    @classmethod
    def connect(cls, *args, **kwargs):
        """factory method to create a DB proxy

        Accepts all arguments of psycopg2.connect; only some specific
        possibilities are reported below.

        Args:
            connstring: libpq2 connection string

        """
        conn = psycopg2.connect(*args, **kwargs)
        return cls(conn)

    def _cursor(self, cur_arg):
        """get a cursor: from cur_arg if given, or a fresh one otherwise

        meant to avoid boilerplate if/then/else in methods that proxy stored
        procedures

        """
        if cur_arg is not None:
            return cur_arg
        # elif self.cur is not None:
        #     return self.cur
        else:
            return self.conn.cursor()

    def __init__(self, conn):
        """create a DB proxy

        Args:
            conn: psycopg2 connection to the SWH DB

        """
        self.conn = conn

    @contextmanager
    def transaction(self):
        """context manager to execute within a DB transaction

        Yields:
            a psycopg2 cursor

        """
        with self.conn.cursor() as cur:
            try:
                yield cur
                self.conn.commit()
            except:
                if not self.conn.closed:
                    self.conn.rollback()
                raise

    def copy_to(self, items, tblname, columns, cur=None, item_cb=None):
        """Copy items' entries to table tblname with columns information.

        Args:
            items (dict): dictionary of data to copy over tblname
            tblname (str): Destination table's name
            columns ([str]): keys to access data in items and also the
              column names in the destination table.
            item_cb (fn): optional function to apply to items's entry

        """
        def escape(data):
            if data is None:
                return ''
            if isinstance(data, bytes):
                return '\\x%s' % binascii.hexlify(data).decode('ascii')
            elif isinstance(data, str):
                return '"%s"' % data.replace('"', '""')
            elif isinstance(data, datetime.datetime):
                # We escape twice to make sure the string generated by
                # isoformat gets escaped
                return escape(data.isoformat())
            elif isinstance(data, dict):
                return escape(json.dumps(data))
            elif isinstance(data, list):
                return escape("{%s}" % ','.join(escape(d) for d in data))
            elif isinstance(data, psycopg2.extras.Range):
                # We escape twice here too, so that we make sure
                # everything gets passed to copy properly
                return escape(
                    '%s%s,%s%s' % (
                        '[' if data.lower_inc else '(',
                        '-infinity' if data.lower_inf else escape(data.lower),
                        'infinity' if data.upper_inf else escape(data.upper),
                        ']' if data.upper_inc else ')',
                    )
                )
            elif isinstance(data, enum.IntEnum):
                return escape(int(data))
            else:
                # We don't escape here to make sure we pass literals properly
                return str(data)
        with tempfile.TemporaryFile('w+') as f:
            for d in items:
                if item_cb is not None:
                    item_cb(d)
                line = [escape(d.get(k)) for k in columns]
                f.write(','.join(line))
                f.write('\n')
            f.seek(0)
            self._cursor(cur).copy_expert('COPY %s (%s) FROM STDIN CSV' % (
                tblname, ', '.join(columns)), f)

    def mktemp(self, tblname, cur=None):
        self._cursor(cur).execute('SELECT swh_mktemp(%s)', (tblname,))


class Db(BaseDb):
    """Proxy to the SWH DB, with wrappers around stored procedures

    """
    def mktemp_dir_entry(self, entry_type, cur=None):
        self._cursor(cur).execute('SELECT swh_mktemp_dir_entry(%s)',
                                  (('directory_entry_%s' % entry_type),))

    @stored_procedure('swh_mktemp_revision')
    def mktemp_revision(self, cur=None): pass

    @stored_procedure('swh_mktemp_release')
    def mktemp_release(self, cur=None): pass

    @stored_procedure('swh_mktemp_occurrence_history')
    def mktemp_occurrence_history(self, cur=None): pass

    @stored_procedure('swh_mktemp_entity_lister')
    def mktemp_entity_lister(self, cur=None): pass

    @stored_procedure('swh_mktemp_entity_history')
    def mktemp_entity_history(self, cur=None): pass

    @stored_procedure('swh_mktemp_bytea')
    def mktemp_bytea(self, cur=None): pass

    @stored_procedure('swh_mktemp_content_ctags')
    def mktemp_content_ctags(self, cur=None): pass

    @stored_procedure('swh_mktemp_content_ctags_missing')
    def mktemp_content_ctags_missing(self, cur=None): pass

    def register_listener(self, notify_queue, cur=None):
        """Register a listener for NOTIFY queue `notify_queue`"""
        self._cursor(cur).execute("LISTEN %s" % notify_queue)

    def listen_notifies(self, timeout):
        """Listen to notifications for `timeout` seconds"""
        if select.select([self.conn], [], [], timeout) == ([], [], []):
            return
        else:
            self.conn.poll()
            while self.conn.notifies:
                yield self.conn.notifies.pop(0)

    @stored_procedure('swh_content_add')
    def content_add_from_temp(self, cur=None): pass

    @stored_procedure('swh_directory_add')
    def directory_add_from_temp(self, cur=None): pass

    @stored_procedure('swh_skipped_content_add')
    def skipped_content_add_from_temp(self, cur=None): pass

    @stored_procedure('swh_revision_add')
    def revision_add_from_temp(self, cur=None): pass

    @stored_procedure('swh_release_add')
    def release_add_from_temp(self, cur=None): pass

    @stored_procedure('swh_occurrence_history_add')
    def occurrence_history_add_from_temp(self, cur=None): pass

    @stored_procedure('swh_entity_history_add')
    def entity_history_add_from_temp(self, cur=None): pass

    def store_tmp_bytea(self, ids, cur=None):
        """Store the given identifiers in a new tmp_bytea table"""
        cur = self._cursor(cur)

        self.mktemp_bytea(cur)
        self.copy_to(({'id': elem} for elem in ids), 'tmp_bytea',
                     ['id'], cur)

    def content_update_from_temp(self, keys_to_update, cur=None):
        cur = self._cursor(cur)
        cur.execute("""select swh_content_update(ARRAY[%s] :: text[])""" %
                    keys_to_update)

    content_get_metadata_keys = [
        'sha1', 'sha1_git', 'sha256', 'blake2s256', 'length', 'status']

    skipped_content_keys = [
        'sha1', 'sha1_git', 'sha256', 'blake2s256',
        'length', 'reason', 'status', 'origin']

    def content_get_metadata_from_temp(self, cur=None):
        cur = self._cursor(cur)
        cur.execute("""select t.id as sha1, %s from tmp_bytea t
                       left join content on t.id = content.sha1
                    """ % ', '.join(self.content_get_metadata_keys[1:]))

        yield from cursor_to_bytes(cur)

    def content_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)

        cur.execute("""SELECT sha1, sha1_git, sha256, blake2s256
                       FROM swh_content_missing()""")

        yield from cursor_to_bytes(cur)

    def content_missing_per_sha1_from_temp(self, cur=None):
        cur = self._cursor(cur)

        cur.execute("""SELECT *
                       FROM swh_content_missing_per_sha1()""")

        yield from cursor_to_bytes(cur)

    def skipped_content_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)

        cur.execute("""SELECT sha1, sha1_git, sha256, blake2s256
                       FROM swh_skipped_content_missing()""")

        yield from cursor_to_bytes(cur)

    def occurrence_get(self, origin_id, cur=None):
        """Retrieve latest occurrence's information by origin_id.

        """
        cur = self._cursor(cur)

        cur.execute("""SELECT origin, branch, target, target_type,
                              (select max(date) from origin_visit
                               where origin=%s) as date
                       FROM occurrence
                       WHERE origin=%s
                    """,
                    (origin_id, origin_id))

        yield from cursor_to_bytes(cur)

    content_find_cols = ['sha1', 'sha1_git', 'sha256', 'blake2s256', 'length',
                         'ctime', 'status']

    def content_find(self, sha1=None, sha1_git=None, sha256=None,
                     blake2s256=None, cur=None):
        """Find the content optionally on a combination of the following
        checksums sha1, sha1_git, sha256 or blake2s256.

        Args:
            sha1: sha1 content
            git_sha1: the sha1 computed `a la git` sha1 of the content
            sha256: sha256 content
            blake2s256: blake2s256 content

        Returns:
            The tuple (sha1, sha1_git, sha256, blake2s256) if found or None.

        """
        cur = self._cursor(cur)

        cur.execute("""SELECT %s
                       FROM swh_content_find(%%s, %%s, %%s, %%s)
                       LIMIT 1""" % ','.join(self.content_find_cols),
                    (sha1, sha1_git, sha256, blake2s256))

        content = line_to_bytes(cur.fetchone())
        if set(content) == {None}:
            return None
        else:
            return content

    def directory_get_from_temp(self, cur=None):
        cur = self._cursor(cur)
        cur.execute('''SELECT id, file_entries, dir_entries, rev_entries
                       FROM swh_directory_get()''')
        yield from cursor_to_bytes(cur)

    def directory_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)
        cur.execute('SELECT * FROM swh_directory_missing()')
        yield from cursor_to_bytes(cur)

    directory_ls_cols = ['dir_id', 'type', 'target', 'name', 'perms',
                         'status', 'sha1', 'sha1_git', 'sha256', 'length']

    def directory_walk_one(self, directory, cur=None):
        cur = self._cursor(cur)
        cols = ', '.join(self.directory_ls_cols)
        query = 'SELECT %s FROM swh_directory_walk_one(%%s)' % cols
        cur.execute(query, (directory,))
        yield from cursor_to_bytes(cur)

    def directory_walk(self, directory, cur=None):
        cur = self._cursor(cur)
        cols = ', '.join(self.directory_ls_cols)
        query = 'SELECT %s FROM swh_directory_walk(%%s)' % cols
        cur.execute(query, (directory,))
        yield from cursor_to_bytes(cur)

    def directory_entry_get_by_path(self, directory, paths, cur=None):
        """Retrieve a directory entry by path.

        """
        cur = self._cursor(cur)

        cols = ', '.join(self.directory_ls_cols)
        query = (
            'SELECT %s FROM swh_find_directory_entry_by_path(%%s, %%s)' % cols)
        cur.execute(query, (directory, paths))

        data = cur.fetchone()
        if set(data) == {None}:
            return None
        return line_to_bytes(data)

    def revision_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)

        cur.execute('SELECT id FROM swh_revision_missing() as r(id)')

        yield from cursor_to_bytes(cur)

    revision_add_cols = [
        'id', 'date', 'date_offset', 'date_neg_utc_offset', 'committer_date',
        'committer_date_offset', 'committer_date_neg_utc_offset', 'type',
        'directory', 'message', 'author_fullname', 'author_name',
        'author_email', 'committer_fullname', 'committer_name',
        'committer_email', 'metadata', 'synthetic',
    ]

    revision_get_cols = revision_add_cols + [
        'author_id', 'committer_id', 'parents']

    def origin_visit_add(self, origin, ts, cur=None):
        """Add a new origin_visit for origin origin at timestamp ts with
        status 'ongoing'.

        Args:
            origin: origin concerned by the visit
            ts: the date of the visit

        Returns:
            The new visit index step for that origin

        """
        cur = self._cursor(cur)
        self._cursor(cur).execute('SELECT swh_origin_visit_add(%s, %s)',
                                  (origin, ts))
        return cur.fetchone()[0]

    def origin_visit_update(self, origin, visit_id, status,
                            metadata, cur=None):
        """Update origin_visit's status."""
        cur = self._cursor(cur)
        update = """UPDATE origin_visit
                    SET status=%s, metadata=%s
                    WHERE origin=%s AND visit=%s"""
        cur.execute(update, (status, jsonize(metadata), origin, visit_id))

    origin_visit_get_cols = ['origin', 'visit', 'date', 'status', 'metadata']

    def origin_visit_get_all(self, origin_id,
                             last_visit=None, limit=None, cur=None):
        """Retrieve all visits for origin with id origin_id.

        Args:
            origin_id: The occurrence's origin

        Yields:
            The occurrence's history visits

        """
        cur = self._cursor(cur)

        query_suffix = ''
        if last_visit:
            query_suffix += ' AND %s < visit' % last_visit

        if limit:
            query_suffix += ' LIMIT %s' % limit

        query = """\
        SELECT %s
        FROM origin_visit
        WHERE origin=%%s %s""" % (
            ', '.join(self.origin_visit_get_cols), query_suffix)

        cur.execute(query, (origin_id, ))

        yield from cursor_to_bytes(cur)

    def origin_visit_get(self, origin_id, visit_id, cur=None):
        """Retrieve information on visit visit_id of origin origin_id.

        Args:
            origin_id: the origin concerned
            visit_id: The visit step for that origin

        Returns:
            The origin_visit information

        """
        cur = self._cursor(cur)

        query = """\
            SELECT %s
            FROM origin_visit
            WHERE origin = %%s AND visit = %%s
            """ % (', '.join(self.origin_visit_get_cols))

        cur.execute(query, (origin_id, visit_id))
        r = cur.fetchall()
        if not r:
            return None
        return line_to_bytes(r[0])

    occurrence_cols = ['origin', 'branch', 'target', 'target_type']

    def occurrence_by_origin_visit(self, origin_id, visit_id, cur=None):
        """Retrieve all occurrences for a particular origin_visit.

        Args:
            origin_id: the origin concerned
            visit_id: The visit step for that origin

        Yields:
            The occurrence's history visits

        """
        cur = self._cursor(cur)

        query = """\
            SELECT %s
            FROM swh_occurrence_by_origin_visit(%%s, %%s)
            """ % (', '.join(self.occurrence_cols))

        cur.execute(query, (origin_id, visit_id))
        yield from cursor_to_bytes(cur)

    def revision_get_from_temp(self, cur=None):
        cur = self._cursor(cur)
        query = 'SELECT %s FROM swh_revision_get()' % (
            ', '.join(self.revision_get_cols))
        cur.execute(query)
        yield from cursor_to_bytes(cur)

    def revision_log(self, root_revisions, limit=None, cur=None):
        cur = self._cursor(cur)

        query = """SELECT %s
                   FROM swh_revision_log(%%s, %%s)
                """ % ', '.join(self.revision_get_cols)

        cur.execute(query, (root_revisions, limit))
        yield from cursor_to_bytes(cur)

    revision_shortlog_cols = ['id', 'parents']

    def revision_shortlog(self, root_revisions, limit=None, cur=None):
        cur = self._cursor(cur)

        query = """SELECT %s
                   FROM swh_revision_list(%%s, %%s)
                """ % ', '.join(self.revision_shortlog_cols)

        cur.execute(query, (root_revisions, limit))
        yield from cursor_to_bytes(cur)

    def release_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)
        cur.execute('SELECT id FROM swh_release_missing() as r(id)')
        yield from cursor_to_bytes(cur)

    object_find_by_sha1_git_cols = ['sha1_git', 'type', 'id', 'object_id']

    def object_find_by_sha1_git(self, ids, cur=None):
        cur = self._cursor(cur)

        self.store_tmp_bytea(ids, cur)
        query = 'select %s from swh_object_find_by_sha1_git()' % (
            ', '.join(self.object_find_by_sha1_git_cols)
        )
        cur.execute(query)

        yield from cursor_to_bytes(cur)

    def stat_counters(self, cur=None):
        cur = self._cursor(cur)
        cur.execute('SELECT * FROM swh_stat_counters()')
        yield from cur

    fetch_history_cols = ['origin', 'date', 'status', 'result', 'stdout',
                          'stderr', 'duration']

    def create_fetch_history(self, fetch_history, cur=None):
        """Create a fetch_history entry with the data in fetch_history"""
        cur = self._cursor(cur)
        query = '''INSERT INTO fetch_history (%s)
                   VALUES (%s) RETURNING id''' % (
            ','.join(self.fetch_history_cols),
            ','.join(['%s'] * len(self.fetch_history_cols))
        )
        cur.execute(query, [fetch_history.get(col) for col in
                            self.fetch_history_cols])

        return cur.fetchone()[0]

    def get_fetch_history(self, fetch_history_id, cur=None):
        """Get a fetch_history entry with the given id"""
        cur = self._cursor(cur)
        query = '''SELECT %s FROM fetch_history WHERE id=%%s''' % (
            ', '.join(self.fetch_history_cols),
        )
        cur.execute(query, (fetch_history_id,))

        data = cur.fetchone()

        if not data:
            return None

        ret = {'id': fetch_history_id}
        for i, col in enumerate(self.fetch_history_cols):
            ret[col] = data[i]

        return ret

    def update_fetch_history(self, fetch_history, cur=None):
        """Update the fetch_history entry from the data in fetch_history"""
        cur = self._cursor(cur)
        query = '''UPDATE fetch_history
                   SET %s
                   WHERE id=%%s''' % (
            ','.join('%s=%%s' % col for col in self.fetch_history_cols)
        )
        cur.execute(query, [jsonize(fetch_history.get(col)) for col in
                            self.fetch_history_cols + ['id']])

    base_entity_cols = ['uuid', 'parent', 'name', 'type',
                        'description', 'homepage', 'active',
                        'generated', 'lister_metadata',
                        'metadata']

    entity_cols = base_entity_cols + ['last_seen', 'last_id']
    entity_history_cols = base_entity_cols + ['id', 'validity']

    def origin_add(self, type, url, cur=None):
        """Insert a new origin and return the new identifier."""
        insert = """INSERT INTO origin (type, url) values (%s, %s)
                    RETURNING id"""

        cur.execute(insert, (type, url))
        return cur.fetchone()[0]

    def origin_get_with(self, type, url, cur=None):
        """Retrieve the origin id from its type and url if found."""
        cur = self._cursor(cur)

        query = """SELECT id, type, url, lister, project
                   FROM origin
                   WHERE type=%s AND url=%s"""

        cur.execute(query, (type, url))
        data = cur.fetchone()
        if data:
            return line_to_bytes(data)
        return None

    def origin_get(self, id, cur=None):
        """Retrieve the origin per its identifier.

        """
        cur = self._cursor(cur)

        query = "SELECT id, type, url, lister, project FROM origin WHERE id=%s"

        cur.execute(query, (id,))
        data = cur.fetchone()
        if data:
            return line_to_bytes(data)
        return None

    person_cols = ['fullname', 'name', 'email']
    person_get_cols = person_cols + ['id']

    def person_add(self, person, cur=None):
        """Add a person identified by its name and email.

        Returns:
            The new person's id

        """
        cur = self._cursor(cur)

        query_new_person = '''\
        INSERT INTO person(%s)
        VALUES (%s)
        RETURNING id''' % (
            ', '.join(self.person_cols),
            ', '.join('%s' for i in range(len(self.person_cols)))
        )
        cur.execute(query_new_person,
                    [person[col] for col in self.person_cols])
        return cur.fetchone()[0]

    def person_get(self, ids, cur=None):
        """Retrieve the persons identified by the list of ids.

        """
        cur = self._cursor(cur)

        query = """SELECT %s
                   FROM person
                   WHERE id IN %%s""" % ', '.join(self.person_get_cols)

        cur.execute(query, (tuple(ids),))
        yield from cursor_to_bytes(cur)

    release_add_cols = [
        'id', 'target', 'target_type', 'date', 'date_offset',
        'date_neg_utc_offset', 'name', 'comment', 'synthetic',
        'author_fullname', 'author_name', 'author_email',
    ]
    release_get_cols = release_add_cols + ['author_id']

    def release_get_from_temp(self, cur=None):
        cur = self._cursor(cur)
        query = '''
        SELECT %s
            FROM swh_release_get()
        ''' % ', '.join(self.release_get_cols)
        cur.execute(query)
        yield from cursor_to_bytes(cur)

    def release_get_by(self,
                       origin_id,
                       limit=None,
                       cur=None):
        """Retrieve a release by occurrence criterion (only origin right now)

        Args:
            - origin_id: The origin to look for.

        """
        cur = self._cursor(cur)
        query = """
        SELECT %s
            FROM swh_release_get_by(%%s)
            LIMIT %%s
        """ % ', '.join(self.release_get_cols)
        cur.execute(query, (origin_id, limit))
        yield from cursor_to_bytes(cur)

    def revision_get_by(self,
                        origin_id,
                        branch_name,
                        datetime,
                        limit=None,
                        cur=None):
        """Retrieve a revision by occurrence criterion.

        Args:
            - origin_id: The origin to look for
            - branch_name: the branch name to look for
            - datetime: the lower bound of timerange to look for.
            - limit: limit number of results to return
            The upper bound being now.
        """
        cur = self._cursor(cur)
        if branch_name and isinstance(branch_name, str):
            branch_name = branch_name.encode('utf-8')

        query = '''
        SELECT %s
            FROM swh_revision_get_by(%%s, %%s, %%s)
            LIMIT %%s
        ''' % ', '.join(self.revision_get_cols)

        cur.execute(query, (origin_id, branch_name, datetime, limit))
        yield from cursor_to_bytes(cur)

    def entity_get(self, uuid, cur=None):
        """Retrieve the entity and its parent hierarchy chain per uuid.

        """
        cur = self._cursor(cur)
        cur.execute("""SELECT %s
                       FROM swh_entity_get(%%s)""" % (
                           ', '.join(self.entity_cols)),
                    (uuid, ))
        yield from cursor_to_bytes(cur)

    def entity_get_one(self, uuid, cur=None):
        """Retrieve a single entity given its uuid.

        """
        cur = self._cursor(cur)
        cur.execute("""SELECT %s
                       FROM entity
                       WHERE uuid = %%s""" % (
                           ', '.join(self.entity_cols)),
                    (uuid, ))
        data = cur.fetchone()
        if not data:
            return None
        return line_to_bytes(data)

    content_mimetype_cols = [
        'id', 'mimetype', 'encoding',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    @stored_procedure('swh_mktemp_content_mimetype_missing')
    def mktemp_content_mimetype_missing(self, cur=None): pass

    def content_mimetype_missing_from_temp(self, cur=None):
        """List missing mimetypes.

        """
        cur = self._cursor(cur)
        cur.execute("SELECT * FROM swh_content_mimetype_missing()")
        yield from cursor_to_bytes(cur)

    @stored_procedure('swh_mktemp_content_mimetype')
    def mktemp_content_mimetype(self, cur=None): pass

    def content_mimetype_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_content_mimetype_add(%s)",
                                  (conflict_update, ))

    def content_mimetype_get_from_temp(self, cur=None):
        cur = self._cursor(cur)
        query = "SELECT %s FROM swh_content_mimetype_get()" % (
            ','.join(self.content_mimetype_cols))
        cur.execute(query)
        yield from cursor_to_bytes(cur)

    content_language_cols = [
        'id', 'lang',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    @stored_procedure('swh_mktemp_content_language')
    def mktemp_content_language(self, cur=None): pass

    @stored_procedure('swh_mktemp_content_language_missing')
    def mktemp_content_language_missing(self, cur=None): pass

    def content_language_missing_from_temp(self, cur=None):
        """List missing languages.

        """
        cur = self._cursor(cur)
        cur.execute("SELECT * FROM swh_content_language_missing()")
        yield from cursor_to_bytes(cur)

    def content_language_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_content_language_add(%s)",
                                  (conflict_update, ))

    def content_language_get_from_temp(self, cur=None):
        cur = self._cursor(cur)
        query = "SELECT %s FROM swh_content_language_get()" % (
            ','.join(self.content_language_cols))
        cur.execute(query)
        yield from cursor_to_bytes(cur)

    content_ctags_cols = [
        'id', 'name', 'kind', 'line', 'lang',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    def content_ctags_missing_from_temp(self, cur=None):
        """List missing ctags.

        """
        cur = self._cursor(cur)
        cur.execute("SELECT * FROM swh_content_ctags_missing()")
        yield from cursor_to_bytes(cur)

    def content_ctags_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_content_ctags_add(%s)",
                                  (conflict_update, ))

    def content_ctags_get_from_temp(self, cur=None):
        cur = self._cursor(cur)
        query = "SELECT %s FROM swh_content_ctags_get()" % (
            ','.join(self.content_ctags_cols))
        cur.execute(query)
        yield from cursor_to_bytes(cur)

    def content_ctags_search(self, expression, last_sha1, limit, cur=None):
        cur = self._cursor(cur)
        if not last_sha1:
            query = """SELECT %s
                       FROM swh_content_ctags_search(%%s, %%s)""" % (
                           ','.join(self.content_ctags_cols))
            cur.execute(query, (expression, limit))
        else:
            if last_sha1 and isinstance(last_sha1, bytes):
                last_sha1 = '\\x%s' % hashutil.hash_to_hex(last_sha1)
            elif last_sha1:
                last_sha1 = '\\x%s' % last_sha1

            query = """SELECT %s
                       FROM swh_content_ctags_search(%%s, %%s, %%s)""" % (
                           ','.join(self.content_ctags_cols))
            cur.execute(query, (expression, limit, last_sha1))

        yield from cursor_to_bytes(cur)

    content_fossology_license_cols = [
        'id', 'tool_id', 'tool_name', 'tool_version', 'tool_configuration',
        'licenses']

    @stored_procedure('swh_mktemp_content_fossology_license')
    def mktemp_content_fossology_license(self, cur=None): pass

    def content_fossology_license_add_from_temp(self, conflict_update,
                                                cur=None):
        """Add new licenses per content.

        """
        self._cursor(cur).execute(
            "SELECT swh_content_fossology_license_add(%s)",
            (conflict_update, ))

    def content_fossology_license_get_from_temp(self, cur=None):
        """Retrieve licenses per content.

        """
        cur = self._cursor(cur)
        query = "SELECT %s FROM swh_content_fossology_license_get()" % (
            ','.join(self.content_fossology_license_cols))
        cur.execute(query)
        yield from cursor_to_bytes(cur)

    content_metadata_cols = [
        'id', 'translated_metadata',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    @stored_procedure('swh_mktemp_content_metadata')
    def mktemp_content_metadata(self, cur=None): pass

    @stored_procedure('swh_mktemp_content_metadata_missing')
    def mktemp_content_metadata_missing(self, cur=None): pass

    def content_metadata_missing_from_temp(self, cur=None):
        """List missing metadatas.

        """
        cur = self._cursor(cur)
        cur.execute("SELECT * FROM swh_content_metadata_missing()")
        yield from cursor_to_bytes(cur)

    def content_metadata_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_content_metadata_add(%s)",
                                  (conflict_update, ))

    def content_metadata_get_from_temp(self, cur=None):
        cur = self._cursor(cur)
        query = "SELECT %s FROM swh_content_metadata_get()" % (
            ','.join(self.content_metadata_cols))
        cur.execute(query)
        yield from cursor_to_bytes(cur)

    revision_metadata_cols = [
        'id', 'translated_metadata',
        'tool_id', 'tool_name', 'tool_version', 'tool_configuration']

    @stored_procedure('swh_mktemp_revision_metadata')
    def mktemp_revision_metadata(self, cur=None): pass

    @stored_procedure('swh_mktemp_revision_metadata_missing')
    def mktemp_revision_metadata_missing(self, cur=None): pass

    def revision_metadata_missing_from_temp(self, cur=None):
        """List missing metadatas.

        """
        cur = self._cursor(cur)
        cur.execute("SELECT * FROM swh_revision_metadata_missing()")
        yield from cursor_to_bytes(cur)

    def revision_metadata_add_from_temp(self, conflict_update, cur=None):
        self._cursor(cur).execute("SELECT swh_revision_metadata_add(%s)",
                                  (conflict_update, ))

    def revision_metadata_get_from_temp(self, cur=None):
        cur = self._cursor(cur)
        query = "SELECT %s FROM swh_revision_metadata_get()" % (
            ','.join(self.revision_metadata_cols))
        cur.execute(query)
        yield from cursor_to_bytes(cur)

    def origin_metadata_add(self, origin, ts, provider, tool,
                            metadata, cur=None):
        """ Add an origin_metadata for the origin at ts with provider, tool and
        metadata.

        Args:
            origin (int): the origin's id for which the metadata is added
            ts (datetime): time when the metadata was found
            provider (int): the metadata provider identifier
            tool (int): the tool's identifier used to extract metadata
            metadata (jsonb): the metadata retrieved at the time and location

        Returns:
            id (int): the origin_metadata unique id

        """
        cur = self._cursor(cur)
        insert = """INSERT INTO origin_metadata (origin_id, discovery_date,
                    provider_id, tool_id, metadata) values (%s, %s, %s, %s, %s)
                    RETURNING id"""
        cur.execute(insert, (origin, ts, provider, tool, jsonize(metadata)))

        return cur.fetchone()[0]

    origin_metadata_get_cols = ['id', 'origin_id', 'discovery_date',
                                'tool_id', 'metadata', 'provider_id',
                                'provider_name', 'provider_type',
                                'provider_url']

    def origin_metadata_get_by(self, origin_id, provider_type=None, cur=None):
        """Retrieve all origin_metadata entries for one origin_id

        """
        cur = self._cursor(cur)
        if not provider_type:
            query = '''SELECT %s
                       FROM swh_origin_metadata_get_by_origin(
                            %%s)''' % (','.join(
                                          self.origin_metadata_get_cols))

            cur.execute(query, (origin_id, ))

        else:
            query = '''SELECT %s
                       FROM swh_origin_metadata_get_by_provider_type(
                            %%s, %%s)''' % (','.join(
                                          self.origin_metadata_get_cols))

            cur.execute(query, (origin_id, provider_type))

        yield from cursor_to_bytes(cur)

    indexer_configuration_cols = ['id', 'tool_name', 'tool_version',
                                  'tool_configuration']

    def indexer_configuration_get(self, tool_name,
                                  tool_version, tool_configuration, cur=None):
        cur = self._cursor(cur)
        cur.execute('''select %s
                       from indexer_configuration
                       where tool_name=%%s and
                             tool_version=%%s and
                             tool_configuration=%%s''' % (
                                 ','.join(self.indexer_configuration_cols)),
                    (tool_name, tool_version, tool_configuration))

        data = cur.fetchone()
        if not data:
            return None
        return line_to_bytes(data)

    metadata_provider_cols = ['id', 'provider_name', 'provider_type',
                              'provider_url', 'metadata']

    def metadata_provider_add(self, provider_name, provider_type,
                              provider_url, metadata, cur=None):
        """Insert a new provider and return the new identifier."""
        cur = self._cursor(cur)
        insert = """INSERT INTO metadata_provider (provider_name, provider_type,
                    provider_url, metadata) values (%s, %s, %s, %s)
                    RETURNING id"""

        cur.execute(insert, (provider_name, provider_type, provider_url,
                    jsonize(metadata)))
        return cur.fetchone()[0]

    def metadata_provider_get(self, provider_id, cur=None):
        cur = self._cursor(cur)
        cur.execute('''select %s
                       from metadata_provider
                       where provider_id=%%s ''' % (
                                 ','.join(self.metadata_provider_cols)),
                    (provider_id, ))

        data = cur.fetchone()
        if not data:
            return None
        return line_to_bytes(data)

    def metadata_provider_get_by(self, provider_name, provider_url,
                                 cur=None):
        cur = self._cursor(cur)
        cur.execute('''select %s
                       from metadata_provider
                       where provider_name=%%s and
                             provider_url=%%s''' % (
                                 ','.join(self.metadata_provider_cols)),
                    (provider_name, provider_url))

        data = cur.fetchone()
        if not data:
            return None
        return line_to_bytes(data)
