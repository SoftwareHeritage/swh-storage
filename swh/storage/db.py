# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import binascii
import datetime
import functools
import json
import psycopg2
import tempfile

from contextlib import contextmanager

TMP_CONTENT_TABLE = 'tmp_content'


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
    return line.__class__(entry_to_bytes(entry) for entry in line)


def cursor_to_bytes(cursor):
    """Yield all the data from a cursor as bytes"""
    yield from (line_to_bytes(line) for line in cursor)


class Db:
    """Proxy to the SWH DB, with wrappers around stored procedures

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

    def mktemp(self, tblname, cur=None):
        self._cursor(cur).execute('SELECT swh_mktemp(%s)', (tblname,))

    def mktemp_dir_entry(self, entry_type, cur=None):
        self._cursor(cur).execute('SELECT swh_mktemp_dir_entry(%s)',
                                  (('directory_entry_%s' % entry_type),))

    @stored_procedure('swh_mktemp_revision')
    def mktemp_revision(self, cur=None): pass

    @stored_procedure('swh_mktemp_release')
    def mktemp_release(self, cur=None): pass

    @stored_procedure('swh_mktemp_entity_lister')
    def mktemp_entity_lister(self, cur=None): pass

    @stored_procedure('swh_mktemp_entity_history')
    def mktemp_entity_history(self, cur=None): pass

    def copy_to(self, items, tblname, columns, cur=None, item_cb=None):
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

    def content_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)

        cur.execute("""SELECT sha1, sha1_git, sha256
                       FROM swh_content_missing()""")

        yield from cursor_to_bytes(cur)

    def skipped_content_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)

        cur.execute("""SELECT sha1, sha1_git, sha256
                       FROM swh_skipped_content_missing()""")

        yield from cursor_to_bytes(cur)

    def content_find(self, sha1=None, sha1_git=None, sha256=None, cur=None):
        """Find the content optionally on a combination of the following
        checksums sha1, sha1_git or sha256.

        Args:
            sha1: sha1 content
            git_sha1: the sha1 computed `a la git` sha1 of the content
            sha256: sha256 content

        Returns:
            The triplet (sha1, sha1_git, sha256) if found or None.

        """
        cur = self._cursor(cur)

        cur.execute("""SELECT sha1, sha1_git, sha256
                       FROM swh_content_find(%s, %s, %s)
                       LIMIT 1""", (sha1, sha1_git, sha256))

        content = line_to_bytes(cur.fetchone())
        return None if content == (None, None, None) else content

    def content_find_occurrence(self, sha1, cur=None):
        """Find one content's occurrence.

        Args:
            sha1: sha1 content
            cur: cursor to use

        Returns:
            One occurrence for that particular sha1

        """
        cur = self._cursor(cur)

        cur.execute("""SELECT *
                       FROM swh_content_find_occurrence(%s)
                       LIMIT 1""",
                    (sha1, ))

        return line_to_bytes(cur.fetchone())

    def directory_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)
        cur.execute('SELECT * FROM swh_directory_missing()')
        yield from cursor_to_bytes(cur)

    def directory_walk_one(self, directory, cur=None):
        cur = self._cursor(cur)
        cur.execute('SELECT * FROM swh_directory_walk_one(%s)', (directory,))
        yield from cursor_to_bytes(cur)

    def directory_walk(self, directory, cur=None):
        cur = self._cursor(cur)
        cur.execute('SELECT * FROM swh_directory_walk(%s)', (directory,))
        yield from cursor_to_bytes(cur)

    def revision_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)

        cur.execute('SELECT id FROM swh_revision_missing() as r(id)')

        yield from cursor_to_bytes(cur)

    def revision_get_from_temp(self, cur=None):
        cur = self._cursor(cur)
        cur.execute('SELECT * FROM swh_revision_get()')
        yield from cursor_to_bytes(cur)

    def release_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)
        cur.execute('SELECT id FROM swh_release_missing() as r(id)')
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
                        'generated', 'lister', 'lister_metadata',
                        'doap']

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

    def release_get(self, sha1s, cur=None):
        """Retrieve the releases from their sha1.

        Args:
            - sha1s: sha1s (as bytes) list

        Yields:
            Releases as tuples id, revision, date, date_offset, name, comment,
            author, synthetic

        """
        def escape(data):
            if isinstance(data, bytes):
                return '\\x%s' % binascii.hexlify(data).decode('ascii')
            return data

        cur = self._cursor(cur)

        query = """SELECT id, revision, date, date_offset, name, comment,
                          author, synthetic
                   FROM release
                   WHERE id IN %s"""
        cur.execute(query, (tuple(map(escape, sha1s)), ))

        yield from cursor_to_bytes(cur)
