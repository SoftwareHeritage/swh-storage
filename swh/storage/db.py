# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import binascii
import functools
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


def entry_to_bytes(entry):
    """Convert an entry coming from the database to bytes"""
    if isinstance(entry, memoryview):
        return entry.tobytes()
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

    def copy_to(self, items, tblname, columns, cur=None, item_cb=None):
        def escape(data):
            if data is None:
                return ''
            if isinstance(data, bytes):
                return '\\x%s' % binascii.hexlify(data).decode('ascii')
            elif isinstance(data, str):
                return '"%s"' % data.replace('"', '""')
            else:
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

    @stored_procedure('swh_revision_add')
    def revision_add_from_temp(self, cur=None): pass

    @stored_procedure('swh_release_add')
    def release_add_from_temp(self, cur=None): pass

    def content_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)

        cur.execute("""SELECT sha1, sha1_git, sha256
                       FROM swh_content_missing()""")

        yield from cursor_to_bytes(cur)

    def content_present(self, column_key, hash, cur=None):
        cur = self._cursor(cur)

        escaped_query = """SELECT {0}
                           FROM content
                           WHERE {0}=%s
                           LIMIT 1""".format(column_key)
        cur.execute(escaped_query, (hash, ))

        yield from cursor_to_bytes(cur)

    def directory_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)

        cur.execute('SELECT id FROM swh_directory_missing()')

        yield from cursor_to_bytes(cur)

    def directory_walk_one(self, directory, cur=None):
        cur = self._cursor(cur)

        cur.execute('SELECT * FROM swh_directory_walk_one(%s)', (directory,))

        yield from cursor_to_bytes(cur)

    def revision_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)

        cur.execute('SELECT id FROM swh_revision_missing() as r(id)')

        yield from cursor_to_bytes(cur)

    def release_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)

        cur.execute('SELECT id FROM swh_release_missing() as r(id)')

        yield from cursor_to_bytes(cur)
