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

    def copy_to(self, items, tblname, columns, cur=None, item_cb=None):
        def escape(data):
            if isinstance(data, bytes):
                return '\\\\x%s' % binascii.hexlify(data).decode('ascii')
            else:
                return str(data)
        with tempfile.TemporaryFile('w+') as f:
            for d in items:
                if item_cb is not None:
                    item_cb(d)
                line = '\t'.join([escape(d[k]) for k in columns]) + '\n'
                f.write(line)
            f.seek(0)
            self._cursor(cur).copy_from(f, tblname, columns=columns)

    @stored_procedure('swh_content_add')
    def content_add_from_temp(self, cur=None): pass

    def content_missing_from_temp(self, cur=None):
        cur = self._cursor(cur)

        cur.execute("""SELECT sha1, sha1_git, sha256
                       FROM swh_content_missing()""")

        yield from cur
