# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2

from contextlib import contextmanager

TMP_CONTENT_TABLE = 'tmp_content'


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

    # @stored_procedure('swh_content_mktemp')
    def content_mktemp(self, cur=None):
        self._cursor(cur).execute('SELECT swh_content_mktemp()')

    def content_copy_to_temp(self, fileobj, cur=None):
        self._cursor(cur) \
            .copy_from(fileobj, TMP_CONTENT_TABLE,
                       columns=('sha1', 'sha1_git', 'sha256', 'length'))

    # @stored_procedure('swh_content_add')
    def content_add_from_temp(self, cur):
        self._cursor(cur).execute('SELECT swh_content_add()')
