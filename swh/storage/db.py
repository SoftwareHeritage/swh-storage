# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2


class Db:
    """DB proxy

    """

    def __init__(self, conn):
        """create a DB proxy, connecting to the DB

        Args:
            conn: either a libpq connection string, or an established psycopg2
                connection to the SWH DB
        """
        if isinstance(conn, psycopg2.extensions.connection):
            self.conn = conn
        else:
            self.conn = psycopg2.connect(conn)

    def cursor(self):
        return self.conn.cursor()

    def commit(self):
        return self.conn.commit()

    def rollback(self):
        return self.conn.rollback()
