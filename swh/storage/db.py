# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2


class Db:
    """DB proxy

    """

    def __init__(self, connstring):
        """create a DB proxy, connecting to the DB

        Args:
            connstring: libpq connection string
        """
        self.conn = psycopg2.connect(connstring)

    def cursor(self):
        return self.conn.cursor()

    def commit(self):
        return self.conn.commit()

    def rollback(self):
        return self.conn.rollback()
