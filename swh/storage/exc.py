# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


class StorageDBError(Exception):
    """Specific storage db error (connection, erroneous queries, etc...)

    """

    def __str__(self):
        return 'An unexpected error occurred in the backend: %s' % self.args


class StorageAPIError(Exception):
    """Specific internal storage api (mainly connection)

    """

    def __str__(self):
        args = self.args
        return 'An unexpected error occurred in the api backend: %s' % args
