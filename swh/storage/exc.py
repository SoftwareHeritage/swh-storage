# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


class Error(Exception):

    def __str__(self):
        return 'storage error on object: %s' % self.args


class ObjNotFoundError(Error):

    def __str__(self):
        return 'object not found: %s' % self.args


class StorageBackendError(Exception):

    def __str__(self):
        return 'An unexpected error occurred in the backend: %s' % self.args
