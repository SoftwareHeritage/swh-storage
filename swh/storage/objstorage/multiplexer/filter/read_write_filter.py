# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from .filter import ObjStorageFilter


class ReadObjStorageFilter(ObjStorageFilter):
    """ Filter that disable write operation of the storage.
    """

    def add(self, *args, **kwargs):
        return

    def restore(self, *args, **kwargs):
        return
