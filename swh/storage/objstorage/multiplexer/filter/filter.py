# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from ...objstorage import ObjStorage


class ObjStorageFilter(ObjStorage):
    """ Base implementation of a filter that allow inputs on ObjStorage or not

    This class copy the API of ...objstorage in order to filter the inputs
    of this class.
    If the operation is allowed, return the result of this operation
    applied to the destination implementation. Otherwise, just return
    without any operation.

    This class is an abstract base class for a classic read/write storage.
    Filters can inherit from it and only redefine some methods in order
    to change behavior.
    """

    def __init__(self, storage):
        self.storage = storage

    def __contains__(self, *args, **kwargs):
        return self.storage.__contains__(*args, **kwargs)

    def __iter__(self):
        return self.storage.__iter__()

    def __len__(self):
        return self.storage.__len__()

    def add(self, *args, **kwargs):
        return self.storage.add(*args, **kwargs)

    def restore(self, *args, **kwargs):
        return self.storage.restore(*args, **kwargs)

    def get(self, *args, **kwargs):
        return self.storage.get(*args, **kwargs)

    def check(self, *args, **kwargs):
        return self.storage.check(*args, **kwargs)

    def get_random(self, *args, **kwargs):
        return self.storage.get_random(*args, **kwargs)
