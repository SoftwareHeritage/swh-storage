# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools


def db_transaction(meth):
    """decorator to execute Storage methods within DB transactions

    The decorated method must accept a `cur` keyword argument
    """
    @functools.wraps(meth)
    def _meth(self, *args, **kwargs):
        if 'cur' in kwargs and kwargs['cur']:
            return meth(self, *args, **kwargs)
        else:
            with self.db.transaction() as cur:
                return meth(self, *args, cur=cur, **kwargs)
    return _meth


def db_transaction_generator(meth):
    """decorator to execute Storage methods within DB transactions, while
    returning a generator

    The decorated method must accept a `cur` keyword argument

    """
    @functools.wraps(meth)
    def _meth(self, *args, **kwargs):
        if 'cur' in kwargs and kwargs['cur']:
            yield from meth(self, *args, **kwargs)
        else:
            with self.db.transaction() as cur:
                yield from meth(self, *args, cur=cur, **kwargs)
    return _meth
