# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import inspect
import functools


def apply_options(cursor, options):
    """Applies the given postgresql client options to the given cursor.

    Returns a dictionary with the old values if they changed."""
    old_options = {}
    for option, value in options.items():
        cursor.execute('SHOW %s' % option)
        old_value = cursor.fetchall()[0][0]
        if old_value != value:
            cursor.execute('SET LOCAL %s TO %%s' % option, (value,))
            old_options[option] = old_value
    return old_options


def db_transaction(**client_options):
    """decorator to execute Storage methods within DB transactions

    The decorated method must accept a `cur` and `db` keyword argument

    Client options are passed as `set` options to the postgresql server
    """
    def decorator(meth, __client_options=client_options):
        if inspect.isgeneratorfunction(meth):
            raise ValueError(
                    'Use db_transaction_generator for generator functions.')

        @functools.wraps(meth)
        def _meth(self, *args, **kwargs):
            if 'cur' in kwargs and kwargs['cur']:
                cur = kwargs['cur']
                old_options = apply_options(cur, __client_options)
                ret = meth(self, *args, **kwargs)
                apply_options(cur, old_options)
                return ret
            else:
                db = self.get_db()
                with db.transaction() as cur:
                    apply_options(cur, __client_options)
                    return meth(self, *args, db=db, cur=cur, **kwargs)
        return _meth

    return decorator


def db_transaction_generator(**client_options):
    """decorator to execute Storage methods within DB transactions, while
    returning a generator

    The decorated method must accept a `cur` and `db` keyword argument

    Client options are passed as `set` options to the postgresql server
    """
    def decorator(meth, __client_options=client_options):
        if not inspect.isgeneratorfunction(meth):
            raise ValueError(
                    'Use db_transaction for non-generator functions.')

        @functools.wraps(meth)
        def _meth(self, *args, **kwargs):
            if 'cur' in kwargs and kwargs['cur']:
                cur = kwargs['cur']
                old_options = apply_options(cur, __client_options)
                yield from meth(self, *args, **kwargs)
                apply_options(cur, old_options)
            else:
                db = self.get_db()
                with db.transaction() as cur:
                    apply_options(cur, __client_options)
                    yield from meth(self, *args, db=db, cur=cur, **kwargs)
        return _meth
    return decorator
