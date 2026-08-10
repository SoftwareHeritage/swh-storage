.. _swh-storage-cassandra-migrations:

Cassandra migrations
====================

While swh-storage's PostgreSQL backend relies on the generic ``swh db`` CLI provided by
:ref:`swh-core` to manage migrations, its Cassandra backend uses a specific design and CLI.


Migration model
---------------

Due to the potentially long runtime (tables need to be rewritten to apply some changes),
and non-transactionality of migrations in Cassandra, we model migrations as a DAG instead
of a sequence of updates. This allows, for example, running two migrations in parallel,
as long as they don't depend on each other.

There are two types of migrations: those that can be done automatically (and are implemented
in Python), and those that need to be performed manually (for example, by setting up a
:ref:`replayer <architecture-overview>`, to write the content of a new table from the
:ref:`journal <swh-journal>`).

Each migration has the following properties:

``id``
    A unique human-readable name, starting with the date it was written.
    The date does not have any impact in the order migrations are applied.

``dependencies``
    A set of migration ids that need to be applied before this migration can start

``min_read_version``
    What minimum version of swh-storage can read the database after this migration
    started (or after it is completed).

``status``
    a string among ``pending``, ``running``, or ``completed``


Version checking
----------------

swh-storage's :meth:`check_config <swh.storage.interface.StorageInterface.check_config`
returns :const:`False` and prints a warning if its database meets any of the following conditions:

* any migration with status ``running`` or ``completed`` has a ``min_read_version`` strictly
  newer than the current swh-storage version, meaning the Python code was rolled back and is
  no longer able to read the database
* any migration it considers "required" does not have status ``completed``. This means the database
  needs to be upgraded (or its current upgrades must be finished), as the Python code no longer
  supports this old version of the database

In order to avoid long downtimes, this means that updates that need a long-running migrations
happen this way:

1. Version ``N`` is released, which adds a new migration ``X`` required by version ``N`` but
   still compatible with version ``N-1``; and optionally a new migration ``Y`` which is **not** required
   by version ``N`` and may not be compatible with version ``N-1``
2. Some database clients can be updated to ``N``; migration ``X`` is applied
3. Remaining database clients must be updated to ``N``
4. Migration ``Y`` is applied
5. Version ``N+1`` is released, which removes support for databases without migration ``X``, or even ``Y``
6. Database clients can be updated to ``N+1``

Operations
----------

``swh storage cassandra init``
    Creates a new keyspace, types and tables, and marks all migrations known by the current
    Python code as completed.

``swh storage cassandra list-migrations``
    Prints all migrations known to the Python code and their current status.
    In case the database has a migration applied that is not known to the Python code
    (ie. the Python code was rolled back), it is not displayed.

``swh storage cassandra upgrade``
    Applies one or more migrations. If some migrations cannot run automatically, documentation for
    how to run the migration is displayed to the user, and further migrations are not applied.

    This can be run automatically before deploying a new version of the swh-storage RPC server.
    If the return code is 0, 3, or 5, then the RPC server can safely be started.
    Otherwise, this requires operator attention.
    See the `swh storage cassandra upgrade <https://docs.softwareheritage.org/devel/swh-storage/cli.html#swh-storage-cassandra-upgrade>`_
    documentation for details.

``swh storage cassandra mark-upgraded``
    Tells the database a migration was applied. This is typically used after manually applying
    a migration, as ``swh storage cassandra upgrade`` does it on its own for automated migrations.
