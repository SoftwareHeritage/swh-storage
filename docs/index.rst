.. _swh-storage:

.. include:: README.rst

The Software Heritage storage consist of a high-level storage layer
(:mod:`swh.storage`) that exposes a client/server API
(:mod:`swh.storage.api`). The API is exposed by a server
(:mod:`swh.storage.api.server`) and accessible via a client
(:mod:`swh.storage.api.client`).

The low-level implementation of the storage is split between an object storage
(:ref:`swh.objstorage <swh-objstorage>`), which stores all "blobs" (i.e., the
leaves of the :ref:`data-model`) and a SQL representation of the rest of the
graph (:mod:`swh.storage.storage`).


Using ``swh-storage``
---------------------

First, note that ``swh-storage`` is an internal API of Software Heritage, that
is only available to software running on the SWH infrastructure and developers
:ref:`running their own Software Heritage <getting-started>`.
If you want to access the Software Heritage archive without running your own,
you should use the :swh_web:`Web API <api/>` instead.

As ``swh-storage`` has multiple backends, it is instantiated via the
:py:func:`swh.storage.get_storage` function, which takes as argument the
backend type (usually ``remote``, if you already have access to a running
swh-storage).

It returns an instance of a class implementing
:py:class:`swh.storage.interface.StorageInterface`; which is mostly a set of
key-value stores, one for each object type.

Many of the arguments and return types are "model objects", ie. immutable
objects that are instances of the classes defined in :py:mod:`swh.model.model`.

Methods returning long lists of arguments are paginated; by returning both a
list of results and an opaque token to get the next page of results. For
example, to list all the visits of an origin using ``origin_visit_get`` ten
visits at a time, you can do:

.. code-block::

   storage = get_storage("remote", url="http://localhost:5002")
   while True:
       page = storage.origin_visit_get(origin="https://github.com/torvalds/linux")
       for visit in page.results:
           print(visit)
       if page.next_page_token is None:
           break

Or, using :py:func:`swh.core.api.classes.stream_results` for convenience:

.. code-block::

   storage = get_storage("remote", url="http://localhost:5002")
   visits = stream_results(
      storage.origin_visit_get, origin="https://github.com/torvalds/linux"
   )
   for visit in visits:
        print(visit)

Database schema
---------------

* :ref:`sql-storage`


Archive copies
--------------

* :ref:`archive-copies`

Specifications
--------------

.. toctree::

   extrinsic-metadata-specification
   object-masking


Reference Documentation
-----------------------

.. toctree::
   :maxdepth: 2

   cli

.. only:: standalone_package_doc

   Indices and tables
   ------------------

   * :ref:`genindex`
   * :ref:`modindex`
   * :ref:`search`
