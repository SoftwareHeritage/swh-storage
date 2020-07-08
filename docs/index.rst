.. _swh-storage:

Software Heritage - Storage
===========================

Abstraction layer over the archive, allowing to access all stored source code
artifacts as well as their metadata


The Software Heritage storage consist of a high-level storage layer
(:mod:`swh.storage`) that exposes a client/server API
(:mod:`swh.storage.api`). The API is exposed by a server
(:mod:`swh.storage.api.server`) and accessible via a client
(:mod:`swh.storage.api.client`).

The low-level implementation of the storage is split between an object storage
(:ref:`swh.objstorage <swh-objstorage>`), which stores all "blobs" (i.e., the
leaves of the :ref:`data-model`) and a SQL representation of the rest of the
graph (:mod:`swh.storage.storage`).


Database schema
---------------

* :ref:`sql-storage`


Archive copies
--------------

* :ref:`archive-copies`

Specifications
--------------

* :ref:`extrinsic-metadata-specification`


Reference Documentation
-----------------------

.. toctree::
   :maxdepth: 2

   /apidoc/swh.storage
