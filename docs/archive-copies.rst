:orphan:

.. _archive-copies:

Archive copies
==============

.. _swh-storage-copies-layout:
.. figure:: images/swh-archive-copies.svg
   :width: 1024px
   :align: center

   Layout of Software Heritage archive copies (click to zoom).

The Software Heritage archive exists in several copies, to minimize the risk of
losing archived source code artifacts. The layout of existing copies, their
relationships, as well as their geographical and administrative domains are
shown in the layout diagram above.

We recall that the archive is conceptually organized as a graph, and
specifically a Merkle DAG, see :ref:`data model <data-model>` for more
information.

Ingested source code artifacts land directly on the **primary copy**, which is
updated live and also used as reference for deduplication purposes. There,
different parts of the Merkle DAG as stored using different backend
technologies. The leaves of the graph, i.e., *content objects* (or "blobs"), are
stored in a key-value object storage, using their SHA1 or SHA256 identifiers as
keys (see :ref:`persistent identifiers <persistent-identifiers>` and our
:ref:`object storage overview <objstorage-overview>`). As our objstorage
instances use a single hash as primary key, when :mod:`swh.storage` detects
contents with any colliding hashes, both contents are pushed into a dedicated
:ref:`journal topic <journal-specs>` for further processing. The *rest of the
graph* is stored in a Cassandra cluster (see :ref:`the cassandra schema
<swh-storage-cassandra-schema>` and
:class:`swh.storage.cassandra.storage.CassandraStorage`).

At of 2025-09-25, the primary object storage contains about 26 billion blobs
with a median size of 3 KB---yes, that is *a lot of very small files*---for a
total compressed size of more than 2 petabytes. The main Cassandra cluster
comprises 16 nodes each using 10 TB of compressed storage with a cluster-wide
replication factor of 3 (that is, around 50 terabytes of data before
replication). In terms of graph metrics, the Merkle DAG has about 50 billion
nodes and 900 billion edges.

**Secondary copies** of the object storage are hosted on Microsoft Azure and
Amazon S3, using their native blob storages. The Microsoft Azure copy is
maintained synchronously, the Amazon S3 copy is maintained asynchronously using
:mod:`swh.objstorage.replayer`. A secondary copy of the graph storage, using the
PostgreSQL backend, is maintained using a set of :mod:`swh.storage.replayer`
instances for each object type. (see :ref:`SQL storage <sql-storage>`).

Archive copies (as opposed to :ref:`archive mirrors <mirror>`) are operated by the Software
Heritage Team at Inria. Both copies of the graph storage are located in
Rocquencourt, France; The primary object storage ceph cluster is located in
Saint-Aubin, France. The secondary copies of the object storage are located in
the West Europe Azure region and in the us-east-1 AWS region.
