Software Heritage Archiver
==========================

The Software Heritage (SWH) Archiver is responsible for backing up SWH objects
as to reduce the risk of losing them.

Currently, the archiver only deals with content objects (i.e., those referenced
by the content table in the DB and stored in the SWH object storage). The
database itself is lively replicated by other means.


Requirements
------------

* **Master/slave architecture**

  There is 1 master copy and 1 or more slave copies of each object. A retention
  policy specifies the minimum number of copies that are required to be "safe".

* **Append-only archival**

  The archiver treats master as read-only storage and slaves as append-only
  storages. The archiver never deletes any object. If removals are needed, in
  either master or slaves, they will be dealt with by other means.

* **Asynchronous archival.**

  Periodically (e.g., via cron), the archiver runs, produces a list of objects
  that need to be copied from master to slaves, and starts copying them over.
  Very likely, during any given archival run other new objects will be added to
  master; it will be the responsibility of *future* archiver runs, and not the
  current one, to copy new objects over.

* **Integrity at archival time.**

  Before archiving objects, the archiver performs suitable integrity checks on
  them. For instance, content objects are verified to ensure that they can be
  decompressed and that their content match their (checksum-based) unique
  identifiers. Corrupt objects will not be archived and suitable errors
  reporting about the corruption will be emitted.

  Note that archival-time integrity checks are *not meant to replace periodic
  integrity checks* on both master and slave copies.

* **Parallel archival**

  Once the list of objects to be archived has been identified, it SHOULD be
  possible to archive objects in parallel w.r.t. one another.

* **Persistent archival status**

  The archiver maintains a mapping between objects and the locations where they
  are stored. Locations are the set {master, slave_1, ..., slave_n}.

  Each pair <object,destination> is also associated to the following
  information:

  * **status**: 3-state: *missing* (copy not present at destination), *ongoing*
    (copy to destination ongoing), *present* (copy present at destination)
  * **mtime**: timestamp of last status change. This is either the destination
    archival time (when status=present), or the timestamp of the last archival
    request (status=ongoing); the timestamp is undefined when status=missing.


Architecture
------------

The archiver is comprised of the following software components:

* archiver director
* archiver worker
* archiver copier


### Archiver director

The archiver director is run periodically, e.g., via cron.

Runtime parameters:

* execution periodicity (external)
* retention policy
* archival max age
* archival batch size

At each execution the director:

1. for each object: retrieve its archival status
2. for each object that is in the master storage but has fewer copies than
   those requested by the retention policy:
   1. if status=ongoing and mtime is not older than archival max age  
      then continue to next object
   2. check object integrity (e.g., with swh.storage.ObjStorage.check(obj_id))
   3. mark object as needing archival
3. group objects in need of archival in batches of archival batch size
4. for each batch:
   1. set status=ongoing and mtime=now() for each object in the batch
   2. spawn an archive worker on the whole batch (e.g., submitting the relevant
      celery task)

Note that if an archiver worker task takes a long time (t > archival max age)
to complete, it is possible for another task to be scheduled on the same batch,
or an overlapping one.


### Archiver worker

The archiver is executed on demand (e.g., by a celery worker) to archive a
given set of objects.

Runtime parameters:

* objects to archive

At each execution a worker:

1. create empty map { destinations -> objects that need to be copied there }
2. for each object to archive:
   1. retrieve current archive status for all destinations
   2. update the map noting where the object needs to be copied
3. for each destination:
   1. look up in the map objects that need to be copied there
   2. copy all objects to destination using the copier
   3. set status=present and mtime=now() for each copied object

Note that:

* In case multiple jobs where tasked to archive the same of overlapping
  objects, step (2.2) might decide that some/all objects of this batch no
  longer need to be archived to some/all destinations.

* Due to parallelism, it is also possible that the same objects will be copied
  over at the same time by multiple workers.


### Archiver copier

The copier is run on demand by archiver workers, to transfer file batches from
master to a given destination.

The copier transfers all files together with a single network connection. The
copying process is atomic at the file granularity (i.e., individual files might
be visible on the destination before *all* files have been transferred) and
ensures that *concurrent transfer of the same files by multiple copier
instances do not result in corrupted files*. Note that, due to this and the
fact that timestamps are updated by the director, all files copied in the same
batch will have the same mtime even though the actual file creation times on a
given destination might differ.

As a first approximation, the copier can be implemented using rsync, but a
dedicated protocol can be devised later. In the case of rsync, one should use
--files-from to list the file to be copied. Rsync atomically renames files
one-by-one during transfer; so as long as --inplace is *not* used, concurrent
rsync of the same files should not be a problem.


DB structure
------------

Postgres SQL definitions for the archival status:

    CREATE DOMAIN archive_id AS TEXT;

    CREATE TABLE archives (
      id   archive_id PRIMARY KEY,
      url  TEXT
    );

    CREATE TYPE archive_status AS ENUM (
      'missing',
      'ongoing',
      'present'
    );

    CREATE TABLE content_archive (
      content_id  sha1 REFERENCES content(sha1),
      archive_id  archive_id REFERENCES archives(id),
      status      archive_status,
      mtime       timestamptz,
      PRIMARY KEY (content_id, archive_id)
    );
