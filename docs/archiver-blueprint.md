Software Heritage Archiver
==========================

The Software Heritage (SWH) Archiver is responsible for backing up SWH objects
as to reduce the risk of losing them.

Currently, the archiver only deals with content objects (i.e., those referenced
by the content table in the DB and stored in the SWH object storage). The
database itself is lively replicated by other means.


Requirements
------------

* **Peer-to-peer topology**

  Every storage involved in the archival process can be used as a source or a
  destination for the archival, depending on the blobs it contains. A
  retention policy specifies the minimum number of copies that are required
  to be "safe".

  Althoug the servers are totally equals the coordination of which content
  should be copied and from where to where is centralized.

* **Append-only archival**

  The archiver treats involved storages as append-only storages. The archiver
  never deletes any object. If removals are needed, they will be dealt with
  by other means.

* **Asynchronous archival.**

  Periodically (e.g., via cron), the archiver runs, produces a list of objects
  that need to have more copies, and starts copying them over. The decision of
  which storages are choosen to be sources and destinations are not up to the
  storages themselves.

  Very likely, during any given archival run, other new objects will be added
  to storages; it will be the responsibility of *future* archiver runs, and
  not the current one, to copy new objects over if needed.

* **Integrity at archival time.**

  Before archiving objects, the archiver performs suitable integrity checks on
  them. For instance, content objects are verified to ensure that they can be
  decompressed and that their content match their (checksum-based) unique
  identifiers. Corrupt objects will not be archived and suitable errors
  reporting about the corruption will be emitted.

  Note that archival-time integrity checks are *not meant to replace periodic
  integrity checks*.

* **Parallel archival**

  Once the list of objects to be archived has been identified, it SHOULD be
  possible to archive objects in parallel w.r.t. one another.

* **Persistent archival status**

  The archiver maintains a mapping between objects and the locations where they
  are stored. Locations are the set {master, slave_1, ..., slave_n}.

  Each pair <object,destination> is also associated to the following
  information:

  * **status**: 3-state: *missing* (copy not present at destination), *ongoing*
    (copy to destination ongoing), *present* (copy present at destination),
    *corrupted* (content detected as corrupted during an archival).
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
2. for each object that has fewer copies than those requested by the
   retention policy:
   1. mark object as needing archival
3. group objects in need of archival in batches of `archival batch size`
4. for each batch:
   1. spawn an archive worker on the whole batch (e.g., submitting the relevant
      celery task)


### Archiver worker

The archiver is executed on demand (e.g., by a celery worker) to archive a
given set of objects.

Runtime parameters:

* objects to archive
* archival policies (retention & archival max age)

At each execution a worker:

1. for each object in the batch
   1. check that the object still need to be archived
      (#present copies < retention policy)
   2. if an object has status=ongoing but the elapsed time from task submission
      is less than the *archival max age*, it count as present, as we assume
      that it will be copied in the futur. If the delay is elapsed, otherwise,
      is count as a missing copy.
2. for each object to archive:
   1. retrieve current archive status for all destinations
   2. create a map noting where the object is present and where it can be copied
   3. Randomly choose couples (source, destination), where destinations are all
      differents, to make enough copies
3. for each (content, source, destination):
   1. Join the contents by key (source, destination) to have a map
      {(source, destination) -> [contents]}
4. for each (source, destination) -> contents
   1. for each content in content, check its integrity on the source storage
      * if the object if corrupted or missing
        * update its status in the database
        * remove it from the current contents list
5. start the copy of the batces by launching for each transfer tuple a copier
      * if an error occured on one of the content that should have been valid,
        consider the whole batch as a failure.
6. set status=present and mtime=now for each successfully copied object

Note that:

* In case multiple jobs where tasked to archive the same of overlapping
  objects, step (1) might decide that some/all objects of this batch no
  longer need to be archived.

* Due to parallelism, it is possible that the same objects will be copied
  over at the same time by multiple workers. Also, the same object could end
  having more copies than the minimal number required.


### Archiver copier

The copier is run on demand by archiver workers, to transfer file batches from
a given source to a given destination.

The copier transfers files one by one. The copying process is atomic at the file
granularity (i.e., individual files might be visible on the destination before
*all* files have been transferred) and ensures that *concurrent transfer of the
same files by multiple copier instances do not result in corrupted files*. Note
that, due to this and the fact that timestamps are updated by the worker, all
files copied in the same batch will have the same mtime even though the actual
file creation times on a given destination might differ.

The copier is implemented using the ObjStorage API for the sources and
destinations.


DB structure
------------

Postgres SQL definitions for the archival status:

    -- Those names are sample of archives server names
    CREATE TYPE archive_id AS ENUM (
      'uffizi',
      'banco'
    );

    CREATE TABLE archive (
      id   archive_id PRIMARY KEY,
      url  TEXT
    );

    CREATE TYPE archive_status AS ENUM (
      'missing',
      'ongoing',
      'present',
      'corrupted'
    );

    CREATE TABLE content_archive (
      content_id sha1 unique,
      copies jsonb
    );


Where the content_archive.copies field is of type jsonb and contains datas
about the storages that contains (or not) the content represented by the sha1

    {
        "$schema": "http://json-schema.org/schema#",
        "title": "Copies data",
        "description": "Data about the presence of a content into the storages",
        "type": "object",
        "Patternproperties": {
            "^[a-zA-Z1-9]+$": {
                "description": "archival status for the server named by key",
                "type": "object",
                "properties": {
                    "status": {
                        "description": "Archival status on this copy",
                        "type": "string",
                        "enum": ["missing", "ongoing", "present", "corrupted"]
                    },
                    "mtime": {
                        "description": "Last time of the status update",
                        "type": "float"
                    }
                }
            }
        },
        "additionalProperties": false
    }
