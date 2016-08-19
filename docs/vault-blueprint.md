Software Heritage Vault
=======================

Software source code **objects**---e.g., individual source code files,
tarballs, commits, tagged releases, etc.---are stored in the Software Heritage
(SWH) Archive in fully deduplicated form. That allows direct access to
individual artifacts but require some preparation, usually in the form of
collecting and assemblying multiple artifacts in a single **bundle**, when fast
access to a set of related artifacts (e.g., the snapshot of a VCS repository,
the archive corresponding to a Git commit, or a specific software release as a
zip archive) is required.

The **Software Heritage Vault** is a cache of pre-built source code bundles
which are assembled opportunistically retrieving objects from the Software
Heritage Archive, can be accessed efficiently, and might be garbage collected
after a long period of non use.


Requirements
------------

* **Shared cache**

  The vault is a cache shared among the various origins that the SWH archive
  tracks. If the same bundle, originally coming from different origins, is
  requested, a single entry for it in the cache shall exist.

* **Efficient retrieval**

  Where supported by the desired access protocol (e.g., HTTP) it should be
  possible for the vault to serve bundles efficiently (e.g., as static files
  served via HTTP, possibly further proxied/cached at that level). In
  particular, this rules out building bundles on the fly from the archive DB.


API
---

All URLs below are meant to be mounted at API root, which is currently at
<https://archive.softwareheritage.org/api/1/>. Unless otherwise stated, all API
endpoints respond on HTTP GET method.


## Object identification

The vault stores bundles corresponding to different kinds of objects. The
following object kinds are supported:

* directories
* revisions
* repository snapshots

The URL fragment `:objectkind/:objectid` is used throughout the vault API to
fully identify vault objects. The syntax and meaning of :objectid for the
different object kinds is detailed below.


### Directories

* object kind: directory
* URL fragment: directory/:sha1git

where :sha1git is the directory ID in the SWH data model.

### Revisions

* object kind: revision
* URL fragment: revision/:sha1git

where :sha1git is the revision ID in the SWH data model.

### Repository snapshots

* object kind: snapshot
* URL fragment: snapshot/:sha1git

where :sha1git is the snapshot ID in the SWH data model. (**TODO** repository
snapshots don't exist yet as first-class citizens in the SWH data model; see
References below.)


## Cooking

Bundles in the vault might be ready for retrieval or not. When they are not,
they will need to be **cooked** before they can be retrieved. A cooked bundle
will remain around until it expires; at that point it will need to be cooked
again before it can be retrieved. Cooking is idempotent, and a no-op in between
a previous cooking operation and expiration.

To cook a bundle:

* POST /vault/:objectkind/:objectid

  Request body: **TODO** something here in a JSON payload that would allow
  notifying the user when the bundle is ready.

  Response: 201 Created


## Retrieval

* GET /vault/:objectkind

  (paginated) list of all bundles of a given kind available in the vault; see
  Pagination. Note that, due to cache expiration, objects might disappear
  between listing and subsequent actions on them

  Examples:

  * GET /vault/directory
  * GET /vault/revision

* GET /vault/:objectkind/:objectid

  Retrieve a specific bundle from the vault.

  Response:

  * 200 OK:        bundle available; response body is the bundle
  * 404 Not Found: missing bundle; client should request its preparation (see Cooking)


References
----------

* [Repository snapshot objects](https://wiki.softwareheritage.org/index.php?title=User:StefanoZacchiroli/Repository_snapshot_objects)
* Amazon Web Services,
  [API Reference for Amazon Glacier](http://docs.aws.amazon.com/amazonglacier/latest/dev/amazon-glacier-api.html);
  specifically
  [Job Operations](http://docs.aws.amazon.com/amazonglacier/latest/dev/job-operations.html)


TODO
====

* **TODO** pagination using HATEOAS
* **TODO** authorization: the cooking API should be somehow controlled to avoid
  obvious abuses (e.g., let's cache everything)
* **TODO** finalize repository snapshot proposal
