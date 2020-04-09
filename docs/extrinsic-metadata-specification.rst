:orphan:

.. _extrinsic-metadata-specification:

Extrinsic metadata specification
================================

:term:`Extrinsic metadata` is information about software that is not part
of the source code itself but still closely related to the software.
Typical sources for extrinsic metadata are: the hosting place of a
repository, which can offer metadata via its web view or API; external
registries like collaborative curation initiatives; and out-of-band
information available at source code archival time.

Since they are not part of the source code, a dedicated mechanism to fetch
and store them is needed.

This specification assumes the reader is familiar with Software Heritage's
:ref:`architecture` and :ref:`data-model`.


Metadata sources
----------------

Authorities
^^^^^^^^^^^

Metadata authorities are entities that provide metadata about an
:term:`origin`. Metadata authorities include: code hosting places,
:term:`deposit` submitters, and registries (eg. Wikidata).

An authority is uniquely defined by these properties:

  * its type, representing the kind of authority, which is one of these values:
    * `deposit`, for metadata pushed to Software Heritage at the same time
      as a software artifact
    * `forge`, for metadata pulled from the same source as the one hosting
      the software artifacts (which includes package managers)
    * `registry`, for metadata pulled from a third-party
  * its URL, which unambiguously identifies an instance of the authority type.

Examples:

=============== =================================
type            url
=============== =================================
deposit         https://hal.archives-ouvertes.fr/
deposit         https://hal.inria.fr/
deposit         https://software.intel.com/
forge           https://gitlab.com/
forge           https://gitlab.inria.fr/
forge           https://0xacab.org/
forge           https://github.com/
registry        https://www.wikidata.org/
registry        https://swmath.org/
registry        https://ascl.net/
=============== =================================

Metadata fetchers
^^^^^^^^^^^^^^^^^

Metadata fetchers are software components used to fetch metadata from
a metadata authority, and ingest them into the Software Heritage archive.

A metadata fetcher is uniquely defined by these properties:

* its type
* its version

Examples:

* :term:`loaders <loader>`, which may either discover metadata as a
  side-effect of loading source code, or be dedicated to fetching metadata.

* :term:`listers <lister>`, which may discover metadata as a side-effect
  of discovering origins.

* :term:`deposit` submitters, which push metadata to SWH from a
  third-party; usually at the same time as a :term:`software artifact`

* crawlers, which fetch metadata from an authority in a way that is
  none of the above (eg. by querying a specific API of the origin's forge).


Storage API
-----------

Authorities and metadata fetchers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :term:`storage` API offers these endpoints to manipulate metadata
authorities and metadata fetchers:

* ``metadata_authority_add(type, url, metadata)``
  which adds a new metadata authority to the storage.

* ``metadata_authority_get(type, url)``
  which looks up a known authority (there is at most one) and if it is
  known, returns a dictionary with keys ``type``, ``url``, and ``metadata``.

* ``metadata_fetcher_add(name, version, metadata)``
  which adds a new metadata fetcher to the storage.

* ``metadata_fetcher_get(name, version)``
  which looks up a known fetcher (there is at most one) and if it is
  known, returns a dictionary with keys ``name``, ``version``, and
  ``metadata``.

These `metadata` fields contain JSON-encodable dictionaries
with information about the authority/fetcher, in a format specific to each
authority/fetcher.
With authority, the `metadata` field is reserved for information describing
and qualifying the authority.
With fetchers, the `metadata` field is reserved for configuration metadata
and other technical usage.

Origin metadata
^^^^^^^^^^^^^^^

Extrinsic metadata are stored in SWH's :term:`storage database`.
The storage API offers three endpoints to manipulate origin metadata:

* Adding metadata::

      origin_metadata_add(origin_url, discovery_date,
                          authority, fetcher,
                          format, metadata)

  which adds a new `metadata` byte string obtained from a given authority
  and associated to the origin.
  `discovery_date` is a Python datetime.
  `authority` must be a dict containing keys `type` and `url`, and
  `fetcher` a dict containing keys `name` and `version`.
  The authority and fetcher must be known to the storage before using this
  endpoint.
  `format` is a text field indicating the format of the content of the
  `metadata` byte string.

* Getting latest metadata::

      origin_metadata_get_latest(origin_url, authority)

  where `authority` must be a dict containing keys `type` and `url`,
  which returns a dictionary corresponding to the latest metadata entry
  added from this origin, in the format::

      {
        'origin_url': ...,
        'authority': {'type': ..., 'url': ...},
        'fetcher': {'name': ..., 'version': ...},
        'discovery_date': ...,
        'format': '...',
        'metadata': b'...'
      }


* Getting all metadata::

      origin_metadata_get(origin_url,
                          authority,
                          after, limit)

  which returns a list of dictionaries, one for each metadata item
  deposited, corresponding to the given origin and obtained from the
  specified authority.
  `authority` must be a dict containing keys `type` and `url`.

  Each of these dictionaries is in the following format::

      {
        'authority': {'type': ..., 'url': ...},
        'fetcher': {'name': ..., 'version': ...},
        'discovery_date': ...,
        'format': '...',
        'metadata': b'...'
      }

The parameters ``after`` and ``limit`` are used for pagination based on the
order defined by the ``discovery_date``.

``metadata`` is a bytes array (eventually encoded using Base64).
Its format is specific to each authority; and is treated as an opaque value
by the storage.
Unifying these various formats into a common language is outside the scope
of this specification.
