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
    * `deposit_client`, for metadata pushed to Software Heritage at the same time
      as a software artifact
    * `forge`, for metadata pulled from the same source as the one hosting
      the software artifacts (which includes package managers)
    * `registry`, for metadata pulled from a third-party
  * its URL, which unambiguously identifies an instance of the authority type.

Examples:

=============== =================================
type            url
=============== =================================
deposit_client  https://hal.archives-ouvertes.fr/
deposit_client  https://hal.inria.fr/
deposit_client  https://software.intel.com/
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

      raw_extrinsic_metadata_add(
         "origin", origin_url, discovery_date,
         authority, fetcher,
         format, metadata
      )

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

      raw_extrinsic_metadata_get_latest(
         "origin", origin_url, authority
      )

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

      raw_extrinsic_metadata_get(
         "origin", origin_url,
         authority,
         page_token, limit
      )

  where `authority` must be a dict containing keys `type` and `url`
  which returns a dictionary with keys:

  * `next_page_token`, which is an opaque token to be used as
    `page_token` for retrieving the next page. if absent, there is
    no more pages to gather.
  * `results`: list of dictionaries, one for each metadata item
    deposited, corresponding to the given origin and obtained from the
    specified authority.

  Each of these dictionaries is in the following format::

      {
        'authority': {'type': ..., 'url': ...},
        'fetcher': {'name': ..., 'version': ...},
        'discovery_date': ...,
        'format': '...',
        'metadata': b'...'
      }

The parameters ``page_token`` and ``limit`` are used for pagination based on
an arbitrary order. An initial query to ``origin_metadata_get`` must set
``page_token`` to ``None``, and further query must use the value from the
previous query's ``next_page_token`` to get the next page of results.

``metadata`` is a bytes array (eventually encoded using Base64).
Its format is specific to each authority; and is treated as an opaque value
by the storage.
Unifying these various formats into a common language is outside the scope
of this specification.

Artifact metadata
^^^^^^^^^^^^^^^^^

In addition to origin metadata, the storage database stores metadata on
all software artifacts supported by the data model.

This works similarly to origin metadata, with one major difference:
extrinsic metadata can be given on a specific artifact within a specified
context (for example: a directory in a specific revision from a specific
visit on a specific origin) which will be stored along the metadata itself.

For example, two origins may develop the same file independently;
the information about authorship, licensing or even description may vary
about the same artifact in a different context.
This is why it is important to qualify the metadata with the complete
context for which it is intended, if any.

The same two endpoints as for origin can be used, but with a different
value for the first argument:

* Adding metadata::

      raw_extrinsic_metadata_add(
         type, id, context, discovery_date,
         authority, fetcher,
         format, metadata
      )


* Getting all metadata::

      raw_extrinsic_metadata_get(
         type, id,
         authority,
         after,
         page_token, limit
      )


definited similarly to ``origin_metadata_add`` and ``origin_metadata_get``,
but where ``id`` is a core SWHID (with type matching ``<X>``),
and with an extra ``context`` (argument when adding metadata, and dictionary
key when getting them) that is a dictionary with keys
depending on the artifact ``type``:

* for ``snapshot``: ``origin`` (a URL) and ``visit`` (an integer)
* for ``release``: those above, plus ``snapshot``
  (the core SWHID of a snapshot)
* for ``revision``: all those above, plus ``release``
  (the core SWHID of a release)
* for ``directory``: all those above, plus ``revision``
  (the core SWHID of a revision)
  and ``path`` (a byte string), representing the path to this directory
  from the root of the ``revision``
* for ``content``: all those above, plus ``directory``
  (the core SWHID of a directory)

All keys are optional, but should be provided whenever possible.
The dictionary may be empty, if metadata is fully independent from context.

In all cases, ``visit`` should only be provided if ``origin`` is
(as visit ids are only unique with respect to an origin).
