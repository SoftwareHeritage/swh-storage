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

  * ``deposit_client``, for metadata pushed to Software Heritage at the same time
    as a software artifact
  * ``forge``, for metadata pulled from the same source as the one hosting
    the software artifacts (which includes package managers)
  * ``registry``, for metadata pulled from a third-party

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

Data model
~~~~~~~~~~

The :term:`storage` API uses these structures to represent metadata
authorities and metadata fetchers (simplified Python code)::

   class MetadataAuthorityType(Enum):
       DEPOSIT_CLIENT = "deposit_client"
       FORGE = "forge"
       REGISTRY = "registry"

   class MetadataAuthority(BaseModel):
       """Represents an entity that provides metadata about an origin or
       software artifact."""

       object_type = "metadata_authority"

       type: MetadataAuthorityType
       url: str

   class MetadataFetcher(BaseModel):
       """Represents a software component used to fetch metadata from a metadata
       authority, and ingest them into the Software Heritage archive."""

       object_type = "metadata_fetcher"

       name: str
       version: str

Storage API
~~~~~~~~~~~

* ``metadata_authority_add(authorities: List[MetadataAuthority])``
  which adds a list of ``MetadataAuthority`` to the storage.

* ``metadata_authority_get(type: MetadataAuthorityType, url: str) -> Optional[MetadataAuthority]``
  which looks up a known authority (there is at most one) and if it is
  known, returns the corresponding ``MetadataAuthority``

* ``metadata_fetcher_add(fetchers: List[MetadataFetcher])``
  which adds a list of ``MetadataFetcher`` to the storage.

* ``metadata_fetcher_get(name: str, version: str) -> Optional[MetadataFetcher]``
  which looks up a known fetcher (there is at most one) and if it is
  known, returns the corresponding ``MetadataFetcher``

Artifact metadata
^^^^^^^^^^^^^^^^^

Data model
~~~~~~~~~~

The storage database stores metadata on origins, and all software artifacts
supported by the data model.
They are represented using this structure (simplified Python code)::

   class RawExtrinsicMetadata(HashableObject, BaseModel):
       object_type = "raw_extrinsic_metadata"

       # target object
       target: ExtendedSWHID

       # source
       discovery_date: datetime.datetime
       authority: MetadataAuthority
       fetcher: MetadataFetcher

       # the metadata itself
       format: str
       metadata: bytes

       # context
       origin: Optional[str] = None
       visit: Optional[int] = None
       snapshot: Optional[CoreSWHID] = None
       release: Optional[CoreSWHID] = None
       revision: Optional[CoreSWHID] = None
       path: Optional[bytes] = None
       directory: Optional[CoreSWHID] = None

       id: Sha1Git

The ``target`` may be:

* a regular :ref:`core SWHID <persistent-identifiers>`,
* a SWHID-like string with type ``ori`` and the SHA1 of an origin URL
* a SWHID-like string with type ``emd`` and the SHA1 of an other
  ``RawExtrinsicMetadata`` object (to represent metadata on metadata objects)

``id`` is a sha1 hash of the ``RawExtrinsicMetadata`` object itself;
it may be used in other ``RawExtrinsicMetadata`` as target.

``discovery_date`` is a Python datetime.
``authority`` must be a dict containing keys ``type`` and ``url``, and
``fetcher`` a dict containing keys ``name`` and ``version``.
The authority and fetcher must be known to the storage before using this
endpoint.
``format`` is a text field indicating the format of the content of the
``metadata`` byte string, see `extrinsic-metadata-formats`_.

``metadata`` is a byte array.
Its format is specific to each authority; and is treated as an opaque value
by the storage.
Unifying these various formats into a common language is outside the scope
of this specification.

Finally, the remaining fields allow metadata can be given on a specific artifact within
a specified context (for example: a directory in a specific revision from a specific
visit on a specific origin) which will be stored along the metadata itself.

For example, two origins may develop the same file independently;
the information about authorship, licensing or even description may vary
about the same artifact in a different context.
This is why it is important to qualify the metadata with the complete
context for which it is intended, if any.

The allowed context fields for each ``target`` type are:

* for ``emd`` (extrinsic metadata) and ``ori`` (origin): none
* for ``snp`` (snapshot): ``origin`` (a URL) and ``visit`` (an integer)
* for ``rel`` (release): those above, plus ``snapshot``
  (the core SWHID of a snapshot)
* for ``rev`` (revision): all those above, plus ``release``
  (the core SWHID of a release)
* for ``dir`` (directory): all those above, plus ``revision``
  (the core SWHID of a revision)
  and ``path`` (a byte string), representing the path to this directory
  from the root of the ``revision``
* for ``cnt`` (content): all those above, plus ``directory``
  (the core SWHID of a directory)

All keys are optional, but should be provided whenever possible.
The dictionary may be empty, if metadata is fully independent from context.

In all cases, ``visit`` should only be provided if ``origin`` is
(as visit ids are only unique with respect to an origin).

Storage API
~~~~~~~~~~~

The storage API offers three endpoints to manipulate origin metadata:

* Adding metadata::

      raw_extrinsic_metadata_add(metadata: List[RawExtrinsicMetadata])

  which adds a list of ``RawExtrinsicMetadata`` objects, whose ``metadata`` field
  is a byte string obtained from a given authority and associated to the ``target``.


* Getting all metadata::

      raw_extrinsic_metadata_get(
          target: ExtendedSWHID,
          authority: MetadataAuthority,
          after: Optional[datetime.datetime] = None,
          page_token: Optional[bytes] = None,
          limit: int = 1000,
      ) -> PagedResult[RawExtrinsicMetadata]:

  returns a list of ``RawExtrinsicMetadata`` with the given ``target`` and from
  the given ``authority``.
  If ``after`` is provided, only objects whose discovery date is more recent are
  returnered.

  ``PagedResult`` is a structure containing the results themselves,
  and a ``next_page_token`` used to fetch the next list of results, if any


.. _extrinsic-metadata-formats:

Extrinsic metadata formats
--------------------------

Formats are identified by an opaque string.
When possible, it should be the MIME type already in use to describe the
metadata format outside Software Heritage.
Otherwise it should be unambiguous, printable ASCII without spaces,
and human-readable.

Here is a list of all the metadata format stored:

``application/vnd.github.v3+json``
    The metadata is the response of an API call to GitHub.
``cpan-release-json``
    The metadata is the response of an API call to `CPAN's /release/{author}/{release}
    <https://github.com/metacpan/metacpan-api/blob/master/docs/API-docs.md#releaseauthorrelease>`_ endpoint.
``gitlab-project-json``
    The metadata is the response of an API call to `Gitlab's /api/v4/projects/:id
    <https://docs.gitlab.com/ee/api/projects.html#get-single-project>`_ endpoint.
``gitea-project-json``
    The metadata is the response of an API call to `Gitea's /api/v1/repos/{owner}/{repo}
    <https://gittea.dev/api/swagger#/repository/repoGet>`_ endpoint.
``gogs-project-json``
    The metadata is the response of an API call to `Gogs's /api/v1/repos/:owner/:repo
    <https://github.com/gogs/docs-api/tree/master/Repositories#get>`_ endpoint.
``pypi-project-json``
    The metadata is a release entry from a PyPI project's
    JSON file, extracted and re-serialized.
``replicate-npm-package-json``
    ditto, but from a replicate.npmjs.com project
``nixguix-sources-json``
    ditto, but from https://nix-community.github.io/nixpkgs-swh/
``original-artifacts-json``
    tarball data, see below
``sword-v2-atom-codemeta``
    XML Atom document, with Codemeta metadata,
    as sent by a deposit client, see the
    :ref:`Deposit protocol reference <deposit-protocol>`.
``sword-v2-atom-codemeta-v2``
    Deprecated alias of ``sword-v2-atom-codemeta``
``sword-v2-atom-codemeta-v2-in-json``
    Deprecated, JSON serialization of a ``sword-v2-atom-codemeta`` document.
``xml-deposit-info``
    Information about a deposit, to identify the provenance of
    a metadata object sent via swh-deposit, see below

Details on some of these formats:


.. _extrinsic-metadata-original-artifacts-json:

original-artifacts-json
^^^^^^^^^^^^^^^^^^^^^^^

This is a loosely defined format, originally used as a ``metadata`` column
on the ``revision`` table that changed over the years.

It is a JSON array, and each entry is a JSON object representing an archive
(tarball, zipball, ...) that was unpackaged by the SWH loader
before loading its content in Software Heritage.

When writing this specification, it was stabilized to this format::

   [
      {
         "length": <int>,
         "filename": "<original filename>",
         "checksums": {
             "sha1": "<hex-encoded string>",
             "sha256": "<hex-encoded string>",
         },
         "url": "<URL the archive was downloaded from>"
      },
      ...
   ]

Older ``original-artifacts-json`` were migrated to use this format,
but may be missing some of the keys.


xml-deposit-info
^^^^^^^^^^^^^^^^

Deposits with code objects are loaded as their own origin, so we can
look them up in the deposit database from their metadata (which hold the
origin as a context).

This is not true for metadata-only deposits, because we don't create an
origin for them; so we need to store this information somewhere.
The naive solution would be to insert them in the Atom entry provided by
the client, but it means altering a document before we archive it, which
potentially corrupts it or loses part of the data.

Therefore, on each metadata-only deposit, the deposit creates an extra
"metametadata" object, with the original metadata object as target,
and using this format::

   <deposit xmlns="https://www.softwareheritage.org/schema/2018/deposit">
       <deposit_id>{{ deposit.id }}</deposit_id>
       <deposit_client>{{ deposit.client.provider_url }}</deposit_client>
       <deposit_collection>{{ deposit.collection.name }}</deposit_collection>
   </deposit>
