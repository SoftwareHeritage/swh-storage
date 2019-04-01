---
--- SQL implementation of the Software Heritage data model
---

-- schema versions
create table dbversion
(
  version     int primary key,
  release     timestamptz,
  description text
);

-- latest schema version
insert into dbversion(version, release, description)
      values(133, now(), 'Work In Progress');

-- a SHA1 checksum
create domain sha1 as bytea check (length(value) = 20);

-- a Git object ID, i.e., a Git-style salted SHA1 checksum
create domain sha1_git as bytea check (length(value) = 20);

-- a SHA256 checksum
create domain sha256 as bytea check (length(value) = 32);

-- a blake2 checksum
create domain blake2s256 as bytea check (length(value) = 32);

-- UNIX path (absolute, relative, individual path component, etc.)
create domain unix_path as bytea;

-- a set of UNIX-like access permissions, as manipulated by, e.g., chmod
create domain file_perms as int;


-- Checksums about actual file content. Note that the content itself is not
-- stored in the DB, but on external (key-value) storage. A single checksum is
-- used as key there, but the other can be used to verify that we do not inject
-- content collisions not knowingly.
create table content
(
  sha1       sha1 not null,
  sha1_git   sha1_git not null,
  sha256     sha256 not null,
  blake2s256 blake2s256,
  length     bigint not null,
  ctime      timestamptz not null default now(),
             -- creation time, i.e. time of (first) injection into the storage
  status     content_status not null default 'visible',
  object_id  bigserial
);


-- An origin is a place, identified by an URL, where software source code
-- artifacts can be found. We support different kinds of origins, e.g., git and
-- other VCS repositories, web pages that list tarballs URLs (e.g.,
-- http://www.kernel.org), indirect tarball URLs (e.g.,
-- http://www.example.org/latest.tar.gz), etc. The key feature of an origin is
-- that it can be *fetched* from (wget, git clone, svn checkout, etc.) to
-- retrieve all the contained software.
create table origin
(
  id       bigserial not null,
  type     text, -- TODO use an enum here (?)
  url      text not null
);


-- Content blobs observed somewhere, but not ingested into the archive for
-- whatever reason. This table is separate from the content table as we might
-- not have the sha1 checksum of skipped contents (for instance when we inject
-- git repositories, objects that are too big will be skipped here, and we will
-- only know their sha1_git). 'reason' contains the reason the content was
-- skipped. origin is a nullable column allowing to find out which origin
-- contains that skipped content.
create table skipped_content
(
  sha1       sha1,
  sha1_git   sha1_git,
  sha256     sha256,
  blake2s256 blake2s256,
  length     bigint not null,
  ctime      timestamptz not null default now(),
  status     content_status not null default 'absent',
  reason     text not null,
  origin     bigint,
  object_id  bigserial
);


-- Log of all origin fetches (i.e., origin crawling) that have been done in the
-- past, or are still ongoing. Similar to list_history, but for origins.
create table fetch_history
(
  id        bigserial,
  origin    bigint,
  date      timestamptz not null,
  status    boolean,  -- true if and only if the fetch has been successful
  result    jsonb,     -- more detailed returned values, times, etc...
  stdout    text,
  stderr    text,     -- null when status is true, filled otherwise
  duration  interval  -- fetch duration of NULL if still ongoing
);


-- A file-system directory.  A directory is a list of directory entries (see
-- tables: directory_entry_{dir,file}).
--
-- To list the contents of a directory:
-- 1. list the contained directory_entry_dir using array dir_entries
-- 2. list the contained directory_entry_file using array file_entries
-- 3. list the contained directory_entry_rev using array rev_entries
-- 4. UNION
--
-- Synonyms/mappings:
-- * git: tree
create table directory
(
  id            sha1_git not null,
  dir_entries   bigint[],  -- sub-directories, reference directory_entry_dir
  file_entries  bigint[],  -- contained files, reference directory_entry_file
  rev_entries   bigint[],  -- mounted revisions, reference directory_entry_rev
  object_id     bigserial  -- short object identifier
);

-- A directory entry pointing to a (sub-)directory.
create table directory_entry_dir
(
  id      bigserial,
  target  sha1_git,   -- id of target directory
  name    unix_path,  -- path name, relative to containing dir
  perms   file_perms  -- unix-like permissions
);

-- A directory entry pointing to a file content.
create table directory_entry_file
(
  id      bigserial,
  target  sha1_git,   -- id of target file
  name    unix_path,  -- path name, relative to containing dir
  perms   file_perms  -- unix-like permissions
);

-- A directory entry pointing to a revision.
create table directory_entry_rev
(
  id      bigserial,
  target  sha1_git,   -- id of target revision
  name    unix_path,  -- path name, relative to containing dir
  perms   file_perms  -- unix-like permissions
);


-- A person referenced by some source code artifacts, e.g., a VCS revision or
-- release metadata.
create table person
(
  id        bigserial,
  name      bytea,          -- advisory: not null if we managed to parse a name
  email     bytea,          -- advisory: not null if we managed to parse an email
  fullname  bytea not null  -- freeform specification; what is actually used in the checksums
                            --     will usually be of the form 'name <email>'
);


-- The state of a source code tree at a specific point in time.
--
-- Synonyms/mappings:
-- * git / subversion / etc: commit
-- * tarball: a specific tarball
--
-- Revisions are organized as DAGs. Each revision points to 0, 1, or more (in
-- case of merges) parent revisions. Each revision points to a directory, i.e.,
-- a file-system tree containing files and directories.
create table revision
(
  id                    sha1_git not null,
  date                  timestamptz,
  date_offset           smallint,
  committer_date        timestamptz,
  committer_date_offset smallint,
  type                  revision_type not null,
  directory             sha1_git,  -- source code "root" directory
  message               bytea,
  author                bigint,
  committer             bigint,
  synthetic             boolean not null default false,  -- true iff revision has been created by Software Heritage
  metadata              jsonb,  -- extra metadata (tarball checksums, extra commit information, etc...)
  object_id             bigserial,
  date_neg_utc_offset   boolean,
  committer_date_neg_utc_offset boolean
);

-- either this table or the sha1_git[] column on the revision table
create table revision_history
(
  id           sha1_git not null,
  parent_id    sha1_git not null,
  parent_rank  int not null default 0
    -- parent position in merge commits, 0-based
);


-- Crawling history of software origins visited by Software Heritage. Each
-- visit is a 3-way mapping between a software origin, a timestamp, and a
-- snapshot object capturing the full-state of the origin at visit time.
create table origin_visit
(
  origin       bigint not null,
  visit        bigint not null,
  date         timestamptz not null,
  status       origin_visit_status not null,
  metadata     jsonb,
  snapshot     sha1_git
);

comment on column origin_visit.origin is 'Visited origin';
comment on column origin_visit.visit is 'Sequential visit number for the origin';
comment on column origin_visit.date is 'Visit timestamp';
comment on column origin_visit.status is 'Visit result';
comment on column origin_visit.metadata is 'Origin metadata at visit time';
comment on column origin_visit.snapshot is 'Origin snapshot at visit time';


-- A snapshot represents the entire state of a software origin as crawled by
-- Software Heritage. This table is a simple mapping between (public) intrinsic
-- snapshot identifiers and (private) numeric sequential identifiers.
create table snapshot
(
  object_id  bigserial not null,  -- PK internal object identifier
  id         sha1_git not null    -- snapshot intrinsic identifier
);

-- Each snapshot associate "branch" names to other objects in the Software
-- Heritage Merkle DAG. This table describes branches as mappings between names
-- and target typed objects.
create table snapshot_branch
(
  object_id    bigserial not null,  -- PK internal object identifier
  name         bytea not null,      -- branch name, e.g., "master" or "feature/drag-n-drop"
  target       bytea,               -- target object identifier, e.g., a revision identifier
  target_type  snapshot_target      -- target object type, e.g., "revision"
);

-- Mapping between snapshots and their branches.
create table snapshot_branches
(
  snapshot_id  bigint not null,  -- snapshot identifier, ref. snapshot.object_id
  branch_id    bigint not null   -- branch identifier, ref. snapshot_branch.object_id
);


-- A "memorable" point in time in the development history of a software
-- project.
--
-- Synonyms/mappings:
-- * git: tag (of the annotated kind, otherwise they are just references)
-- * tarball: the release version number
create table release
(
  id          sha1_git not null,
  target      sha1_git,
  date        timestamptz,
  date_offset smallint,
  name        bytea,
  comment     bytea,
  author      bigint,
  synthetic   boolean not null default false,  -- true iff release has been created by Software Heritage
  object_id   bigserial,
  target_type object_type not null,
  date_neg_utc_offset  boolean
);


-- Tools
create table tool
(
  id serial not null,
  name text not null,
  version text not null,
  configuration jsonb
);

comment on table tool is 'Tool information';
comment on column tool.id is 'Tool identifier';
comment on column tool.version is 'Tool name';
comment on column tool.version is 'Tool version';
comment on column tool.configuration is 'Tool configuration: command line, flags, etc...';


create table metadata_provider
(
  id            serial not null,
  provider_name text   not null,
  provider_type text   not null,
  provider_url  text,
  metadata      jsonb
);

comment on table metadata_provider is 'Metadata provider information';
comment on column metadata_provider.id is 'Provider''s identifier';
comment on column metadata_provider.provider_name is 'Provider''s name';
comment on column metadata_provider.provider_url is 'Provider''s url';
comment on column metadata_provider.metadata is 'Other metadata about provider';


-- Discovery of metadata during a listing, loading, deposit or external_catalog of an origin
-- also provides a translation to a defined json schema using a translation tool (tool_id)
create table origin_metadata
(
  id             bigserial     not null,  -- PK internal object identifier
  origin_id      bigint        not null,  -- references origin(id)
  discovery_date timestamptz   not null,  -- when it was extracted
  provider_id    bigint        not null,  -- ex: 'hal', 'lister-github', 'loader-github'
  tool_id        bigint        not null,
  metadata       jsonb         not null
);

comment on table origin_metadata is 'keeps all metadata found concerning an origin';
comment on column origin_metadata.id is 'the origin_metadata object''s id';
comment on column origin_metadata.origin_id is 'the origin id for which the metadata was found';
comment on column origin_metadata.discovery_date is 'the date of retrieval';
comment on column origin_metadata.provider_id is 'the metadata provider: github, openhub, deposit, etc.';
comment on column origin_metadata.tool_id is 'the tool used for extracting metadata: lister-github, etc.';
comment on column origin_metadata.metadata is 'metadata in json format but with original terms';


-- Keep a cache of object counts
create table object_counts
(
  object_type text,             -- table for which we're counting objects (PK)
  value bigint,                 -- count of objects in the table
  last_update timestamptz,      -- last update for the object count in this table
  single_update boolean         -- whether we update this table standalone (true) or through bucketed counts (false)
);

create table object_counts_bucketed
(
    line serial not null,       -- PK
    object_type text not null,  -- table for which we're counting objects
    identifier text not null,   -- identifier across which we're bucketing objects
    bucket_start bytea,         -- lower bound (inclusive) for the bucket
    bucket_end bytea,           -- upper bound (exclusive) for the bucket
    value bigint,               -- count of objects in the bucket
    last_update timestamptz     -- last update for the object count in this bucket
);
