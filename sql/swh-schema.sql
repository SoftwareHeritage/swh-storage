---
--- Software Heritage Data Model
---

-- drop schema if exists swh cascade;
-- create schema swh;
-- set search_path to swh;

create table dbversion
(
  version     int primary key,
  release     timestamptz,
  description text
);

insert into dbversion(version, release, description)
      values(98, now(), 'Work In Progress');

-- a SHA1 checksum (not necessarily originating from Git)
create domain sha1 as bytea check (length(value) = 20);

-- a Git object ID, i.e., a SHA1 checksum
create domain sha1_git as bytea check (length(value) = 20);

-- a SHA256 checksum
create domain sha256 as bytea check (length(value) = 32);

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
  sha1      sha1 not null,
  sha1_git  sha1_git not null,
  sha256    sha256 not null,
  length    bigint not null,
  ctime     timestamptz not null default now(),
            -- creation time, i.e. time of (first) injection into the storage
  status    content_status not null default 'visible',
  object_id bigserial
);


-- Entities constitute a typed hierarchy of organization, hosting
-- facilities, groups, people and software projects.
--
-- Examples of entities: Software Heritage, Debian, GNU, GitHub,
-- Apache, The Linux Foundation, the Debian Python Modules Team, the
-- torvalds GitHub user, the torvalds/linux GitHub project.
--
-- The data model is hierarchical (via the parent attribute) and might
-- store sub-branches of existing entities. The key feature of an
-- entity is might be *listed* (if it is available in listable_entity)
-- to retrieve information about its content, i.e: sub-entities,
-- projects, origins.

-- The history of entities. Allows us to keep historical metadata
-- about entities.  The temporal invariant is the uuid. Root
-- organization uuids are manually generated (and available in
-- swh-data.sql).
--
-- For generated entities (generated = true), we can provide
-- generation_metadata to allow listers to retrieve the uuids of previous
-- iterations of the entity.
--
-- Inactive entities that have been active in the past (active =
-- false) should register the timestamp at which we saw them
-- deactivate, in a new entry of entity_history.
create table entity_history
(
  id               bigserial not null,
  uuid             uuid,
  parent           uuid,             -- should reference entity_history(uuid)
  name             text not null,
  type             entity_type not null,
  description      text,
  homepage         text,
  active           boolean not null, -- whether the entity was seen on the last listing
  generated        boolean not null, -- whether this entity has been generated by a lister
  lister_metadata  jsonb,            -- lister-specific metadata, used for queries
  metadata         jsonb,
  validity         timestamptz[]     -- timestamps at which we have seen this entity
);

-- The entity table provides a view of the latest information on a
-- given entity. It is updated via a trigger on entity_history.
create table entity
(
  uuid             uuid not null,
  parent           uuid,
  name             text not null,
  type             entity_type not null,
  description      text,
  homepage         text,
  active           boolean not null, -- whether the entity was seen on the last listing
  generated        boolean not null, -- whether this entity has been generated by a lister
  lister_metadata  jsonb,            -- lister-specific metadata, used for queries
  metadata         jsonb,
  last_seen        timestamptz,      -- last listing time or disappearance time for active=false
  last_id          bigint            -- last listing id
);

-- Register the equivalence between two entities. Allows sideways
-- navigation in the entity table
create table entity_equivalence
(
  entity1 uuid,
  entity2 uuid
);

-- Register a lister for a specific entity.
create table listable_entity
(
  uuid         uuid,
  enabled      boolean not null default true, -- do we list this entity automatically?
  list_engine  text,  -- crawler to be used to list entity's content
  list_url     text,  -- root URL to start the listing
  list_params  jsonb,  -- org-specific listing parameter
  latest_list  timestamptz  -- last time the entity's content has been listed
);

-- Log of all entity listings (i.e., entity crawling) that have been
-- done in the past, or are still ongoing.
create table list_history
(
  id        bigserial not null,
  entity    uuid,
  date      timestamptz not null,
  status    boolean,  -- true if and only if the listing has been successful
  result    jsonb,     -- more detailed return value, depending on status
  stdout    text,
  stderr    text,
  duration  interval  -- fetch duration of NULL if still ongoing
);


-- An origin is a place, identified by an URL, where software can be found. We
-- support different kinds of origins, e.g., git and other VCS repositories,
-- web pages that list tarballs URLs (e.g., http://www.kernel.org), indirect
-- tarball URLs (e.g., http://www.example.org/latest.tar.gz), etc. The key
-- feature of an origin is that it can be *fetched* (wget, git clone, svn
-- checkout, etc.) to retrieve all the contained software.
create table origin
(
  id       bigserial not null,
  type     text, -- TODO use an enum here (?)
  url      text not null,
  lister   uuid,
  project  uuid
);

-- Content we have seen but skipped for some reason. This table is
-- separate from the content table as we might not have the sha1
-- checksum of that data (for instance when we inject git
-- repositories, objects that are too big will be skipped here, and we
-- will only know their sha1_git). 'reason' contains the reason the
-- content was skipped. origin is a nullable column allowing to find
-- out which origin contains that skipped content.
create table skipped_content
(
  sha1      sha1,
  sha1_git  sha1_git,
  sha256    sha256,
  length    bigint not null,
  ctime     timestamptz not null default now(),
  status    content_status not null default 'absent',
  reason    text not null,
  origin    bigint,
  object_id bigserial
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
  id            sha1_git,
  dir_entries   bigint[],  -- sub-directories, reference directory_entry_dir
  file_entries  bigint[],  -- contained files, reference directory_entry_file
  rev_entries   bigint[],  -- mounted revisions, reference directory_entry_rev
  object_id     bigserial  -- short object identifier
);

-- A directory entry pointing to a sub-directory.
create table directory_entry_dir
(
  id      bigserial,
  target  sha1_git,   -- id of target directory
  name    unix_path,  -- path name, relative to containing dir
  perms   file_perms  -- unix-like permissions
);

-- A directory entry pointing to a file.
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

create table person
(
  id        bigserial,
  fullname  bytea not null, -- freeform specification; what is actually used in the checksums
                            --     will usually be of the form 'name <email>'
  name      bytea,          -- advisory: not null if we managed to parse a name
  email     bytea           -- advisory: not null if we managed to parse an email
);

-- A snapshot of a software project at a specific point in time.
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
  id                    sha1_git,
  date                  timestamptz,
  date_offset           smallint,
  date_neg_utc_offset   boolean,
  committer_date        timestamptz,
  committer_date_offset smallint,
  committer_date_neg_utc_offset boolean,
  type                  revision_type not null,
  directory             sha1_git,  -- file-system tree
  message               bytea,
  author                bigint,
  committer             bigint,
  metadata              jsonb, -- extra metadata (tarball checksums, extra commit information, etc...)
  synthetic             boolean not null default false,  -- true if synthetic (cf. swh-loader-tar)
  object_id             bigserial
);


-- either this table or the sha1_git[] column on the revision table
create table revision_history
(
  id           sha1_git,
  parent_id    sha1_git,
  parent_rank  int not null default 0
    -- parent position in merge commits, 0-based
);

-- The timestamps at which Software Heritage has made a visit of the given origin.
create table origin_visit
(
  origin    bigint not null,
  visit     bigint not null,
  date      timestamptz not null,
  status    origin_visit_status not null,
  metadata  jsonb
);

comment on column origin_visit.origin is 'Visited origin';
comment on column origin_visit.visit is 'Visit number the visit occurred for that origin';
comment on column origin_visit.date is 'Visit date for that origin';
comment on column origin_visit.status is 'Visit status for that origin';
comment on column origin_visit.metadata is 'Metadata associated with the visit';


-- The content of software origins is indexed starting from top-level pointers
-- called "branches". Every time we fetch some origin we store in this table
-- where the branches pointed to at fetch time.
--
-- Synonyms/mappings:
-- * git: ref (in the "git update-ref" sense)
create table occurrence_history
(
  origin       bigint not null,
  branch       bytea not null,        -- e.g., b"master" (for VCS), or b"sid" (for Debian)
  target       sha1_git not null,     -- ref target, e.g., commit id
  target_type  object_type not null,  -- ref target type
  object_id    bigserial not null,    -- short object identifier
  visits       bigint[] not null      -- the visits where that occurrence was valid. References
                                      -- origin_visit(visit), where o_h.origin = origin_visit.origin.
);

-- Materialized view of occurrence_history, storing the *current* value of each
-- branch, as last seen by SWH.
create table occurrence
(
  origin    bigint,
  branch    bytea not null,
  target    sha1_git not null,
  target_type object_type not null
);

-- A "memorable" point in the development history of a project.
--
-- Synonyms/mappings:
-- * git: tag (of the annotated kind, otherwise they are just references)
-- * tarball: the release version number
create table release
(
  id          sha1_git,
  target      sha1_git,
  target_type object_type,
  date        timestamptz,
  date_offset smallint,
  date_neg_utc_offset  boolean,
  name        bytea,
  comment     bytea,
  author      bigint,
  synthetic   boolean not null default false,  -- true if synthetic (cf. swh-loader-tar)
  object_id   bigserial
);


-- Content provenance information caches
-- https://forge.softwareheritage.org/T547
--
-- Those tables aren't expected to be exhaustive, and get filled on a case by
-- case basis: absence of data doesn't mean the data is not there

-- content <-> revision mapping cache
--
-- semantics: "we have seen the content with given id in the given path inside
-- the given revision"

create table cache_content_revision (
    content         sha1_git not null,
    blacklisted     boolean default false,
    revision_paths  bytea[][]
);

create table cache_content_revision_processed (
    revision  sha1_git not null
);

-- revision <-> origin_visit mapping cache
--
-- semantics: "we have seen the given revision in the given origin during the
-- given visit"

create table cache_revision_origin (
   revision  sha1_git not null,
   origin    bigint not null,
   visit     bigint not null
);

-- Computing metadata on sha1's contents

create table indexer_configuration (
  id serial not null,
  tool_name text not null,
  tool_version text not null,
  tool_configuration jsonb
);

comment on table indexer_configuration is 'Indexer''s configuration version';
comment on column indexer_configuration.id is 'Tool identifier';
comment on column indexer_configuration.tool_version is 'Tool name';
comment on column indexer_configuration.tool_version is 'Tool version';
comment on column indexer_configuration.tool_configuration is 'Tool configuration: command line, flags, etc...';

-- Properties (mimetype, encoding, etc...)
create table content_mimetype (
  id sha1 not null,
  mimetype bytea not null,
  encoding bytea not null,
  indexer_configuration_id bigserial
);

comment on table content_mimetype is 'Metadata associated to a raw content';
comment on column content_mimetype.mimetype is 'Raw content Mimetype';
comment on column content_mimetype.encoding is 'Raw content encoding';
comment on column content_mimetype.indexer_configuration_id is 'Tool used to compute the information';

-- Language metadata
create table content_language (
  id sha1 not null,
  lang languages not null,
  indexer_configuration_id bigserial
);

comment on table content_language is 'Language information on a raw content';
comment on column content_language.lang is 'Language information';
comment on column content_language.indexer_configuration_id is 'Tool used to compute the information';

-- ctags information per content
create table content_ctags (
  id sha1 not null,
  name text not null,
  kind text not null,
  line bigint not null,
  lang ctags_languages not null,
  indexer_configuration_id bigserial
);

comment on table content_ctags is 'Ctags information on a raw content';
comment on column content_ctags.id is 'Content identifier';
comment on column content_ctags.name is 'Symbol name';
comment on column content_ctags.kind is 'Symbol kind (function, class, variable, const...)';
comment on column content_ctags.line is 'Symbol line';
comment on column content_ctags.lang is 'Language information for that content';
comment on column content_ctags.indexer_configuration_id is 'Tool used to compute the information';

create table fossology_license(
  id smallserial,
  name text not null
);

comment on table fossology_license is 'Possible license recognized by license indexer';
comment on column fossology_license.id is 'License identifier';
comment on column fossology_license.name is 'License name';

create table content_fossology_license (
  id sha1 not null,
  license_id smallserial not null,
  indexer_configuration_id bigserial not null
);

comment on table content_fossology_license is 'license associated to a raw content';
comment on column content_fossology_license.id is 'Raw content identifier';
comment on column content_fossology_license.license_id is 'One of the content''s license identifier';
comment on column content_fossology_license.indexer_configuration_id is 'Tool used to compute the information';
