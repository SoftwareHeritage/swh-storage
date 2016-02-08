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
      values(57, now(), 'Work In Progress');

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

create type content_status as enum ('absent', 'visible', 'hidden');

-- Checksums about actual file content. Note that the content itself is not
-- stored in the DB, but on external (key-value) storage. A single checksum is
-- used as key there, but the other can be used to verify that we do not inject
-- content collisions not knowingly.
create table content
(
  sha1      sha1 primary key,
  sha1_git  sha1_git not null,
  sha256    sha256 not null,
  length    bigint not null,
  ctime     timestamptz not null default now(),
            -- creation time, i.e. time of (first) injection into the storage
  status    content_status not null default 'visible',
  object_id bigserial
);

create unique index on content(sha1_git);
create unique index on content(sha256);
create index on content(ctime);  -- TODO use a BRIN index here (postgres >= 9.5)

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

-- Types of entities.
--
-- - organization: a root entity, usually backed by a non-profit, a
-- company, or another kind of "association". (examples: Software
-- Heritage, Debian, GNU, GitHub)
--
-- - group_of_entities: used for hierarchies, doesn't need to have a
-- concrete existence. (examples: GNU hosting facilities, Debian
-- hosting facilities, GitHub users, ...)
--
-- - hosting: a hosting facility, can usually be listed to generate
-- other data. (examples: GitHub git hosting, alioth.debian.org,
-- snapshot.debian.org)
--
-- - group_of_persons: an entity representing a group of
-- persons. (examples: a GitHub organization, a Debian team)
--
-- - person: an entity representing a person. (examples:
-- a GitHub user, a Debian developer)
--
-- - project: an entity representing a software project. (examples: a
-- GitHub project, Apache httpd, a Debian source package, ...)
create type entity_type as enum (
  'organization',
  'group_of_entities',
  'hosting',
  'group_of_persons',
  'person',
  'project'
);

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
  id               bigserial primary key,
  uuid             uuid,
  parent           uuid,             -- should reference entity_history(uuid)
  name             text not null,
  type             entity_type not null,
  description      text,
  homepage         text,
  active           boolean not null, -- whether the entity was seen on the last listing
  generated        boolean not null, -- whether this entity has been generated by a lister
  lister           uuid,             -- should reference entity_history(uuid)
  lister_metadata  jsonb,            -- lister-specific metadata
  doap             jsonb,            -- metadata in doap format
  validity         timestamptz[]     -- timestamps at which we have seen this entity
);

create index on entity_history(uuid);
create index on entity_history(name);

-- The entity table provides a view of the latest information on a
-- given entity. It is updated via a trigger on entity_history.
create table entity
(
  uuid             uuid primary key,
  parent           uuid references entity(uuid) deferrable initially deferred,
  name             text not null,
  type             entity_type not null,
  description      text,
  homepage         text,
  active           boolean not null, -- whether the entity was seen on the last listing
  generated        boolean not null, -- whether this entity has been generated by a lister
  lister           uuid,             -- the entity that created this entity.
                                     --   References listable_entity(uuid) later on.
  lister_metadata  jsonb,            -- lister-specific metadata
  doap             jsonb,            -- metadata in doap format
  last_seen        timestamptz,      -- last listing time or disappearance time for active=false
  last_id          bigint references entity_history(id) -- last listing id
);

create index on entity(name);

-- Register the equivalence between two entities. Allows sideways
-- navigation in the entity table
create table entity_equivalence
(
  entity1 uuid references entity(uuid),
  entity2 uuid references entity(uuid),
  primary key (entity1, entity2),
  constraint order_entities check (entity1 < entity2)
);

-- Register a lister for a specific entity.
create table listable_entity
(
  uuid         uuid references entity(uuid) primary key,
  enabled      boolean not null default true, -- do we list this entity automatically?
  list_engine  text,  -- crawler to be used to list entity's content
  list_url     text,  -- root URL to start the listing
  list_params  json,  -- org-specific listing parameter
  latest_list  timestamptz  -- last time the entity's content has been listed
);

-- Add the circular foreign key on entity(lister) -> listable_entity(uuid)
alter table entity add foreign key (lister) references listable_entity(uuid);

-- Log of all entity listings (i.e., entity crawling) that have been
-- done in the past, or are still ongoing.
create table list_history
(
  id        bigserial primary key,
  entity    uuid references listable_entity(uuid),
  date      timestamptz not null,
  status    boolean,  -- true if and only if the listing has been successful
  result    json,     -- more detailed return value, depending on status
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
  id       bigserial primary key,
  type     text, -- TODO use an enum here (?)
  url      text not null,
  lister   uuid references listable_entity(uuid),
  project  uuid references entity(uuid)
);

create index on origin(type, url);

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
  origin    bigint references origin(id),
  object_id bigserial,
  unique (sha1, sha1_git, sha256)
);

-- those indexes support multiple NULL values.
create unique index on skipped_content(sha1);
create unique index on skipped_content(sha1_git);
create unique index on skipped_content(sha256);


-- Log of all origin fetches (i.e., origin crawling) that have been done in the
-- past, or are still ongoing. Similar to list_history, but for origins.
create table fetch_history
(
  id        bigserial primary key,
  origin    bigint references origin(id),
  date      timestamptz not null,
  status    boolean,  -- true if and only if the fetch has been successful
  result    json,     -- more detailed returned values, times, etc...
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
  id            sha1_git primary key,
  dir_entries   bigint[],  -- sub-directories, reference directory_entry_dir
  file_entries  bigint[],  -- contained files, reference directory_entry_file
  rev_entries   bigint[],  -- mounted revisions, reference directory_entry_rev
  object_id     bigserial  -- short object identifier
);

create index on directory using gin (dir_entries);
create index on directory using gin (file_entries);
create index on directory using gin (rev_entries);

-- A directory entry pointing to a sub-directory.
create table directory_entry_dir
(
  id      bigserial primary key,
  target  sha1_git,   -- id of target directory
  name    unix_path,  -- path name, relative to containing dir
  perms   file_perms  -- unix-like permissions
);

create unique index on directory_entry_dir(target, name, perms);

-- A directory entry pointing to a file.
create table directory_entry_file
(
  id      bigserial primary key,
  target  sha1_git,   -- id of target file
  name    unix_path,  -- path name, relative to containing dir
  perms   file_perms  -- unix-like permissions
);

create unique index on directory_entry_file(target, name, perms);

-- A directory entry pointing to a revision.
create table directory_entry_rev
(
  id      bigserial primary key,
  target  sha1_git,   -- id of target revision
  name    unix_path,  -- path name, relative to containing dir
  perms   file_perms  -- unix-like permissions
);

create unique index on directory_entry_rev(target, name, perms);

create table person
(
  id     bigserial primary key,
  name   bytea not null default '',
  email  bytea not null default ''
);

create unique index on person(name, email);

create type revision_type as enum ('git', 'tar', 'dsc');

-- the data object types stored in our data model
create type object_type as enum ('content', 'directory', 'revision', 'release');

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
  id                    sha1_git primary key,
  date                  timestamptz,
  date_offset           smallint,
  date_neg_utc_offset   boolean,
  committer_date        timestamptz,
  committer_date_offset smallint,
  committer_date_neg_utc_offset boolean,
  type                  revision_type not null,
  directory             sha1_git,  -- file-system tree
  message               bytea,
  author                bigint references person(id),
  committer             bigint references person(id),
  metadata              jsonb, -- extra metadata (tarball checksums, extra commit information, etc...)
  synthetic             boolean not null default false,  -- true if synthetic (cf. swh-loader-tar)
  object_id             bigserial
);

create index on revision(directory);

-- either this table or the sha1_git[] column on the revision table
create table revision_history
(
  id           sha1_git references revision(id),
  parent_id    sha1_git,
  parent_rank  int not null default 0,
    -- parent position in merge commits, 0-based
  primary key (id, parent_rank)
);

create index on revision_history(parent_id);

-- The timestamps at which Software Heritage has made a visit of the given origin.
create table origin_visit
(
  origin  bigint not null references origin(id),
  visit   bigint not null,
  date    timestamptz not null,
  primary key (origin, visit)
);

create index on origin_visit(date);

-- The content of software origins is indexed starting from top-level pointers
-- called "branches". Every time we fetch some origin we store in this table
-- where the branches pointed to at fetch time.
--
-- Synonyms/mappings:
-- * git: ref (in the "git update-ref" sense)
create table occurrence_history
(
  origin       bigint references origin(id) not null,
  branch       bytea not null,        -- e.g., b"master" (for VCS), or b"sid" (for Debian)
  target       sha1_git not null,     -- ref target, e.g., commit id
  target_type  object_type not null,  -- ref target type
  object_id    bigserial not null,    -- short object identifier
  visits       bigint[] not null,     -- the visits where that occurrence was valid. References
                                      -- origin_visit(visit), where o_h.origin = origin_visit.origin.
  primary key (object_id)
);

create index on occurrence_history(target, target_type);
create index on occurrence_history(origin, branch);
create unique index on occurrence_history(origin, branch, target, target_type);

-- Materialized view of occurrence_history, storing the *current* value of each
-- branch, as last seen by SWH.
create table occurrence
(
  origin    bigint references origin(id) not null,
  branch    bytea not null,
  target    sha1_git not null,
  target_type object_type not null,
  primary key(origin, branch)
);

-- A "memorable" point in the development history of a project.
--
-- Synonyms/mappings:
-- * git: tag (of the annotated kind, otherwise they are just references)
-- * tarball: the release version number
create table release
(
  id          sha1_git primary key,
  target      sha1_git,
  target_type object_type,
  date        timestamptz,
  date_offset smallint,
  date_neg_utc_offset  boolean,
  name        bytea,
  comment     bytea,
  author      bigint references person(id),
  synthetic   boolean not null default false,  -- true if synthetic (cf. swh-loader-tar)
  object_id   bigserial
);

create index on release(target, target_type);
