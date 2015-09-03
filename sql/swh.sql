---
--- Software Heritage Data Model
---

begin;


create extension if not exists btree_gist;


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
      values(6, now(), 'Work In Progress');

-- a Git object ID, i.e., a SHA1 checksum
-- TODO rename this to (swh_?)object_id
create domain git_object_id as text;

-- a SHA1 checksum (not necessarily originating from Git)
create domain sha1 as text;

-- a SHA256 checksum
create domain sha256 as text;

-- a set of UNIX-like access permissions, as manipulated by, e.g., chmod
create domain file_perms as int;

-- An organization (or part thereof) that might be in charge of running
-- software projects. Examples: Debian, GNU, GitHub, Apache, The Linux
-- Foundation. The data model is hierarchical (via parent_id) and might store
-- sub-branches of existing organizations. The key feature of an organization
-- is that it can be *listed* to retrieve information about its content, i.e:
-- sub-organizations, projects, origins.
create table organization
(
  id           bigserial primary key,
  parent_id    bigint references organization(id),
  name         text not null,
  description  text,
  homepage     text,
  list_engine  text,  -- crawler to be used to org's content
  list_url     text,  -- root URL to start the listing
  list_params  json,  -- org-specific listing parameter
  latest_list  timestamptz  -- last time the org's content has been listed
);

-- Log of all organization listings (i.e., organization crawling) that have
-- been done int he past, or are still ongoing. Similar to fetch_history, but
-- for organizations.
create table list_history
(
  id           bigserial primary key,
  organization bigint references organization(id),
  date         timestamptz not null,
  status       boolean,  -- true if and only if the listing has been successful
  result       json,     -- more detailed return value, depending on status
  stdout       text,
  stderr       text,
  duration     interval  -- fetch duration of NULL if still ongoing
);

-- An origin is a place, identified by an URL, where software can be found. We
-- support different kinds of origins, e.g., git and other VCS repositories,
-- web pages that list tarballs URLs (e.g., http://www.kernel.org), indirect
-- tarball URLs (e.g., http://www.example.org/latest.tar.gz), etc. The key
-- feature of an origin is that it can be *fetched* (wget, git clone, svn
-- checkout, etc.) to retrieve all the contained software.
create table origin
(
  id         bigserial primary key,
  parent_id  bigint references origin(id),  -- TODO for nested tarballs (?)
  type       text, -- TODO use an enum here (?)
  url        text not null
);

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

-- A specific software project, e.g., the Linux kernel, Apache httpd. A
-- software project is version-less at this level, but is associated to several
-- metadata. Metadata can evolve over time, this table only contains the most
-- recent version of them; for old versions of project see table
-- project_history.
create table project
(
  id            bigserial primary key,
  organization  bigint references organization(id),  -- the "owning" organization
  origin        bigint references origin(id),  -- where to find project releases
  name          text,
  description   text,
  homepage      text,
  doap          jsonb
  -- other kinds of metadata/software project description ontologies can be
  -- added here, in addition to DOAP
);

-- History of project metadata. Time-sensitive version of the table project.
create table project_history
(
  id            bigserial primary key,
  project       bigint references project(id),
  validity      tstzrange,
  organization  bigint references organization(id),
  origin        bigint references origin(id),
  name          text,
  description   text,
  homepage      text,
  doap          jsonb
);

-- A file-system directory.  A directory is a list of directory entries (see
-- table directory_entry).
--
-- Synonyms/mappings:
-- * git: tree
create table directory
(
  id  git_object_id primary key
);

-- Directory entry types: file or directory.
create type directory_entry_type as enum ('file', 'directory');

-- An individual entry contained in a directory.  A directory entry points to
-- either a file (= a leaf) or a directory (= an inner node). In both cases,
-- the pointer is a git_object_id. In the former case (files), the pointer can
-- be resolved using the content table; in the latter case (directories) using
-- the directory table.
create table directory_entry
(
  name       text,  -- path name, relative to parent entry or root
  id         git_object_id,  -- id of the object pointed to by this entry;
                             -- might be a file or sub-dir, depending on type
  type       directory_entry_type,  -- whether the entry is a file or a dir
  perms      file_perms,   -- unix-like permissions
  atime      timestamptz,  -- time of last access
  mtime      timestamptz,  -- time of last modification
  ctime      timestamptz,  -- time of last status change
  directory  git_object_id references directory(id),  -- containing directory
  primary key (name, directory)
);

create table person
(
  id     bigserial primary key,
  name   text,
  email  text
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
  id         git_object_id primary key,
  -- parent_ids   git_object_id[],  -- either this or the revision_history table
                                    -- note: no FK allowed from arrays to columns
  date       timestamptz,
  directory  git_object_id references directory(id),  -- file-system tree
  message    text,
  author     bigint references person(id),
  committer  bigint references person(id)
);

-- either this table or the git_object_id[] column on the revision table
create table revision_history
(
  id          git_object_id references revision(id), -- deferrable,
  parent_id   git_object_id references revision(id), -- deferrable,
  primary key (id, parent_id)
  -- TODO: note: if we use FK for parent_id here, we lose the ability to
  -- represent the fact that a commit points to another commit that does not
  -- exist in the revision table
  -- `deferrable` -> checked only at the transaction's end
);

-- The content of software origins is indexed starting from top-level pointers
-- called "references". Every time we fetch some origin we store in this table
-- where the references pointed to at fetch time.
--
-- Synonyms/mappings:
-- * git: ref (in the "git update-ref" sense)
-- * TODO what is the ref equivalent for other VCS?
-- * TODO what is the ref equivalent for tarballs?
create table occurrence_history
(
  origin       bigint references origin(id),
  reference    text,  -- ref name, e.g., "master"
  revision     git_object_id references revision(id),  -- ref target, e.g., commit id
  validity     tstzrange,  -- The time validity of this table entry. If the upper
                           -- bound is missing, the entry is still valid.
  exclude using gist (origin with =,
                      reference with =,
                      revision with =,
                      validity with &&)
  -- unicity exclusion constraint on lines where the same value is found for
  -- `origin`, `reference`, `revision` and overlapping values for `validity`.
);

-- TODO do we still need this table?
-- create table reference
-- (
--   sha1         git_object_id primary key
-- );

-- Materialized view of occurrence_history, storing the *current* value of each
-- reference.
create table occurrence
(
  origin     bigint references origin(id),
  reference  text,
  revision   git_object_id references revision(id),
  primary key(origin, reference, revision)
);

-- A "memorable" point in the development history of a project.
--
-- Synonyms/mappings:
-- * git: tag (of the annotated kind, otherwise they are just references)
-- * tarball: the release version number
create table release
(
  id        git_object_id primary key,
  revision  git_object_id references revision(id),
  date      timestamptz,
  name      text,
  comment   text,
  author     bigint references person(id)
);

-- Checksums about actual file content. Note that the content itself is not
-- stored in the DB, but on external (key-value) storage. A single checksum is
-- used as key there, but the other can be used to verify that we do not inject
-- content collisions not knowingly.
create table content
(
  id      git_object_id primary key,
  sha1    sha1   not null,
  sha256  sha256 not null,
  length  bigint not null
);

-- -- TODO work in progress
-- create table directory
-- (
--   id  git_object_id primary key
-- );

-- -- TODO work in progress
-- create table directory_path
-- (
--   directory         git_object_id not null,
--   directory_parent  git_object_id not null,
--   name              text,
--   atime             timestamptz,
--   ctime             timestamptz,
--   mtime             timestamptz,
--   perms             file_perms,
--   primary key(directory, directory_parent)
-- );

-- -- TODO work in progress
-- create table file
-- (
--   id         git_object_id references content(id),
--   directory  git_object_id references directory(id),
--   name       text,
--   primary key(id)
-- );


commit;
