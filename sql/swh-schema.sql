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
      values(11, now(), 'Work In Progress');

-- a SHA1 checksum (not necessarily originating from Git)
create domain sha1 as text;

-- a Git object ID, i.e., a SHA1 checksum
create domain sha1_git as text;

-- a SHA256 checksum
create domain sha256 as text;

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
  status    content_status not null default 'visible'
);

create unique index on content(sha1_git);
-- create unique index on content(sha256);

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
-- been done in the past, or are still ongoing. Similar to fetch_history, but
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
-- tables: directory_entry_{dir,file}).
--
-- To list the contents of a directory:
-- 1. list the contained directory_entry_dir using table directory_list_dir
-- 2. list the contained directory_entry_file using table directory_list_file
-- 3. UNION
--
-- Synonyms/mappings:
-- * git: tree
create table directory
(
  id  sha1_git primary key
);

-- A directory entry pointing to a sub-directory.
create table directory_entry_dir
(
  id      bigserial primary key,
  target  sha1_git references directory(id) deferrable initially deferred,
                 -- id of target directory
  name    text,  -- path name, relative to containing dir
  perms   file_perms,   -- unix-like permissions
  atime   timestamptz,  -- time of last access
  mtime   timestamptz,  -- time of last modification
  ctime   timestamptz   -- time of last status change
);

-- Mapping between directories and contained sub-directories.
create table directory_list_dir
(
  dir_id     sha1_git references directory(id),
  entry_id   bigint references directory_entry_dir(id),
  primary key (dir_id, entry_id)
);

-- A directory entry pointing to a file.
create table directory_entry_file
(
  id      bigserial primary key,
  target  sha1_git, -- id of target file
  name    text,  -- path name, relative to containing dir
  perms   file_perms,   -- unix-like permissions
  atime   timestamptz,  -- time of last access
  mtime   timestamptz,  -- time of last modification
  ctime   timestamptz   -- time of last status change
);

-- Mapping between directories and contained files.
create table directory_list_file
(
  dir_id     sha1_git references directory(id),
  entry_id   bigint references directory_entry_file(id),
  primary key (dir_id, entry_id)
);

create table person
(
  id     bigserial primary key,
  name   text,
  email  text
);

create type revision_type as enum ('git', 'tar', 'dsc');

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
  id             sha1_git primary key,
  -- parent_ids   sha1_git[],  -- either this or the revision_history table
                               -- note: no FK allowed from arrays to columns
  date           timestamptz,
  committer_date timestamptz,
  type           revision_type not null,
  directory      sha1_git,  -- file-system tree
  message        text,
  author         bigint references person(id),
  committer      bigint references person(id)
);

-- either this table or the sha1_git[] column on the revision table
create table revision_history
(
  id           sha1_git references revision(id),
  parent_id    sha1_git,
  parent_rank  int not null default 0,
    -- parent position in merge commits, 0-based
  primary key (id, parent_id),
  unique (id, parent_rank)
);

-- The content of software origins is indexed starting from top-level pointers
-- called "branches". Every time we fetch some origin we store in this table
-- where the branches pointed to at fetch time.
--
-- Synonyms/mappings:
-- * git: ref (in the "git update-ref" sense)
create table occurrence_history
(
  origin     bigint references origin(id),
  branch     text,  -- e.g., "master" (for VCS), or "sid" (for Debian)
  revision   sha1_git references revision(id),  -- ref target, e.g., commit id
  authority  bigint references organization(id) not null,
                      -- who is claiming to have seen the occurrence.
                      -- Note: SWH is such an authority, and has an entry in
                      -- the organization table.
  validity   tstzrange,  -- The time validity of this table entry. If the upper
                         -- bound is missing, the entry is still valid.
  exclude using gist (origin with =,
                      branch with =,
                      revision with =,
                      authority with =,
                      validity with &&),
  -- unicity exclusion constraint on lines where the same value is found for
  -- `origin`, `reference`, `revision`, `authority` and overlapping values for
  -- `validity`.
  primary key (origin, branch, revision, authority, validity)
);

-- Materialized view of occurrence_history, storing the *current* value of each
-- branch, as last seen by SWH.
create table occurrence
(
  origin    bigint references origin(id),
  branch    text,
  revision  sha1_git references revision(id),
  primary key(origin, branch, revision)
);

-- A "memorable" point in the development history of a project.
--
-- Synonyms/mappings:
-- * git: tag (of the annotated kind, otherwise they are just references)
-- * tarball: the release version number
create table release
(
  id        sha1_git primary key,
  revision  sha1_git references revision(id),
  date      timestamptz,
  name      text,
  comment   text,
  author    bigint references person(id)
);
