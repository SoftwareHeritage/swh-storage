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
      values(24, now(), 'Work In Progress');

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

-- An origin is a place, identified by an URL, where software can be found. We
-- support different kinds of origins, e.g., git and other VCS repositories,
-- web pages that list tarballs URLs (e.g., http://www.kernel.org), indirect
-- tarball URLs (e.g., http://www.example.org/latest.tar.gz), etc. The key
-- feature of an origin is that it can be *fetched* (wget, git clone, svn
-- checkout, etc.) to retrieve all the contained software.
create table origin
(
  id         bigserial primary key,
  type       text, -- TODO use an enum here (?)
  url        text not null
);

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
create unique index on content(sha256);
create index on content(ctime);  -- TODO use a BRIN index here (postgres >= 9.5)

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
  unique (sha1, sha1_git, sha256)
);

-- those indexes support multiple NULL values.
create unique index on skipped_content(sha1);
create unique index on skipped_content(sha1_git);
create unique index on skipped_content(sha256);

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
  rev_entries   bigint[]   -- mounted revisions, reference directory_entry_rev
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
  committer_date        timestamptz,
  committer_date_offset smallint,
  type                  revision_type not null,
  directory             sha1_git,  -- file-system tree
  message               bytea,
  author                bigint references person(id),
  committer             bigint references person(id),
  synthetic             boolean not null default false  -- true if synthetic (cf. swh-loader-tar)
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
  revision   sha1_git,  -- ref target, e.g., commit id
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

create index on occurrence_history(revision);

-- Materialized view of occurrence_history, storing the *current* value of each
-- branch, as last seen by SWH.
create table occurrence
(
  origin    bigint references origin(id),
  branch    text,
  revision  sha1_git,
  primary key(origin, branch, revision)
);

-- A "memorable" point in the development history of a project.
--
-- Synonyms/mappings:
-- * git: tag (of the annotated kind, otherwise they are just references)
-- * tarball: the release version number
create table release
(
  id          sha1_git primary key,
  revision    sha1_git,
  date        timestamptz,
  date_offset smallint,
  name        text,
  comment     bytea,
  author      bigint references person(id),
  synthetic   boolean not null default false  -- true if synthetic (cf. swh-loader-tar)
);

create index on release(revision);
