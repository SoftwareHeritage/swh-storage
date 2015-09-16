
-- create a temporary table called tmp_TBLNAME, mimicking existing table
-- TBLNAME
--
-- Args:
--     tblname: name of the table to mimick
create or replace function swh_mktemp(tblname regclass)
    returns void
    language plpgsql
as $$
begin
    execute format('
	create temporary table tmp_%I
	    (like %I including defaults)
	    on commit drop
	', tblname, tblname);
    return;
end
$$;

-- create a temporary table for directory entries called tmp_TBLNAME,
-- mimicking existing table TBLNAME with an extra dir_id (sha1_git)
-- column, and dropping the id column.
--
-- This is used to create the tmp_directory_entry_<foo> tables.
--
-- Args:
--     tblname: name of the table to mimick
create or replace function swh_mktemp_dir_entry(tblname regclass)
    returns void
    language plpgsql
as $$
begin
    execute format('
	create temporary table tmp_%I
	    (like %I including defaults, dir_id sha1_git)
	    on commit drop;
        alter table tmp_%I drop column id;
	', tblname, tblname, tblname, tblname);
    return;
end
$$;

-- create a temporary table for revisions called tmp_revisions,
-- mimicking existing table revision, replacing the foreign keys to
-- people with an email and name field
--
create or replace function swh_mktemp_revision()
    returns void
    language plpgsql
as $$
begin
    create temporary table tmp_revision (
        like revision including defaults,
        author_name text not null default '',
        author_email text not null default '',
        committer_name text not null default '',
        committer_email text not null default ''
    ) on commit drop;
    alter table tmp_revision drop column author;
    alter table tmp_revision drop column committer;
    return;
end
$$;

-- a content signature is a set of cryptographic checksums that we use to
-- uniquely identify content, for the purpose of verifying if we already have
-- some content or not during content injection
create type content_signature as (
    sha1      sha1,
    sha1_git  sha1_git,
    sha256    sha256
);


-- check which entries of tmp_content are missing from content
--
-- operates in bulk: 0. swh_mktemp(content), 1. COPY to tmp_content,
-- 2. call this function
create or replace function swh_content_missing()
    returns setof content_signature
    language plpgsql
as $$
begin
    return query
	select sha1, sha1_git, sha256 from tmp_content
	except
	select sha1, sha1_git, sha256 from content;
    return;
end
$$;


-- add tmp_content entries to content, skipping duplicates
--
-- operates in bulk: 0. swh_mktemp(content), 1. COPY to tmp_content,
-- 2. call this function
create or replace function swh_content_add()
    returns void
    language plpgsql
as $$
declare
    rows bigint;
begin
    insert into content (sha1, sha1_git, sha256, length, status)
	select distinct sha1, sha1_git, sha256, length, status
	from tmp_content
	where (sha1, sha1_git, sha256) in
	    (select * from swh_content_missing());
	    -- TODO XXX use postgres 9.5 "UPSERT" support here, when available.
	    -- Specifically, using "INSERT .. ON CONFLICT IGNORE" we can avoid
	    -- the extra swh_content_missing() query here.
    return;
end
$$;


-- check which entries of tmp_directory are missing from directory
--
-- operates in bulk: 0. swh_mktemp(directory), 1. COPY to tmp_directory,
-- 2. call this function
create or replace function swh_directory_missing()
    returns setof directory
    language plpgsql
as $$
begin
    return query
	select id from tmp_directory
	except
	select id from directory;
    return;
end
$$;

-- Add tmp_directory_entry_dir entries to directory_entry_dir and
-- directory_list_dir, skipping duplicates in directory_entry_dir.
--
-- operates in bulk: 0. swh_mktemp_dir_entry('directory_entry_dir'), 1 COPY to
-- tmp_directory_entry_dir, 2. call this function
create or replace function swh_directory_entry_dir_add()
    returns void
    language plpgsql
as $$
begin
    insert into directory_entry_dir (target, name, perms, atime, mtime, ctime)
    select t.target, t.name, t.perms, t.atime, t.mtime, t.ctime
    from tmp_directory_entry_dir t
    where not exists (
    select 1
    from directory_entry_dir i
    where t.target = i.target and t.name = i.name and t.perms = i.perms and
       t.atime is not distinct from i.atime and
       t.mtime is not distinct from i.mtime and
       t.ctime is not distinct from i.ctime);

    insert into directory_list_dir (entry_id, dir_id)
    select i.id, t.dir_id
    from tmp_directory_entry_dir t
    inner join directory_entry_dir i
    on t.target = i.target and t.name = i.name and t.perms = i.perms and
       t.atime is not distinct from i.atime and
       t.mtime is not distinct from i.mtime and
       t.ctime is not distinct from i.ctime;
    return;
end
$$;

-- Add tmp_directory_entry_file entries to directory_entry_file and
-- directory_list_file, skipping duplicates in directory_entry_file.
--
-- operates in bulk: 0. swh_mktemp_dir_entry('directory_entry_file'), 1 COPY to
-- tmp_directory_entry_file, 2. call this function
create or replace function swh_directory_entry_file_add()
    returns void
    language plpgsql
as $$
begin
    insert into directory_entry_file (target, name, perms, atime, mtime, ctime)
    select t.target, t.name, t.perms, t.atime, t.mtime, t.ctime
    from tmp_directory_entry_file t
    where not exists (
    select 1
    from directory_entry_file i
    where t.target = i.target and t.name = i.name and t.perms = i.perms and
       t.atime is not distinct from i.atime and
       t.mtime is not distinct from i.mtime and
       t.ctime is not distinct from i.ctime);

    insert into directory_list_file (entry_id, dir_id)
    select i.id, t.dir_id
    from tmp_directory_entry_file t
    inner join directory_entry_file i
    on t.target = i.target and t.name = i.name and t.perms = i.perms and
       t.atime is not distinct from i.atime and
       t.mtime is not distinct from i.mtime and
       t.ctime is not distinct from i.ctime;
    return;
end
$$;

-- Add tmp_directory_entry_rev entries to directory_entry_rev and
-- directory_list_rev, skipping duplicates in directory_entry_rev.
--
-- operates in bulk: 0. swh_mktemp_dir_entry('directory_entry_rev'), 1 COPY to
-- tmp_directory_entry_rev, 2. call this function
create or replace function swh_directory_entry_rev_add()
    returns void
    language plpgsql
as $$
begin
    insert into directory_entry_rev (target, name, perms, atime, mtime, ctime)
    select t.target, t.name, t.perms, t.atime, t.mtime, t.ctime
    from tmp_directory_entry_rev t
    where not exists (
    select 1
    from directory_entry_rev i
    where t.target = i.target and t.name = i.name and t.perms = i.perms and
       t.atime is not distinct from i.atime and
       t.mtime is not distinct from i.mtime and
       t.ctime is not distinct from i.ctime);

    insert into directory_list_rev (entry_id, dir_id)
    select i.id, t.dir_id
    from tmp_directory_entry_rev t
    inner join directory_entry_rev i
    on t.target = i.target and t.name = i.name and t.perms = i.perms and
       t.atime is not distinct from i.atime and
       t.mtime is not distinct from i.mtime and
       t.ctime is not distinct from i.ctime;
    return;
end
$$;

-- a directory listing entry with all the metadata
--
-- can be used to list a directory, and retrieve all the data in one go.
create type directory_entry as
(
  dir_id  sha1_git,     -- id of the parent directory
  type    text,         -- type of entry (one of 'dir', 'file', 'rev')
  target  sha1_git,     -- id of target
  name    unix_path,    -- path name, relative to containing dir
  perms   file_perms,   -- unix-like permissions
  atime   timestamptz,  -- time of last access
  mtime   timestamptz,  -- time of last modification
  ctime   timestamptz   -- time of last status change
);

-- List a single level of directory walked_dir_id
create or replace function swh_directory_walk_one(walked_dir_id sha1_git)
    returns setof directory_entry
    language plpgsql
as $$
begin
    return query (
        select dir_id, 'dir' as type, target, name, perms, atime, mtime, ctime
	from directory_list_dir l
	left join directory_entry_dir d
	on l.entry_id = d.id
	where l.dir_id = walked_dir_id
    union
        select dir_id, 'file' as type, target, name, perms, atime, mtime, ctime
	from directory_list_file l
	left join directory_entry_file d
	on l.entry_id = d.id
	where l.dir_id = walked_dir_id
    union
        select dir_id, 'rev' as type, target, name, perms, atime, mtime, ctime
	from directory_list_rev l
	left join directory_entry_rev d
	on l.entry_id = d.id
	where l.dir_id = walked_dir_id
    ) order by name;
    return;
end
$$;

-- List missing revisions from tmp_revision
create or replace function swh_revision_missing()
    returns setof sha1_git
    language plpgsql
as $$
begin
    return query
        select id from tmp_revision
	except
	select id from revision;
    return;
end
$$;
