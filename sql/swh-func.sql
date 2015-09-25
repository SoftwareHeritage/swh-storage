
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

-- create a temporary table for releases called tmp_release,
-- mimicking existing table release, replacing the foreign keys to
-- people with an email and name field
--
create or replace function swh_mktemp_release()
    returns void
    language plpgsql
as $$
begin
    create temporary table tmp_release (
        like release including defaults,
        author_name text not null default '',
        author_email text not null default ''
    ) on commit drop;
    alter table tmp_release drop column author;
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
    select distinct t.target, t.name, t.perms, t.atime, t.mtime, t.ctime
    from tmp_directory_entry_dir t
    where not exists (
    select 1
    from directory_entry_dir i
    where t.target = i.target and t.name = i.name and t.perms = i.perms and
       t.atime is not distinct from i.atime and
       t.mtime is not distinct from i.mtime and
       t.ctime is not distinct from i.ctime);

    insert into directory_list_dir (dir_id, entry_ids)
    select t.dir_id, array_agg(i.id)
    from tmp_directory_entry_dir t
    inner join directory_entry_dir i
    on t.target = i.target and t.name = i.name and t.perms = i.perms and
       t.atime is not distinct from i.atime and
       t.mtime is not distinct from i.mtime and
       t.ctime is not distinct from i.ctime
    group by t.dir_id;
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
    select distinct t.target, t.name, t.perms, t.atime, t.mtime, t.ctime
    from tmp_directory_entry_file t
    where not exists (
    select 1
    from directory_entry_file i
    where t.target = i.target and t.name = i.name and t.perms = i.perms and
       t.atime is not distinct from i.atime and
       t.mtime is not distinct from i.mtime and
       t.ctime is not distinct from i.ctime);

    insert into directory_list_file (dir_id, entry_ids)
    select t.dir_id, array_agg(i.id)
    from tmp_directory_entry_file t
    inner join directory_entry_file i
    on t.target = i.target and t.name = i.name and t.perms = i.perms and
       t.atime is not distinct from i.atime and
       t.mtime is not distinct from i.mtime and
       t.ctime is not distinct from i.ctime
    group by t.dir_id;
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
    select distinct t.target, t.name, t.perms, t.atime, t.mtime, t.ctime
    from tmp_directory_entry_rev t
    where not exists (
    select 1
    from directory_entry_rev i
    where t.target = i.target and t.name = i.name and t.perms = i.perms and
       t.atime is not distinct from i.atime and
       t.mtime is not distinct from i.mtime and
       t.ctime is not distinct from i.ctime);

    insert into directory_list_rev (dir_id, entry_ids)
    select t.dir_id, array_agg(i.id)
    from tmp_directory_entry_rev t
    inner join directory_entry_rev i
    on t.target = i.target and t.name = i.name and t.perms = i.perms and
       t.atime is not distinct from i.atime and
       t.mtime is not distinct from i.mtime and
       t.ctime is not distinct from i.ctime
    group by t.dir_id;
    return;
end
$$;

create type directory_entry_type as enum('file', 'dir', 'rev');

-- a directory listing entry with all the metadata
--
-- can be used to list a directory, and retrieve all the data in one go.
create type directory_entry as
(
  dir_id  sha1_git,     -- id of the parent directory
  type    directory_entry_type,  -- type of entry
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
        (with l as (select dir_id, unnest(entry_ids) as entry_id from directory_list_dir where dir_id = walked_dir_id)
	select dir_id, 'dir'::directory_entry_type as type, target, name, perms, atime, mtime, ctime
	from l
	left join directory_entry_dir d
	on l.entry_id = d.id)
    union
        (with l as (select dir_id, unnest(entry_ids) as entry_id from directory_list_file where dir_id = walked_dir_id)
        select dir_id, 'file'::directory_entry_type as type, target, name, perms, atime, mtime, ctime
	from l
	left join directory_entry_file d
	on l.entry_id = d.id)
    union
        (with l as (select dir_id, unnest(entry_ids) as entry_id from directory_list_rev where dir_id = walked_dir_id)
        select dir_id, 'rev'::directory_entry_type as type, target, name, perms, atime, mtime, ctime
	from l
	left join directory_entry_rev d
	on l.entry_id = d.id)
    ) order by name;
    return;
end
$$;

-- List all revision IDs starting from a given revision, going back in time
--
-- TODO ordering: should be breadth-first right now (what do we want?)
-- TODO ordering: ORDER BY parent_rank somewhere?
create or replace function swh_revision_list(root_revision sha1_git)
    returns setof sha1_git
    language plpgsql
as $$
begin
    return query
	with recursive rev_list(id) as (
	    (select id from revision where id = root_revision)
	    union
	    (select parent_id
	     from revision_history as h
	     join rev_list on h.id = rev_list.id)
	)
	select * from rev_list;
    return;
end
$$;

-- Detailed entry in a revision log
create type revision_log_entry as
(
  id                     sha1_git,
  date                   timestamptz,
  date_offset            smallint,
  committer_date         timestamptz,
  committer_date_offset  smallint,
  type                   revision_type,
  directory              sha1_git,
  message                bytea,
  author_name            text,
  author_email           text,
  committer_name         text,
  committer_email        text
);

-- "git style" revision log. Similar to swh_revision_list(), but returning all
-- information associated to each revision, and expanding authors/committers
create or replace function swh_revision_log(root_revision sha1_git)
    returns setof revision_log_entry
    language plpgsql
as $$
begin
    return query
        select revision.id, date, date_offset,
	    committer_date, committer_date_offset,
	    type, directory, message,
	    author.name as author_name, author.email as author_email,
	    committer.name as committer_name, committer.email as committer_email
	from swh_revision_list(root_revision) as rev_list
	join revision on revision.id = rev_list
	join person as author on revision.author = author.id
	join person as committer on revision.committer = committer.id;
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


-- Create entries in person from tmp_revision
create or replace function swh_person_add_from_revision()
    returns void
    language plpgsql
as $$
begin
    with t as (
        select author_name as name, author_email as email from tmp_revision
    union
        select committer_name as name, committer_email as email from tmp_revision
    ) insert into person (name, email)
    select distinct name, email from t
    where not exists (
        select 1
	from person p
	where t.name = p.name and t.email = p.email
    );
    return;
end
$$;

-- Create entries in revision from tmp_revision
create or replace function swh_revision_add()
    returns void
    language plpgsql
as $$
begin
    perform swh_person_add_from_revision();

    insert into revision (id, date, date_offset, committer_date, committer_date_offset, type, directory, message, author, committer)
    select t.id, t.date, t.date_offset, t.committer_date, t.committer_date_offset, t.type, t.directory, t.message, a.id, c.id
    from tmp_revision t
    left join person a on a.name = t.author_name and a.email = t.author_email
    left join person c on c.name = t.committer_name and c.email = t.committer_email;
    return;
end
$$;

-- List missing releases from tmp_release
create or replace function swh_release_missing()
    returns setof sha1_git
    language plpgsql
as $$
begin
    return query
        select id from tmp_release
	except
	select id from release;
    return;
end
$$;

-- Create entries in person from tmp_release
create or replace function swh_person_add_from_release()
    returns void
    language plpgsql
as $$
begin
    with t as (
        select distinct author_name as name, author_email as email from tmp_release
    ) insert into person (name, email)
    select name, email from t
    where not exists (
        select 1
	from person p
	where t.name = p.name and t.email = p.email
    );
    return;
end
$$;

-- Create entries in release from tmp_release
create or replace function swh_release_add()
    returns void
    language plpgsql
as $$
begin
    perform swh_person_add_from_release();

    insert into release (id, revision, date, date_offset, name, comment, author)
    select t.id, t.revision, t.date, t.date_offset, t.name, t.comment, a.id
    from tmp_release t
    left join person a on a.name = t.author_name and a.email = t.author_email;
    return;
end
$$;
