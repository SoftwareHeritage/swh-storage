
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

-- create a temporary table for directory entries called tmp_directory_entry_TBLNAME, mimicking existing table
-- directory_entry_TBLNAME with an extra dir_id (sha1_git) column
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
	    on commit drop; alter table tmp_%I alter id set default nextval(''%I_id_seq'');
	', tblname, tblname, tblname, tblname);
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

-- Add directory_entry_dir entries
create or replace function swh_directory_entry_dir_add()
    returns void
    language plpgsql
as $$
begin
    with inserted_entries as (
        insert into directory_entry_dir (
            target, perms, atime, mtime, ctime, name
        )
        select t.target, t.perms, t.atime, t.mtime, t.ctime, t.name
        from tmp_directory_entry_dir t
        returning id, target, perms, atime, mtime, ctime, name
    ) insert into directory_list_dir (entry_id, dir_id)
      select i.id, t.dir_id from tmp_directory_entry_dir t left join inserted_entries i on
      t.target = i.target and t.perms = i.perms and coalesce(t.atime, '-infinity') = coalesce(i.atime, '-infinity') and coalesce(t.ctime, '-infinity') = coalesce(i.ctime, '-infinity') and coalesce(t.mtime, '-infinity') = coalesce(i.mtime, '-infinity') and t.name = i.name;
    return;
end
$$;

-- Add directory_entry_file entries
create or replace function swh_directory_entry_file_add()
    returns void
    language plpgsql
as $$
begin
    with inserted_entries as (
        insert into directory_entry_file (
            target, perms, atime, mtime, ctime, name
        )
        select t.target, t.perms, t.atime, t.mtime, t.ctime, t.name
        from tmp_directory_entry_file t
        returning id, target, perms, atime, mtime, ctime, name
    ) insert into directory_list_file (entry_id, dir_id)
      select i.id, t.dir_id from tmp_directory_entry_file t left join inserted_entries i on
      t.target = i.target and t.perms = i.perms and coalesce(t.atime, '-infinity') = coalesce(i.atime, '-infinity') and coalesce(t.ctime, '-infinity') = coalesce(i.ctime, '-infinity') and coalesce(t.mtime, '-infinity') = coalesce(i.mtime, '-infinity') and t.name = i.name;
    return;
end
$$;

-- Add directory_entry_rev entries
create or replace function swh_directory_entry_rev_add()
    returns void
    language plpgsql
as $$
begin
    with inserted_entries as (
        insert into directory_entry_rev (
            target, perms, atime, mtime, ctime, name
        )
        select t.target, t.perms, t.atime, t.mtime, t.ctime, t.name
        from tmp_directory_entry_rev t
        returning id, target, perms, atime, mtime, ctime, name
    ) insert into directory_list_rev (entry_id, dir_id)
      select i.id, t.dir_id from tmp_directory_entry_rev t left join inserted_entries i on
      t.target = i.target and t.perms = i.perms and coalesce(t.atime, '-infinity') = coalesce(i.atime, '-infinity') and coalesce(t.ctime, '-infinity') = coalesce(i.ctime, '-infinity') and coalesce(t.mtime, '-infinity') = coalesce(i.mtime, '-infinity') and t.name = i.name;
    return;
end
$$;
