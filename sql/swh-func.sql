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
    language sql
as $$
    create temporary table tmp_revision (
        like revision including defaults,
        author_name bytea not null default '',
        author_email bytea not null default '',
        committer_name bytea not null default '',
        committer_email bytea not null default ''
    ) on commit drop;
    alter table tmp_revision drop column author;
    alter table tmp_revision drop column committer;
$$;


-- create a temporary table for releases called tmp_release,
-- mimicking existing table release, replacing the foreign keys to
-- people with an email and name field
--
create or replace function swh_mktemp_release()
    returns void
    language sql
as $$
    create temporary table tmp_release (
        like release including defaults,
        author_name bytea not null default '',
        author_email bytea not null default ''
    ) on commit drop;
    alter table tmp_release drop column author;
$$;

create or replace function swh_mktemp_release_get()
    returns void
    language sql
as $$
    create temporary table tmp_release_get(
      id sha1_git primary key
    ) on commit drop;
$$;

-- create a temporary table for entity_history, sans id
create or replace function swh_mktemp_entity_history()
    returns void
    language sql
as $$
    create temporary table tmp_entity_history (
        like entity_history including defaults);
    alter table tmp_entity_history drop column id;
$$;

-- create a temporary table for entities called tmp_entity_lister,
-- with only the columns necessary for retrieving the uuid of a listed
-- entity.
create or replace function swh_mktemp_entity_lister()
    returns void
    language sql
as $$
    create temporary table tmp_entity_lister (
        id bigint,
        lister uuid,
	lister_metadata jsonb
    );
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
    -- This query is critical for (single-algorithm) hash collision detection,
    -- so we cannot rely only on the fact that a single hash (e.g., sha1) is
    -- missing from the table content to conclude that a given content is
    -- missing. Ideally, we would want to (try to) add to content all entries
    -- in tmp_content that, when considering all columns together, are missing
    -- from content.
    --
    -- But doing that naively would require a *compound* index on all checksum
    -- columns; that index would not be significantly smaller than the content
    -- table itself, and therefore won't be used. Therefore we union together
    -- all contents that differ on at least one column from what is already
    -- available. If there is a collision on some (but not all) columns, the
    -- relevant tmp_content entry will be included in the set of content to be
    -- added, causing a downstream violation of unicity constraint.
    return query
	(select sha1, sha1_git, sha256 from tmp_content as tmp
	 where not exists
	     (select 1 from content as c where c.sha1 = tmp.sha1))
	union
	(select sha1, sha1_git, sha256 from tmp_content as tmp
	 where not exists
	     (select 1 from content as c where c.sha1_git = tmp.sha1_git))
	union
	(select sha1, sha1_git, sha256 from tmp_content as tmp
	 where not exists
	     (select 1 from content as c where c.sha256 = tmp.sha256));
    return;
end
$$;


-- check which entries of tmp_skipped_content are missing from skipped_content
--
-- operates in bulk: 0. swh_mktemp(skipped_content), 1. COPY to tmp_skipped_content,
-- 2. call this function
create or replace function swh_skipped_content_missing()
    returns setof content_signature
    language plpgsql
as $$
begin
    return query
	select sha1, sha1_git, sha256 from tmp_skipped_content t
	where not exists
	(select 1 from skipped_content s where
	    s.sha1 is not distinct from t.sha1 and
	    s.sha1_git is not distinct from t.sha1_git and
	    s.sha256 is not distinct from t.sha256);
    return;
end
$$;


-- Look up content based on one or several different checksums. Return all
-- content information if the content is found; a NULL row otherwise.
--
-- At least one checksum should be not NULL. If several are not NULL, they will
-- be AND-ed together in the lookup query.
--
-- Note: this function is meant to be used to look up individual contents
-- (e.g., for the web app), for batch lookup of missing content (e.g., to be
-- added) see swh_content_missing
create or replace function swh_content_find(
    sha1      sha1     default NULL,
    sha1_git  sha1_git default NULL,
    sha256    sha256   default NULL
)
    returns content
    language plpgsql
as $$
declare
    con content;
    filters text[] := array[] :: text[];  -- AND-clauses used to filter content
    q text;
begin
    if sha1 is not null then
        filters := filters || format('sha1 = %L', sha1);
    end if;
    if sha1_git is not null then
        filters := filters || format('sha1_git = %L', sha1_git);
    end if;
    if sha256 is not null then
        filters := filters || format('sha256 = %L', sha256);
    end if;

    if cardinality(filters) = 0 then
        return null;
    else
        q = format('select * from content where %s',
	        array_to_string(filters, ' and '));
        execute q into con;
	return con;
    end if;
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


-- add tmp_skipped_content entries to skipped_content, skipping duplicates
--
-- operates in bulk: 0. swh_mktemp(skipped_content), 1. COPY to tmp_skipped_content,
-- 2. call this function
create or replace function swh_skipped_content_add()
    returns void
    language plpgsql
as $$
begin
    insert into skipped_content (sha1, sha1_git, sha256, length, status, reason, origin)
	select distinct sha1, sha1_git, sha256, length, status, reason, origin
	from tmp_skipped_content
	where (coalesce(sha1, ''), coalesce(sha1_git, ''), coalesce(sha256, '')) in
	    (select coalesce(sha1, ''), coalesce(sha1_git, ''), coalesce(sha256, '') from swh_skipped_content_missing());
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
    returns setof sha1_git
    language plpgsql
as $$
begin
    return query
	select id from tmp_directory t
	where not exists (
	    select 1 from directory d
	    where d.id = t.id);
    return;
end
$$;


-- Retrieve information on directory from temporary table
create or replace function swh_directory_get()
    returns setof directory
    language plpgsql
as $$
begin
    return query
	select d.id, d.dir_entries, d.file_entries, d.rev_entries
        from tmp_directory t
        inner join directory d on t.id = d.id;
    return;
end
$$;


create type directory_entry_type as enum('file', 'dir', 'rev');


-- Add tmp_directory_entry_* entries to directory_entry_* and directory,
-- skipping duplicates in directory_entry_*.  This is a generic function that
-- works on all kind of directory entries.
--
-- operates in bulk: 0. swh_mktemp_dir_entry('directory_entry_*'), 1 COPY to
-- tmp_directory_entry_*, 2. call this function
--
-- Assumption: this function is used in the same transaction that inserts the
-- context directory in table "directory".
create or replace function swh_directory_entry_add(typ directory_entry_type)
    returns void
    language plpgsql
as $$
begin
    execute format('
    insert into directory_entry_%1$s (target, name, perms)
    select distinct t.target, t.name, t.perms
    from tmp_directory_entry_%1$s t
    where not exists (
    select 1
    from directory_entry_%1$s i
    where t.target = i.target and t.name = i.name and t.perms = i.perms)
   ', typ);

    execute format('
    with new_entries as (
	select t.dir_id, array_agg(i.id) as entries
	from tmp_directory_entry_%1$s t
	inner join directory_entry_%1$s i
	using (target, name, perms)
	group by t.dir_id
    )
    update tmp_directory as d
    set %1$s_entries = new_entries.entries
    from new_entries
    where d.id = new_entries.dir_id
    ', typ);

    return;
end
$$;

-- Insert the data from tmp_directory, tmp_directory_entry_file,
-- tmp_directory_entry_dir, tmp_directory_entry_rev into their final
-- tables.
--
-- Prerequisites:
--  directory ids in tmp_directory
--  entries in tmp_directory_entry_{file,dir,rev}
--
create or replace function swh_directory_add()
    returns void
    language plpgsql
as $$
begin
    perform swh_directory_entry_add('file');
    perform swh_directory_entry_add('dir');
    perform swh_directory_entry_add('rev');

    insert into directory
    select * from tmp_directory t
    where not exists (
        select 1 from directory d
	where d.id = t.id);

    return;
end
$$;

-- a directory listing entry with all the metadata
--
-- can be used to list a directory, and retrieve all the data in one go.
create type directory_entry as
(
  dir_id   sha1_git,     -- id of the parent directory
  type     directory_entry_type,  -- type of entry
  target   sha1_git,     -- id of target
  name     unix_path,    -- path name, relative to containing dir
  perms    file_perms,   -- unix-like permissions
  status   content_status,  -- visible or absent
  sha1     sha1,            -- content if sha1 if type is not dir
  sha1_git sha1_git,        -- content's sha1 git if type is not dir
  sha256   sha256           -- content's sha256 if type is not dir
);


-- List a single level of directory walked_dir_id
-- FIXME: order by name is not correct. For git, we need to order by
-- lexicographic order but as if a trailing / is present in directory
-- name
create or replace function swh_directory_walk_one(walked_dir_id sha1_git)
    returns setof directory_entry
    language sql
    stable
as $$
    with dir as (
	select id as dir_id, dir_entries, file_entries, rev_entries
	from directory
	where id = walked_dir_id),
    ls_d as (select dir_id, unnest(dir_entries) as entry_id from dir),
    ls_f as (select dir_id, unnest(file_entries) as entry_id from dir),
    ls_r as (select dir_id, unnest(rev_entries) as entry_id from dir)
    (select dir_id, 'dir'::directory_entry_type as type,
            e.target, e.name, e.perms, NULL::content_status,
            NULL::sha1, NULL::sha1_git, NULL::sha256
     from ls_d
     left join directory_entry_dir e on ls_d.entry_id = e.id)
    union
    (select dir_id, 'file'::directory_entry_type as type,
            e.target, e.name, e.perms, c.status,
            c.sha1, c.sha1_git, c.sha256
     from ls_f
     left join directory_entry_file e on ls_f.entry_id = e.id
     left join content c on e.target = c.sha1_git)
    union
    (select dir_id, 'rev'::directory_entry_type as type,
            e.target, e.name, e.perms, NULL::content_status,
            NULL::sha1, NULL::sha1_git, NULL::sha256
     from ls_r
     left join directory_entry_rev e on ls_r.entry_id = e.id)
    order by name;
$$;

-- List recursively the content of a directory
create or replace function swh_directory_walk(walked_dir_id sha1_git)
    returns setof directory_entry
    language sql
    stable
as $$
    with recursive entries as (
        select dir_id, type, target, name, perms, status, sha1, sha1_git,
               sha256
        from swh_directory_walk_one(walked_dir_id)
        union all
        select dir_id, type, target, (dirname || '/' || name)::unix_path as name,
               perms, status, sha1, sha1_git, sha256
        from (select (swh_directory_walk_one(dirs.target)).*, dirs.name as dirname
              from (select target, name from entries where type = 'dir') as dirs) as with_parent
    )
    select dir_id, type, target, name, perms, status, sha1, sha1_git, sha256
    from entries
$$;

-- Find a directory entry by its path
create or replace function swh_find_directory_entry_by_path(
    walked_dir_id sha1_git,
    dir_or_content_path bytea[])
    returns directory_entry
    language plpgsql
as $$
declare
    end_index integer;
    paths bytea default '';
    path bytea;
    res bytea[];
    r record;
begin
    end_index := array_upper(dir_or_content_path, 1);
    res[1] := walked_dir_id;

    for i in 1..end_index
    loop
        path := dir_or_content_path[i];
        -- concatenate path for patching the name in the result record (if we found it)
        if i = 1 then
            paths = path;
        else
            paths := paths || '/' || path;  -- concatenate paths
        end if;

        if i <> end_index then
            select *
            from swh_directory_walk_one(res[i] :: sha1_git)
            where name=path
            and type = 'dir'
            limit 1 into r;
        else
            select *
            from swh_directory_walk_one(res[i] :: sha1_git)
            where name=path
            limit 1 into r;
        end if;

        -- find the path
        if r is null then
           return null;
        else
            -- store the next dir to lookup the next local path from
            res[i+1] := r.target;
        end if;
    end loop;

    -- at this moment, r is the result. Patch its 'name' with the full path before returning it.
    r.name := paths;
    return r;
end
$$;

-- List all revision IDs starting from a given revision, going back in time
--
-- TODO ordering: should be breadth-first right now (what do we want?)
-- TODO ordering: ORDER BY parent_rank somewhere?
create or replace function swh_revision_list(root_revision sha1_git, num_revs bigint default NULL)
    returns table (id sha1_git, parents bytea[])
    language sql
    stable
as $$
    with recursive full_rev_list(id) as (
        (select id from revision where id = root_revision)
        union
        (select h.parent_id
         from revision_history as h
         join full_rev_list on h.id = full_rev_list.id)
    ),
    rev_list as (select id from full_rev_list limit num_revs)
    select rev_list.id as id,
           array(select rh.parent_id::bytea
                 from revision_history rh
                 where rh.id = rev_list.id
                 order by rh.parent_rank
                ) as parent
    from rev_list;
$$;

-- List all the children of a given revision
create or replace function swh_revision_list_children(root_revision sha1_git, num_revs bigint default NULL)
    returns table (id sha1_git, parents bytea[])
    language sql
    stable
as $$
    with recursive full_rev_list(id) as (
        (select id from revision where id = root_revision)
        union
        (select h.id
         from revision_history as h
         join full_rev_list on h.parent_id = full_rev_list.id)
    ),
    rev_list as (select id from full_rev_list limit num_revs)
    select rev_list.id as id,
           array(select rh.parent_id::bytea
                 from revision_history rh
                 where rh.id = rev_list.id
                 order by rh.parent_rank
                ) as parent
    from rev_list;
$$;


-- Detailed entry for a revision
create type revision_entry as
(
  id                     sha1_git,
  date                   timestamptz,
  date_offset            smallint,
  committer_date         timestamptz,
  committer_date_offset  smallint,
  type                   revision_type,
  directory              sha1_git,
  message                bytea,
  author_name            bytea,
  author_email           bytea,
  committer_name         bytea,
  committer_email        bytea,
  metadata               jsonb,
  synthetic              boolean,
  parents                bytea[]
);


-- "git style" revision log. Similar to swh_revision_list(), but returning all
-- information associated to each revision, and expanding authors/committers
create or replace function swh_revision_log(root_revision sha1_git, num_revs bigint default NULL)
    returns setof revision_entry
    language sql
    stable
as $$
    select t.id, r.date, r.date_offset,
           r.committer_date, r.committer_date_offset,
           r.type, r.directory, r.message,
           a.name, a.email, c.name, c.email, r.metadata, r.synthetic,
           t.parents
    from swh_revision_list(root_revision, num_revs) as t
    left join revision r on t.id = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;


-- Retrieve revisions from tmp_revision in bulk
create or replace function swh_revision_get()
    returns setof revision_entry
    language plpgsql
as $$
begin
    return query
        select t.id, r.date, r.date_offset,
               r.committer_date, r.committer_date_offset,
               r.type, r.directory, r.message,
               a.name, a.email, c.name, c.email, r.metadata, r.synthetic,
         array(select rh.parent_id::bytea from revision_history rh where rh.id = t.id order by rh.parent_rank)
                   as parents
        from tmp_revision t
        left join revision r on t.id = r.id
        left join person a on a.id = r.author
        left join person c on c.id = r.committer;
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
        select id from tmp_revision t
	where not exists (
	    select 1 from revision r
	    where r.id = t.id);
    return;
end
$$;

-- Detailed entry for a release
create type release_entry as
(
  id          sha1_git,
  revision    sha1_git,
  date        timestamptz,
  date_offset smallint,
  name        text,
  comment     bytea,
  synthetic   boolean,
  author_name bytea,
  author_email bytea
);

-- Detailed entry for release
create or replace function swh_release_get()
    returns setof release_entry
    language plpgsql
as $$
begin
    return query
        select r.id, r.revision, r.date, r.date_offset, r.name, r.comment,
               r.synthetic, p.name as author_name, p.email as author_email
        from tmp_release_get t
        inner join release r on t.id = r.id
        inner join person p on p.id = r.author;
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

    insert into revision (id, date, date_offset, committer_date, committer_date_offset, type, directory, message, author, committer, metadata, synthetic)
    select t.id, t.date, t.date_offset, t.committer_date, t.committer_date_offset, t.type, t.directory, t.message, a.id, c.id, t.metadata, t.synthetic
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
        select id from tmp_release t
	where not exists (
	select 1 from release r
	where r.id = t.id);
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

    insert into release (id, revision, date, date_offset, name, comment, author, synthetic)
    select t.id, t.revision, t.date, t.date_offset, t.name, t.comment, a.id, t.synthetic
    from tmp_release t
    left join person a on a.name = t.author_name and a.email = t.author_email;
    return;
end
$$;

-- add tmp_occurrence_history entries to occurrence_history
--
-- operates in bulk: 0. swh_mktemp(occurrence_history), 1. COPY to tmp_occurrence_history,
-- 2. call this function
create or replace function swh_occurrence_history_add()
    returns void
    language plpgsql
as $$
begin
    -- Update intervals we have the data to update
    with new_intervals as (
        select t.origin, t.branch, t.authority, t.validity,
	       o.validity - t.validity as new_validity
	from tmp_occurrence_history t
        left join occurrence_history o
        using (origin, branch, authority)
	where o.origin is not null),
    -- do not update intervals if they would become empty (perfect overlap)
    to_update as (
        select * from new_intervals
	where not isempty(new_validity))
    update occurrence_history o set validity = t.new_validity
    from to_update t
    where o.origin = t.origin and o.branch = t.branch and o.authority = t.authority;

    -- Now only insert intervals that aren't already present
    insert into occurrence_history (origin, branch, revision, authority, validity)
	select distinct origin, branch, revision, authority, validity
	from tmp_occurrence_history t
	where not exists (
	    select 1 from occurrence_history o
	    where o.origin = t.origin and o.branch = t.branch and
	          o.authority = t.authority and o.revision = t.revision and
		  o.validity = t.validity);
    return;
end
$$;


-- Absolute path: directory reference + complete path relative to it
create type content_dir as (
    directory  sha1_git,
    path       unix_path
);


-- Find the containing directory of a given content, specified by sha1
-- (note: *not* sha1_git).
--
-- Return a pair (dir_it, path) where path is a UNIX path that, from the
-- directory root, reach down to a file with the desired content. Return NULL
-- if no match is found.
--
-- In case of multiple paths (i.e., pretty much always), an arbitrary one is
-- chosen.
create or replace function swh_content_find_directory(content_id sha1)
    returns content_dir
    language sql
    stable
as $$
    with recursive path as (
	-- Recursively build a path from the requested content to a root
	-- directory. Each iteration returns a pair (dir_id, filename) where
	-- filename is relative to dir_id. Stops when no parent directory can
	-- be found.
	(select dir.id as dir_id, dir_entry_f.name as name, 0 as depth
	 from directory_entry_file as dir_entry_f
	 join content on content.sha1_git = dir_entry_f.target
	 join directory as dir on dir.file_entries @> array[dir_entry_f.id]
	 where content.sha1 = content_id
	 limit 1)
	union all
	(select dir.id as dir_id,
		(dir_entry_d.name || '/' || path.name)::unix_path as name,
		path.depth + 1
	 from path
	 join directory_entry_dir as dir_entry_d on dir_entry_d.target = path.dir_id
	 join directory as dir on dir.dir_entries @> array[dir_entry_d.id]
	 limit 1)
    )
    select dir_id, name from path order by depth desc limit 1;
$$;


-- Walk the revision history starting from a given revision, until a matching
-- occurrence is found. Return all occurrence information if one is found, NULL
-- otherwise.
create or replace function swh_revision_find_occurrence(revision_id sha1_git)
    returns occurrence
    language sql
    stable
as $$
	select origin, branch, revision
  from swh_revision_list_children(revision_id) as rev_list
	left join occurrence_history occ_hist
  on rev_list.id = occ_hist.revision
	where occ_hist.origin is not null
	order by upper(occ_hist.validity)  -- TODO filter by authority?
	limit 1;
$$;


-- Retrieve occurrence by filtering on origin_id and optionally on
-- branch_name and/or validity range
create or replace function swh_occurrence_get_by(
       origin_id bigint,
       branch_name text default NULL,
       validity timestamptz default NULL)
    returns setof occurrence_history
    language plpgsql
as $$
declare
    filters text[] := array[] :: text[];  -- AND-clauses used to filter content
    q text;
begin
    if origin_id is not null then
        filters := filters || format('origin = %L', origin_id);
    end if;
    if branch_name is not null then
        filters := filters || format('branch = %L', branch_name);
    end if;
    if validity is not null then
        filters := filters || format('validity @> %L::timestamptz', validity);
    end if;

    if cardinality(filters) = 0 then
        raise exception 'At least one filter amongst (origin_id, branch_name, validity) is needed';
    else
        q = format('select * ' ||
                   'from occurrence_history ' ||
                   'where %s ' ||
                   'order by validity desc',
	        array_to_string(filters, ' and '));
        return query execute q;
    end if;
end
$$;


-- Retrieve revisions by occurrence criterion filtering
create or replace function swh_revision_get_by(
       origin_id bigint,
       branch_name text default NULL,
       validity timestamptz default NULL)
    returns setof revision_entry
    language sql
    stable
as $$
    select r.id, r.date, r.date_offset,
        r.committer_date, r.committer_date_offset,
        r.type, r.directory, r.message,
        a.name, a.email, c.name, c.email, r.metadata, r.synthetic,
        array(select rh.parent_id::bytea
            from revision_history rh
            where rh.id = r.id
            order by rh.parent_rank
        ) as parents
    from swh_occurrence_get_by(origin_id, branch_name, validity) as occ
    inner join revision r on occ.revision = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;

-- Retrieve a release by occurrence criterion
create or replace function swh_release_get_by(
       origin_id bigint)
    returns setof release_entry
    language sql
    stable
as $$
   select r.id, r.revision, r.date, r.date_offset,
        r.name, r.comment, r.synthetic, a.name as author_name,
        a.email as author_email
    from release r
    inner join occurrence_history occ on occ.revision = r.revision
    left join person a on a.id = r.author
    where occ.origin = origin_id;
$$;


-- Occurrence of some content in a given context
create type content_occurrence as (
    origin_type	 text,
    origin_url	 text,
    branch	 text,
    revision_id	 sha1_git,
    path	 unix_path
);


-- Given the sha1 of some content, look up an occurrence that points to a
-- revision, which in turns reference (transitively) a tree containing the
-- content. Answer the question: "where/when did SWH see a given content"?
-- Return information about an arbitrary occurrence/revision/tree if one is
-- found, NULL otherwise.
create or replace function swh_content_find_occurrence(content_id sha1)
    returns content_occurrence
    language plpgsql
as $$
declare
    dir content_dir;
    rev sha1_git;
    occ occurrence%ROWTYPE;
    coc content_occurrence;
begin
    -- each step could fail if no results are found, and that's OK
    select * from swh_content_find_directory(content_id)     -- look up directory
	into dir;
    if not found then return null; end if;

    select id from revision where directory = dir.directory  -- look up revision
	limit 1
	into rev;
    if not found then return null; end if;

    select * from swh_revision_find_occurrence(rev)	     -- look up occurrence
	into occ;
    if not found then return null; end if;

    select origin.type, origin.url, occ.branch, rev, dir.path
    from origin
    where origin.id = occ.origin
    into coc;

    return coc;  -- might be NULL
end
$$;

-- Create entries in entity_history from tmp_entity_history
--
-- TODO: do something smarter to compress the entries if the data
-- didn't change.
create or replace function swh_entity_history_add()
    returns void
    language plpgsql
as $$
begin
    insert into entity_history (
        uuid, parent, name, type, description, homepage, active, generated,
	lister, lister_metadata, doap, validity
    ) select * from tmp_entity_history;
    return;
end
$$;


create or replace function swh_update_entity_from_entity_history()
    returns trigger
    language plpgsql
as $$
begin
    with all_entities as (
      select uuid, parent, name, type, description, homepage, active,
             generated, lister, lister_metadata, doap, last_seen, last_id
      from (
          select row_number() over (partition by uuid order by unnest(validity) desc) as row,
	         id as last_id, uuid, parent, name, type, description, homepage, active,
		 generated, lister, lister_metadata, doap,
	         unnest(validity) as last_seen
          from entity_history
      ) as latest_entities
      where latest_entities.row = 1
    ),
    updated_uuids as (
      update entity set
        parent = all_entities.parent,
        name = all_entities.name,
	type = all_entities.type,
	description = all_entities.description,
	homepage = all_entities.homepage,
	active = all_entities.active,
	generated = all_entities.generated,
	lister = all_entities.lister,
	lister_metadata = all_entities.lister_metadata,
	doap = all_entities.doap,
	last_seen = all_entities.last_seen,
        last_id = all_entities.last_id
      from all_entities
      where entity.uuid = all_entities.uuid
      returning entity.uuid
    )
    insert into entity
    (select * from all_entities
     where uuid not in (select uuid from updated_uuids));
    return null;
end
$$;

create trigger update_entity
  after insert or update or delete or truncate
  on entity_history
  for each statement
  execute procedure swh_update_entity_from_entity_history();

-- map an id of tmp_entity_lister to a full entity
create type entity_id as (
    id               bigint,
    uuid             uuid,
    parent           uuid,
    name             text,
    type             entity_type,
    description      text,
    homepage         text,
    active           boolean,
    generated        boolean,
    lister           uuid,
    lister_metadata  jsonb,
    doap             jsonb,
    last_seen        timestamptz,
    last_id          bigint
);

-- find out the uuid of the entries of entity with the metadata
-- contained in tmp_entity_lister
create or replace function swh_entity_from_tmp_entity_lister()
    returns setof entity_id
    language plpgsql
as $$
begin
  return query
    select t.id, e.*
    from tmp_entity_lister t
    left join entity e
    on t.lister = e.lister AND e.lister_metadata @> t.lister_metadata;
  return;
end
$$;

create or replace function swh_entity_get(entity_uuid uuid)
    returns setof entity
    language sql
    stable
as $$
  with recursive entity_hierarchy as (
  select e.*
    from entity e where uuid = entity_uuid
    union
    select p.*
    from entity_hierarchy e
    join entity p on e.parent = p.uuid
  )
  select *
  from entity_hierarchy;
$$;

-- simple counter mapping a textual label to an integer value
create type counter as (
    label  text,
    value  bigint
);

-- return statistics about the number of tuples in various SWH tables
--
-- Note: the returned values are based on postgres internal statistics
-- (pg_class table), which are only updated daily (by autovacuum) or so
create or replace function swh_stat_counters()
    returns setof counter
    language sql
    stable
as $$
    select relname::text as label, reltuples::bigint as value
    from pg_class
    where oid in (
        'public.content'::regclass,
        'public.directory'::regclass,
        'public.directory_entry_dir'::regclass,
        'public.directory_entry_file'::regclass,
        'public.directory_entry_rev'::regclass,
        'public.occurrence'::regclass,
        'public.occurrence_history'::regclass,
        'public.origin'::regclass,
        'public.person'::regclass,
        'public.entity'::regclass,
        'public.entity_history'::regclass,
        'public.release'::regclass,
        'public.revision'::regclass,
        'public.revision_history'::regclass,
        'public.skipped_content'::regclass
    );
$$;
