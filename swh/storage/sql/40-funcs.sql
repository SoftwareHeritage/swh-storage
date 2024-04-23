create or replace function hash_sha1(text)
       returns text
as $$
   select encode(digest($1, 'sha1'), 'hex')
$$ language sql strict immutable;

comment on function hash_sha1(text) is 'Compute SHA1 hash as text';

-- create a temporary table called tmp_TBLNAME, mimicking existing table
-- TBLNAME
--
-- Args:
--     tblname: name of the table to mimic
create or replace function swh_mktemp(tblname regclass)
    returns void
    language plpgsql
as $$
begin
    execute format('
	create temporary table if not exists tmp_%1$I
	    (like %1$I including defaults)
	    on commit delete rows;
      alter table tmp_%1$I drop column if exists object_id;
	', tblname);
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
--     tblname: name of the table to mimic
create or replace function swh_mktemp_dir_entry(tblname regclass)
    returns void
    language plpgsql
as $$
begin
    execute format('
	create temporary table if not exists tmp_%1$I
	    (like %1$I including defaults, dir_id sha1_git)
	    on commit delete rows;
        alter table tmp_%1$I drop column if exists id;
	', tblname);
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
    create temporary table if not exists tmp_revision (
        like revision including defaults,
        author_fullname bytea,
        author_name bytea,
        author_email bytea,
        committer_fullname bytea,
        committer_name bytea,
        committer_email bytea
    ) on commit delete rows;
    alter table tmp_revision drop column if exists author;
    alter table tmp_revision drop column if exists committer;
    alter table tmp_revision drop column if exists object_id;
$$;

-- create a temporary table for releases called tmp_release,
-- mimicking existing table release, replacing the foreign keys to
-- people with an email and name field
--
create or replace function swh_mktemp_release()
    returns void
    language sql
as $$
    create temporary table if not exists tmp_release (
        like release including defaults,
        author_fullname bytea,
        author_name bytea,
        author_email bytea
    ) on commit delete rows;
    alter table tmp_release drop column if exists author;
    alter table tmp_release drop column if exists object_id;
$$;

-- create a temporary table for the branches of a snapshot
create or replace function swh_mktemp_snapshot_branch()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_snapshot_branch (
      name bytea not null,
      target bytea,
      target_type snapshot_target
  ) on commit delete rows;
$$;

-- a content signature is a set of cryptographic checksums that we use to
-- uniquely identify content, for the purpose of verifying if we already have
-- some content or not during content injection
create type content_signature as (
    sha1       sha1,
    sha1_git   sha1_git,
    sha256     sha256,
    blake2s256 blake2s256
);


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
	select sha1, sha1_git, sha256, blake2s256 from tmp_skipped_content t
	where not exists
	(select 1 from skipped_content s where
	    s.sha1 is not distinct from t.sha1 and
	    s.sha1_git is not distinct from t.sha1_git and
	    s.sha256 is not distinct from t.sha256);
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
begin
    insert into content (sha1, sha1_git, sha256, blake2s256, length, status, ctime)
        select distinct sha1, sha1_git, sha256, blake2s256, length, status, ctime from tmp_content;
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
    insert into skipped_content (sha1, sha1_git, sha256, blake2s256, length, status, reason, origin)
        select distinct sha1, sha1_git, sha256, blake2s256, length, status, reason, origin
	from tmp_skipped_content
	where (coalesce(sha1, ''), coalesce(sha1_git, ''), coalesce(sha256, '')) in (
            select coalesce(sha1, ''), coalesce(sha1_git, ''), coalesce(sha256, '')
            from swh_skipped_content_missing()
        );
        -- TODO XXX use postgres 9.5 "UPSERT" support here, when available.
        -- Specifically, using "INSERT .. ON CONFLICT IGNORE" we can avoid
        -- the extra swh_skipped_content_missing() query here.
    return;
end
$$;

-- Update content entries from temporary table.
-- (columns are potential new columns added to the schema, this cannot be empty)
--
create or replace function swh_content_update(columns_update text[])
    returns void
    language plpgsql
as $$
declare
   query text;
   tmp_array text[];
begin
    if array_length(columns_update, 1) = 0 then
        raise exception 'Please, provide the list of column names to update.';
    end if;

    tmp_array := array(select format('%1$s=t.%1$s', unnest) from unnest(columns_update));

    query = format('update content set %s
                    from tmp_content t where t.sha1 = content.sha1',
                    array_to_string(tmp_array, ', '));

    execute query;

    return;
end
$$;

comment on function swh_content_update(text[]) IS 'Update existing content''s columns';


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
      where t.target = i.target and t.name = i.name and t.perms = i.perms
	)
	on conflict
	do nothing
	;
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

    insert into directory (id, dir_entries, file_entries, rev_entries, raw_manifest)
    select id, dir_entries, file_entries, rev_entries, raw_manifest from tmp_directory t
    where not exists (
        select 1 from directory d
        where d.id = t.id
	);

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
  sha256   sha256,          -- content's sha256 if type is not dir
  length   bigint           -- content length if type is not dir
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
            NULL::sha1, NULL::sha1_git, NULL::sha256, NULL::bigint
     from ls_d
     left join directory_entry_dir e on ls_d.entry_id = e.id)
    union
    (with known_contents as
	(select dir_id, 'file'::directory_entry_type as type,
            e.target, e.name, e.perms, c.status,
            c.sha1, c.sha1_git, c.sha256, c.length
         from ls_f
         left join directory_entry_file e on ls_f.entry_id = e.id
         inner join content c on e.target = c.sha1_git)
        select * from known_contents
	union
	(select dir_id, 'file'::directory_entry_type as type,
            e.target, e.name, e.perms, c.status,
            c.sha1, c.sha1_git, c.sha256, c.length
         from ls_f
         left join directory_entry_file e on ls_f.entry_id = e.id
         left join skipped_content c on e.target = c.sha1_git
         where not exists (select 1 from known_contents where known_contents.sha1_git=e.target)))
    union
    (select dir_id, 'rev'::directory_entry_type as type,
            e.target, e.name, e.perms, NULL::content_status,
            NULL::sha1, NULL::sha1_git, NULL::sha256, NULL::bigint
     from ls_r
     left join directory_entry_rev e on ls_r.entry_id = e.id)
    order by name;
$$;

-- List recursively the revision directory arborescence
create or replace function swh_directory_walk(walked_dir_id sha1_git)
    returns setof directory_entry
    language sql
    stable
as $$
    with recursive entries as (
        select dir_id, type, target, name, perms, status, sha1, sha1_git,
               sha256, length
        from swh_directory_walk_one(walked_dir_id)
        union all
        select dir_id, type, target, (dirname || '/' || name)::unix_path as name,
               perms, status, sha1, sha1_git, sha256, length
        from (select (swh_directory_walk_one(dirs.target)).*, dirs.name as dirname
              from (select target, name from entries where type = 'dir') as dirs) as with_parent
    )
    select dir_id, type, target, name, perms, status, sha1, sha1_git, sha256, length
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

-- Returns the entries in a directory, without joining with their target tables
create or replace function swh_directory_get_entries(dir_id sha1_git)
    returns table (
        dir_id directory_entry_type, target sha1_git, name unix_path, perms file_perms
    )
    language sql
    stable
as $$
    with dir as (
	select id as dir_id, dir_entries, file_entries, rev_entries
	from directory
	where id = dir_id),
    ls_d as (select dir_id, unnest(dir_entries) as entry_id from dir),
    ls_f as (select dir_id, unnest(file_entries) as entry_id from dir),
    ls_r as (select dir_id, unnest(rev_entries) as entry_id from dir)
    (select 'dir'::directory_entry_type, e.target, e.name, e.perms
     from ls_d
     left join directory_entry_dir e on ls_d.entry_id = e.id)
    union
	(select 'file'::directory_entry_type, e.target, e.name, e.perms
     from ls_f
     left join directory_entry_file e on ls_f.entry_id = e.id)
    union
    (select 'rev'::directory_entry_type, e.target, e.name, e.perms
     from ls_r
     left join directory_entry_rev e on ls_r.entry_id = e.id)
$$;

-- List all revision IDs starting from a given revision, going back in time
--
-- TODO ordering: should be breadth-first right now (what do we want?)
-- TODO ordering: ORDER BY parent_rank somewhere?
create or replace function swh_revision_list(root_revisions bytea[], num_revs bigint default NULL)
    returns table (id sha1_git, parents bytea[])
    language sql
    stable
as $$
    with recursive full_rev_list(id) as (
        (select id from revision where id = ANY(root_revisions))
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


-- Detailed entry for a revision
create type revision_entry as
(
  id                             sha1_git,
  date                           timestamptz,
  date_offset                    smallint,
  date_neg_utc_offset            boolean,
  date_offset_bytes              bytea,
  committer_date                 timestamptz,
  committer_date_offset          smallint,
  committer_date_neg_utc_offset  boolean,
  committer_date_offset_bytes    bytea,
  type                           revision_type,
  directory                      sha1_git,
  message                        bytea,
  author_id                      bigint,
  author_fullname                bytea,
  author_name                    bytea,
  author_email                   bytea,
  committer_id                   bigint,
  committer_fullname             bytea,
  committer_name                 bytea,
  committer_email                bytea,
  metadata                       jsonb,
  synthetic                      boolean,
  parents                        bytea[],
  object_id                      bigint,
  extra_headers                  bytea[][],
  raw_manifest                   bytea
);


create or replace function swh_revision_log(root_revisions bytea[], num_revs bigint default NULL, ignore_displayname boolean default false)
    returns setof revision_entry
    language sql
    stable
as $$
    -- when name and email are null, swh.storage.postgresql.converters.db_to_author()
    -- parses fullname to populate them, so we can just drop them here
    select t.id, r.date, r.date_offset, r.date_neg_utc_offset, r.date_offset_bytes,
           r.committer_date, r.committer_date_offset, r.committer_date_neg_utc_offset, r.committer_date_offset_bytes,
           r.type, r.directory, r.message,
           a.id,
           case when ignore_displayname or a.displayname is null then a.fullname else a.displayname end,
           case when ignore_displayname or a.displayname is null then a.name else null end,
           case when ignore_displayname or a.displayname is null then a.email else null end,
           c.id,
           case when ignore_displayname or c.displayname is null then c.fullname else c.displayname end,
           case when ignore_displayname or c.displayname is null then c.name else null end,
           case when ignore_displayname or c.displayname is null then c.email else null end,
           r.metadata, r.synthetic, t.parents, r.object_id, r.extra_headers,
           r.raw_manifest
    from swh_revision_list(root_revisions, num_revs) as t
    left join revision r on t.id = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;

comment on function swh_revision_log(root_revisions bytea[], num_revs bigint, ignore_displayname boolean)
  is '"git style" revision log. Similar to swh_revision_list(), but returning '
     'all information associated to each revision, and expanding authors/committers';

-- Detailed entry for a release
create type release_entry as
(
  id                   sha1_git,
  target               sha1_git,
  target_type          object_type,
  date                 timestamptz,
  date_offset          smallint,
  date_neg_utc_offset  boolean,
  name                 bytea,
  comment              bytea,
  synthetic            boolean,
  author_id            bigint,
  author_fullname      bytea,
  author_name          bytea,
  author_email         bytea,
  object_id            bigint
);

-- Create entries in person from tmp_revision
create or replace function swh_person_add_from_revision()
    returns void
    language plpgsql
as $$
begin
    with t as (
        select author_fullname as fullname, author_name as name, author_email as email from tmp_revision
    union
        select committer_fullname as fullname, committer_name as name, committer_email as email from tmp_revision
    ) insert into person (fullname, name, email)
    select distinct on (fullname) fullname, name, email from t
    where t.fullname is not null and not exists (
        select 1
        from person p
        where t.fullname = p.fullname
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

    insert into revision (id,   date,   date_offset,   date_neg_utc_offset,   date_offset_bytes,   committer_date,   committer_date_offset,   committer_date_neg_utc_offset,   committer_date_offset_bytes,   type,   directory,   message, author, committer,   metadata,   synthetic,   extra_headers,   raw_manifest)
    select              t.id, t.date, t.date_offset, t.date_neg_utc_offset, t.date_offset_bytes, t.committer_date, t.committer_date_offset, t.committer_date_neg_utc_offset, t.committer_date_offset_bytes, t.type, t.directory, t.message,   a.id,      c.id, t.metadata, t.synthetic, t.extra_headers, t.raw_manifest
    from tmp_revision t
    left join person a on a.fullname = t.author_fullname
    left join person c on c.fullname = t.committer_fullname;
    return;
end
$$;


-- Create entries in extid from tmp_extid
-- operates in bulk: 0. swh_mktemp(extid), 1. COPY to tmp_extid,
-- 2. call this function
create or replace function swh_extid_add()
    returns void
    language plpgsql
as $$
begin
    insert into extid (extid_type, extid, extid_version, target_type, target)
        select distinct t.extid_type, t.extid, t.extid_version, t.target_type, t.target
        from tmp_extid t
		on conflict do nothing;
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
        select distinct author_fullname as fullname, author_name as name, author_email as email from tmp_release
        where author_fullname is not null
    ) insert into person (fullname, name, email)
    select distinct on (fullname) fullname, name, email from t
    where not exists (
        select 1
        from person p
        where t.fullname = p.fullname
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

    insert into release (id,   target,   target_type,   date,   date_offset,   date_neg_utc_offset,   date_offset_bytes,   name,   comment, author,   synthetic,    raw_manifest)
      select distinct  t.id, t.target, t.target_type, t.date, t.date_offset, t.date_neg_utc_offset, t.date_offset_bytes, t.name, t.comment,   a.id, t.synthetic, t.raw_manifest
        from tmp_release t
        left join person a on a.fullname = t.author_fullname
        where not exists (select 1 from release where t.id = release.id);
    return;
end
$$;


-- add a new origin_visit for origin origin_id at date.
--
-- Returns the new visit id.
create or replace function swh_origin_visit_add(origin_url text, date timestamptz, type text)
    returns bigint
    language sql
as $$
  with origin_id as (
    select id
    from origin
    where url = origin_url
  ), last_known_visit as (
    select coalesce(
      (
        select visit
        from origin_visit
        where origin = (select id from origin_id)
        order by visit desc
        limit 1
        for update
      ),
    0) as visit
  )
  insert into origin_visit (origin, date, type, visit)
  values ((select id from origin_id), date, type,
          (select visit from last_known_visit) + 1)
  returning visit;
$$;

create or replace function swh_snapshot_add(snapshot_id sha1_git)
  returns void
  language plpgsql
as $$
declare
  snapshot_object_id snapshot.object_id%type;
begin
  select object_id from snapshot where id = snapshot_id into snapshot_object_id;
  if snapshot_object_id is null then
     insert into snapshot (id) values (snapshot_id) returning object_id into snapshot_object_id;
     insert into snapshot_branch (name, target_type, target)
       select name, target_type, target from tmp_snapshot_branch tmp
       where not exists (
         select 1
         from snapshot_branch sb
         where sb.name = tmp.name
           and sb.target = tmp.target
           and sb.target_type = tmp.target_type
       )
       on conflict do nothing;
     insert into snapshot_branches (snapshot_id, branch_id)
     select snapshot_object_id, sb.object_id as branch_id
       from tmp_snapshot_branch tmp
       join snapshot_branch sb
       using (name, target, target_type)
       where tmp.target is not null and tmp.target_type is not null
     union
     select snapshot_object_id, sb.object_id as branch_id
       from tmp_snapshot_branch tmp
       join snapshot_branch sb
       using (name)
       where tmp.target is null and tmp.target_type is null
         and sb.target is null and sb.target_type is null;
  end if;
  truncate table tmp_snapshot_branch;
end;
$$;

create type snapshot_result as (
  snapshot_id  sha1_git,
  name         bytea,
  target       bytea,
  target_type  snapshot_target
);

create or replace function swh_snapshot_get_by_id(id sha1_git,
    branches_from bytea default '', branches_count bigint default null,
    target_types snapshot_target[] default NULL,
    branch_name_include_substring bytea default NULL,
    branch_name_exclude_prefix bytea default NULL)
  returns setof snapshot_result
  language sql
  stable
as $$
  -- with small limits, the "naive" version of this query can degenerate into
  -- using the deduplication index on snapshot_branch (name, target,
  -- target_type); The planner happily scans several hundred million rows.

  -- Do the query in two steps: first pull the relevant branches for the given
  -- snapshot (filtering them by type), then do the limiting. This two-step
  -- process guides the planner into using the proper index.
  with filtered_snapshot_branches as (
    select swh_snapshot_get_by_id.id as snapshot_id, name, target, target_type
      from snapshot_branches
      inner join snapshot_branch on snapshot_branches.branch_id = snapshot_branch.object_id
      where snapshot_id = (select object_id from snapshot where snapshot.id = swh_snapshot_get_by_id.id)
        and (target_types is null or target_type = any(target_types))
      order by name
  )
  select snapshot_id, name, target, target_type
    from filtered_snapshot_branches
    where name >= branches_from
        and (branch_name_include_substring is null or name like '%'||branch_name_include_substring||'%')
        and (branch_name_exclude_prefix is null or name not like branch_name_exclude_prefix||'%')
    order by name limit branches_count;
$$;

create type snapshot_size as (
  target_type snapshot_target,
  count bigint
);

create or replace function swh_snapshot_count_branches(id sha1_git,
    branch_name_exclude_prefix bytea default NULL)
  returns setof snapshot_size
  language sql
  stable
as $$
  SELECT target_type, count(name)
  from swh_snapshot_get_by_id(swh_snapshot_count_branches.id,
    branch_name_exclude_prefix => swh_snapshot_count_branches.branch_name_exclude_prefix)
  group by target_type;
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

-- Find the visit of origin closest to date visit_date
-- Breaks ties by selecting the largest visit id
create or replace function swh_visit_find_by_date(origin_url text, visit_date timestamptz default NOW(), visit_type text default null)
    returns setof origin_visit
    language plpgsql
    stable
as $$
declare
  origin_id bigint;
begin
  select id into origin_id from origin where url=origin_url;
  return query
  -- first find the two closest dates (this does a one-row scan of the origin_visit
  -- (origin, date) index twice, forward and backward around the given visit_date)
  with closest_two_visit_dates as ((
    select date, (date - visit_date) as interval -- date >= visit_date so interval >= 0
    from origin_visit ov
    where ov.origin = origin_id
          and ov.date >= visit_date
          and (visit_type is null or ov.type = visit_type)
    order by ov.date asc
    limit 1
  ) union (
    select date, (visit_date - date) as interval -- date < visit_date so interval > 0
    from origin_visit ov
    where ov.origin = origin_id
          and ov.date < visit_date
          and (visit_type is null or ov.type = visit_type)
    order by ov.date desc
    limit 1
  ))
  -- then select the data of the visit at the closest date, with the highest visit id
  -- (this uses the origin_visit (origin, date) index a third time to find the highest
  -- visit id at the given date)
  select * from origin_visit
    where origin = origin_id
      and date = (select date from closest_two_visit_dates order by interval limit 1)
      and (visit_type is null or type = visit_type)
    order by visit desc
    limit 1;
end
$$;

-- Object listing by object_id

create or replace function swh_content_list_by_object_id(
    min_excl bigint,
    max_incl bigint
)
    returns setof content
    language sql
    stable
as $$
    select * from content
    where object_id > min_excl and object_id <= max_incl
    order by object_id;
$$;

create or replace function swh_revision_list_by_object_id(
    min_excl bigint,
    max_incl bigint
)
    returns setof revision_entry
    language sql
    stable
as $$
    with revs as (
        select * from revision
        where object_id > min_excl and object_id <= max_incl
    )
    select r.id, r.date, r.date_offset, r.date_neg_utc_offset, r.date_offset_bytes,
           r.committer_date, r.committer_date_offset, r.committer_date_neg_utc_offset, r.committer_date_offset_bytes,
           r.type, r.directory, r.message,
           a.id, a.fullname, a.name, a.email, c.id, c.fullname, c.name, c.email, r.metadata, r.synthetic,
           array(select rh.parent_id::bytea from revision_history rh where rh.id = r.id order by rh.parent_rank)
               as parents, r.object_id, r.extra_headers, r.raw_manifest
    from revs r
    left join person a on a.id = r.author
    left join person c on c.id = r.committer
    order by r.object_id;
$$;

create or replace function swh_release_list_by_object_id(
    min_excl bigint,
    max_incl bigint
)
    returns setof release_entry
    language sql
    stable
as $$
    with rels as (
        select * from release
        where object_id > min_excl and object_id <= max_incl
    )
    select r.id, r.target, r.target_type, r.date, r.date_offset, r.date_neg_utc_offset, r.name, r.comment,
           r.synthetic, p.id as author_id, p.fullname as author_fullname, p.name as author_name, p.email as author_email, r.object_id
    from rels r
    left join person p on p.id = r.author
    order by r.object_id;
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
    select object_type as label, value as value
    from object_counts
    where object_type in (
        'content',
        'directory',
        'directory_entry_dir',
        'directory_entry_file',
        'directory_entry_rev',
        'origin',
        'origin_visit',
        'person',
        'release',
        'revision',
        'revision_history',
        'skipped_content',
        'snapshot'
    );
$$;

create or replace function swh_update_counter(object_type text)
    returns void
    language plpgsql
as $$
begin
    execute format('
	insert into object_counts
    (value, last_update, object_type)
  values
    ((select count(*) from %1$I), NOW(), %1$L)
  on conflict (object_type) do update set
    value = excluded.value,
    last_update = excluded.last_update',
  object_type);
    return;
end;
$$;

create or replace function swh_update_counter_bucketed()
    returns void
    language plpgsql
as $$
declare
  query text;
  line_to_update int;
  new_value bigint;
begin
  select
    object_counts_bucketed.line,
    format(
      'select count(%I) from %I where %s',
      coalesce(identifier, '*'),
      object_type,
      coalesce(
        concat_ws(
          ' and ',
          case when bucket_start is not null then
            format('%I >= %L', identifier, bucket_start) -- lower bound condition, inclusive
          end,
          case when bucket_end is not null then
            format('%I < %L', identifier, bucket_end) -- upper bound condition, exclusive
          end
        ),
        'true'
      )
    )
    from object_counts_bucketed
    order by coalesce(last_update, now() - '1 month'::interval) asc
    limit 1
    into line_to_update, query;

  execute query into new_value;

  update object_counts_bucketed
    set value = new_value,
        last_update = now()
    where object_counts_bucketed.line = line_to_update;

END
$$;

create or replace function swh_update_counters_from_buckets()
  returns trigger
  language plpgsql
as $$
begin
with to_update as (
  select object_type, sum(value) as value, max(last_update) as last_update
  from object_counts_bucketed ob1
  where not exists (
    select 1 from object_counts_bucketed ob2
    where ob1.object_type = ob2.object_type
    and value is null
    )
  group by object_type
) update object_counts
  set
    value = to_update.value,
    last_update = to_update.last_update
  from to_update
  where
    object_counts.object_type = to_update.object_type
    and object_counts.value != to_update.value;
return null;
end
$$;

create trigger update_counts_from_bucketed
  after insert or update
  on object_counts_bucketed
  for each row
  when (NEW.line % 256 = 0)
  execute procedure swh_update_counters_from_buckets();
