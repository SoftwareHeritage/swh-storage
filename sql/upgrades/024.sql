-- SWH DB schema upgrade
-- from_version: 23
-- to_version: 24
-- description:
-- * new indexes
-- * improve rev_find_occurrence (with new function rev_list_children)
-- * mark relevant functions as STABLE
-- * new function: dir_walk (recursive ls)
-- * new function: stat_counters

insert into dbversion(version, release, description)
      values(24, now(), 'Work In Progress');

insert into organization(name, description, homepage)
       values('gnu',
              'GNU''s not Unix!',
              'https://gnu.org/');

create index on revision(directory);
create index on revision_history(parent_id);
create index on occurrence_history(revision);
create index on release(revision);
create index on content(ctime);

create or replace function swh_revision_list_children(root_revision sha1_git)
    returns setof sha1_git
    language sql
    stable
as $$
    with recursive rev_list(id) as (
	(select id from revision where id = root_revision)
	union
	(select h.id
	 from revision_history as h
	 join rev_list on h.parent_id = rev_list.id)
    )
    select * from rev_list;
$$;

create or replace function swh_revision_find_occurrence(revision_id sha1_git)
    returns occurrence
    language plpgsql
as $$
declare
    occ occurrence%ROWTYPE;
    rev sha1_git;
begin
    select origin, branch, revision
    from occurrence_history as occ_hist
    where occ_hist.revision = revision_id
    order by upper(occ_hist.validity)
    limit 1
    into occ;

    if not found then
	select origin, branch, revision
	from swh_revision_list_children(revision_id) as rev_list(sha1_git)
	left join occurrence_history occ_hist
	on rev_list.sha1_git = occ_hist.revision
	where occ_hist.origin is not null
	order by upper(occ_hist.validity)
	limit 1
	into occ;
    end if;

    return occ;
end
$$;

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
	    target, name, perms
     from ls_d
     left join directory_entry_dir d on ls_d.entry_id = d.id)
    union
    (select dir_id, 'file'::directory_entry_type as type,
	    target, name, perms
     from ls_f
     left join directory_entry_file d on ls_f.entry_id = d.id)
    union
    (select dir_id, 'rev'::directory_entry_type as type,
	    target, name, perms
     from ls_r
     left join directory_entry_rev d on ls_r.entry_id = d.id)
    order by name;
$$;

create or replace function swh_revision_list(root_revision sha1_git)
    returns setof sha1_git
    language sql
    stable
as $$
    with recursive rev_list(id) as (
	(select id from revision where id = root_revision)
	union
	(select parent_id
	 from revision_history as h
	 join rev_list on h.id = rev_list.id)
    )
    select * from rev_list;
$$;

alter type revision_log_entry
    alter attribute author_name type bytea,
    alter attribute author_email type bytea,
    alter attribute committer_name type bytea,
    alter attribute committer_email type bytea;

create or replace function swh_revision_log(root_revision sha1_git)
    returns setof revision_log_entry
    language sql
    stable
as $$
    select revision.id, date, date_offset,
	committer_date, committer_date_offset,
	type, directory, message,
	author.name as author_name, author.email as author_email,
	committer.name as committer_name, committer.email as committer_email
    from swh_revision_list(root_revision) as rev_list
    join revision on revision.id = rev_list
    join person as author on revision.author = author.id
    join person as committer on revision.committer = committer.id;
$$;

create or replace function swh_content_find_directory(content_id sha1)
    returns content_dir
    language sql
    stable
as $$
    with recursive path as (
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

create or replace function swh_directory_walk(walked_dir_id sha1_git)
    returns setof directory_entry
    language sql
    stable
as $$
    with recursive entries as (
        select dir_id, type, target, name, perms
        from swh_directory_walk_one(walked_dir_id)
        union all
        select dir_id, type, target, (dirname || '/' || name)::unix_path as name, perms
        from (select (swh_directory_walk_one(dirs.target)).*, dirs.name as dirname
              from (select target, name from entries where type = 'dir') as dirs) as with_parent
    )
    select dir_id, type, target, name, perms
    from entries
$$;

create type counter as (
    label  text,
    value  bigint
);

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
        'public.project'::regclass,
        'public.project_history'::regclass,
        'public.release'::regclass,
        'public.revision'::regclass,
        'public.revision_history'::regclass,
        'public.skipped_content'::regclass
    );
$$;
