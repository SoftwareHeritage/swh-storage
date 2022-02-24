-- SWH DB schema upgrade
-- from_version: 14
-- to_version: 15
-- description: merge directory_list_* tables into directory


insert into dbversion(version, release, description)
      values(15, now(), 'Work In Progress');

alter table directory
    add column dir_entries  bigint[],
    add column file_entries bigint[],
    add column rev_entries  bigint[];

with ls as (
    -- we need an explicit sub-query here, because left joins aren't allowed in
    -- update from_list
    select id,
           ls_d.entry_ids as dir_entries,
	   ls_f.entry_ids as file_entries,
	   ls_r.entry_ids as rev_entries
    from directory as d
    left join directory_list_dir  as ls_d on ls_d.dir_id = d.id
    left join directory_list_file as ls_f on ls_f.dir_id = d.id
    left join directory_list_rev  as ls_r on ls_r.dir_id = d.id
)
update directory
    set dir_entries =  ls.dir_entries,
        file_entries = ls.file_entries,
	rev_entries =  ls.rev_entries
    from ls
    where ls.id = directory.id;

create index on directory using gin (dir_entries);
create index on directory using gin (file_entries);
create index on directory using gin (rev_entries);

drop table directory_list_dir;
drop table directory_list_file;
drop table directory_list_rev;

drop function swh_directory_missing();

create or replace function swh_directory_missing()
    returns setof sha1_git
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

    with new_entries as (
	select t.dir_id, array_agg(i.id) as entries
	from tmp_directory_entry_dir t
	inner join directory_entry_dir i
	on t.target = i.target and t.name = i.name and t.perms = i.perms and
	   t.atime is not distinct from i.atime and
	   t.mtime is not distinct from i.mtime and
	   t.ctime is not distinct from i.ctime
	group by t.dir_id
    )
    update directory as d
    set dir_entries = new_entries.entries
    from new_entries
    where d.id = new_entries.dir_id;

    return;
end
$$;

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

    with new_entries as (
	select t.dir_id, array_agg(i.id) as entries
	from tmp_directory_entry_file t
	inner join directory_entry_file i
	on t.target = i.target and t.name = i.name and t.perms = i.perms and
	   t.atime is not distinct from i.atime and
	   t.mtime is not distinct from i.mtime and
	   t.ctime is not distinct from i.ctime
	group by t.dir_id
    )
    update directory as d
    set file_entries = new_entries.entries
    from new_entries
    where d.id = new_entries.dir_id;

    return;
end
$$;

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

    with new_entries as (
	select t.dir_id, array_agg(i.id) as entries
	from tmp_directory_entry_rev t
	inner join directory_entry_rev i
	on t.target = i.target and t.name = i.name and t.perms = i.perms and
	   t.atime is not distinct from i.atime and
	   t.mtime is not distinct from i.mtime and
	   t.ctime is not distinct from i.ctime
	group by t.dir_id
    )
    update directory as d
    set rev_entries = new_entries.entries
    from new_entries
    where d.id = new_entries.dir_id;

    return;
end
$$;

create or replace function swh_directory_walk_one(walked_dir_id sha1_git)
    returns setof directory_entry
    language plpgsql
as $$
begin
    return query
        with dir as (
	    select id as dir_id, dir_entries, file_entries, rev_entries
	    from directory
	    where id = walked_dir_id),
	ls_d as (select dir_id, unnest(dir_entries) as entry_id from dir),
	ls_f as (select dir_id, unnest(file_entries) as entry_id from dir),
	ls_r as (select dir_id, unnest(rev_entries) as entry_id from dir)
	(select dir_id, 'dir'::directory_entry_type as type,
	        target, name, perms, atime, mtime, ctime
	 from ls_d
	 left join directory_entry_dir d on ls_d.entry_id = d.id)
        union
        (select dir_id, 'file'::directory_entry_type as type,
	        target, name, perms, atime, mtime, ctime
	 from ls_f
	 left join directory_entry_file d on ls_f.entry_id = d.id)
        union
        (select dir_id, 'rev'::directory_entry_type as type,
	        target, name, perms, atime, mtime, ctime
	 from ls_r
	 left join directory_entry_rev d on ls_r.entry_id = d.id)
        order by name;
    return;
end
$$;

create or replace function swh_content_find_directory(content_id sha1)
    returns content_dir
    language plpgsql
as $$
declare
    d content_dir;
begin
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
    select dir_id, name from path order by depth desc limit 1
    into strict d;

    return d;
end
$$;
