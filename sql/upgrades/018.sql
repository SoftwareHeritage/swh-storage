-- SWH DB schema upgrade
-- from_version: 17
-- to_version: 18
-- description: remove atime, ctime, mtime from directory_entry_*

insert into dbversion(version, release, description)
      values(18, now(), 'Work In Progress');

alter table directory_entry_dir drop column atime;
alter table directory_entry_dir drop column mtime;
alter table directory_entry_dir drop column ctime;
create unique index on directory_entry_dir(target, name, perms);

alter table directory_entry_file drop column atime;
alter table directory_entry_file drop column mtime;
alter table directory_entry_file drop column ctime;
create unique index on directory_entry_file(target, name, perms);

alter table directory_entry_rev drop column atime;
alter table directory_entry_rev drop column mtime;
alter table directory_entry_rev drop column ctime;
create unique index on directory_entry_rev(target, name, perms);

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
    update directory as d
    set %1$s_entries = new_entries.entries
    from new_entries
    where d.id = new_entries.dir_id
    ', typ);

    return;
end
$$;

alter type directory_entry drop attribute atime;
alter type directory_entry drop attribute mtime;
alter type directory_entry drop attribute ctime;

create or replace function swh_directory_walk_one(walked_dir_id sha1_git)
    returns setof directory_entry
    language sql
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
