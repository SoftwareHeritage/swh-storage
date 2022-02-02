-- SWH DB schema upgrade
-- from_version: 174
-- to_version: 175
-- description: add directory_get_entries function

insert into dbversion(version, release, description)
  values(175, now(), 'Work In Progress');

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
