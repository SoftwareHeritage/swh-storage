-- SWH DB schema upgrade
-- from_version: 161
-- to_version: 162
-- description: Make swh_directory_walk_one join skipped_content in addition to content

-- latest schema version
insert into dbversion(version, release, description)
      values(162, now(), 'Work Still In Progress');

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

