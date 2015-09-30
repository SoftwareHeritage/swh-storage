-- SWH DB schema upgrade
-- from_version: 16
-- to_version: 17
-- description: improve swh_content_missing, generic shw_directory_entry_add

create or replace function swh_content_missing()
    returns setof content_signature
    language plpgsql
as $$
begin
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

create or replace function swh_directory_entry_add(typ directory_entry_type)
    returns void
    language plpgsql
as $$
begin
    execute format('
    insert into directory_entry_%1$s (target, name, perms, atime, mtime, ctime)
    select distinct t.target, t.name, t.perms, t.atime, t.mtime, t.ctime
    from tmp_directory_entry_%1$s t
    where not exists (
    select 1
    from directory_entry_%1$s i
    where t.target = i.target and t.name = i.name and t.perms = i.perms and
       t.atime is not distinct from i.atime and
       t.mtime is not distinct from i.mtime and
       t.ctime is not distinct from i.ctime)
   ', typ);

    execute format('
    with new_entries as (
	select t.dir_id, array_agg(i.id) as entries
	from tmp_directory_entry_%1$s t
	inner join directory_entry_%1$s i
	on t.target = i.target and t.name = i.name and t.perms = i.perms and
	   t.atime is not distinct from i.atime and
	   t.mtime is not distinct from i.mtime and
	   t.ctime is not distinct from i.ctime
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

drop function swh_directory_entry_dir_add ();
drop function swh_directory_entry_file_add ();
drop function swh_directory_entry_rev_add ();
