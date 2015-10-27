-- SWH DB schema upgrade
-- from_version: 27
-- to_version: 28
-- description: bulk insertion of directories from a temporary table

insert into dbversion(version, release, description)
      values(28, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_directory_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
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

CREATE OR REPLACE FUNCTION swh_directory_entry_add(typ directory_entry_type) RETURNS void
    LANGUAGE plpgsql
    AS $_$
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
$_$;
