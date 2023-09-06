-- SWH DB schema upgrade
-- from_version: 189
-- to_version: 190
-- description: New version of the swh_directory_entry_add function, reducing
-- risks of unicity constraints being triggered on directory_entry_xxx tables.

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
