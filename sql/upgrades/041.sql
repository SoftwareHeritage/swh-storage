-- SWH DB schema upgrade
-- from_version: 40
-- to_version: 41
-- description: Open directory get by id

insert into dbversion(version, release, description)
      values(41, now(), 'Work In Progress');

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
