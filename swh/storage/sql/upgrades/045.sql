-- SWH DB schema upgrade
-- from_version: 44
-- to_version: 45
-- description: Fixup swh_directory_get for the new directory schema

insert into dbversion(version, release, description)
      values(45, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_directory_get() RETURNS SETOF directory
    LANGUAGE plpgsql
    AS $$
begin
    return query
	select d.*
        from tmp_directory t
        inner join directory d on t.id = d.id;
    return;
end
$$;
