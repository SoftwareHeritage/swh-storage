-- SWH DB schema upgrade
-- from_version: 91
-- to_version: 92
-- description: Fix error in function to read license

insert into dbversion(version, release, description)
      values(92, now(), 'Work In Progress');

create or replace function swh_content_fossology_license_get()
    returns setof content_fossology_license_signature
    language plpgsql
as $$
begin
    return query
      select cl.id,
             ic.tool_name,
             ic.tool_version,
             array(select name
                   from fossology_license
                   where id = ANY(array_agg(cl.license_id))) as licenses
      from tmp_bytea tcl
      inner join content_fossology_license cl using(id)
      inner join indexer_configuration ic on ic.id=cl.indexer_configuration_id
      group by cl.id, ic.tool_name, ic.tool_version;
    return;
end
$$;

comment on function swh_content_fossology_license_get() IS 'List content licenses';
