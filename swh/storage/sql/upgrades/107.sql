-- SWH DB schema upgrade
-- from_version: 106
-- to_version: 107
-- description: Improve license endpoint's unknown license policy

insert into dbversion(version, release, description)
      values(107, now(), 'Work In Progress');

DROP FUNCTION swh_content_fossology_license_missing();

DROP FUNCTION swh_content_fossology_license_unknown();

DROP FUNCTION swh_mktemp_content_fossology_license_unknown();

CREATE OR REPLACE FUNCTION swh_content_fossology_license_add(conflict_update boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    -- insert unknown licenses first
    insert into fossology_license (name)
    select distinct license from tmp_content_fossology_license tmp
    where not exists (select 1 from fossology_license where name=tmp.license)
    on conflict(name) do nothing;

    if conflict_update then
        -- delete from content_fossology_license c
        --   using tmp_content_fossology_license tmp, indexer_configuration i
        --   where c.id = tmp.id and i.id=tmp.indexer_configuration_id
        delete from content_fossology_license
        where id in (select tmp.id
                     from tmp_content_fossology_license tmp
                     inner join indexer_configuration i on i.id=tmp.indexer_configuration_id);
    end if;

    insert into content_fossology_license (id, license_id, indexer_configuration_id)
    select tcl.id,
          (select id from fossology_license where name = tcl.license) as license,
          indexer_configuration_id
    from tmp_content_fossology_license tcl
        on conflict(id, license_id, indexer_configuration_id)
        do nothing;
    return;
end
$$;

COMMENT ON FUNCTION swh_content_fossology_license_add(conflict_update boolean) IS 'Add new content licenses';
