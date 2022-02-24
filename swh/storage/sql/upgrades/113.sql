-- SWH DB schema upgrade
-- from_version: 112
-- to_version: 113
-- description: Open indexer_configuration_add endpoint

insert into dbversion(version, release, description)
      values(113, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_indexer_configuration_add() RETURNS SETOF indexer_configuration
    LANGUAGE plpgsql
    AS $$
begin
      insert into indexer_configuration(tool_name, tool_version, tool_configuration)
      select tool_name, tool_version, tool_configuration from tmp_indexer_configuration tmp
      on conflict(tool_name, tool_version, tool_configuration) do nothing;

      return query
          select id, tool_name, tool_version, tool_configuration
          from tmp_indexer_configuration join indexer_configuration
              using(tool_name, tool_version, tool_configuration);

      return;
end
$$;

CREATE OR REPLACE FUNCTION swh_mktemp_indexer_configuration() RETURNS void
    LANGUAGE sql
    AS $$
    create temporary table tmp_indexer_configuration (
      like indexer_configuration including defaults
    ) on commit drop;
    alter table tmp_indexer_configuration drop column id;
$$;
