-- SWH DB schema upgrade
-- from_version: 84
-- to_version: 85
-- description: content cache: insert lines in deterministic order to reduce lock contention

insert into dbversion(version, release, description)
      values(85, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_cache_content_revision_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
declare
  cnt bigint;
  d sha1_git;
begin
  delete from tmp_bytea t where exists (select 1 from cache_content_revision_processed ccrp where t.id = ccrp.revision);

  select count(*) from tmp_bytea into cnt;
  if cnt <> 0 then
    create temporary table tmp_ccr (
        content sha1_git,
        directory sha1_git,
        path unix_path
    ) on commit drop;

    create temporary table tmp_ccrd (
        directory sha1_git,
        revision sha1_git
    ) on commit drop;

    insert into tmp_ccrd
      select directory, id as revision
      from tmp_bytea
      inner join revision using(id);

    insert into cache_content_revision_processed
      select distinct id from tmp_bytea order by id;

    for d in
      select distinct directory from tmp_ccrd
    loop
      insert into tmp_ccr
        select sha1_git as content, d as directory, name as path
        from swh_directory_walk(d)
        where type='file';
    end loop;

    with revision_contents as (
      select content, false as blacklisted, array_agg(ARRAY[revision::bytea, path::bytea]) as revision_paths
      from tmp_ccr
      inner join tmp_ccrd using (directory)
      group by content
      order by content
    ), updated_cache_entries as (
      update cache_content_revision ccr
      set revision_paths = ccr.revision_paths || rc.revision_paths
      from revision_contents rc
      where ccr.content = rc.content and ccr.blacklisted = false
      returning ccr.content
    ) insert into cache_content_revision
        select * from revision_contents rc
        where not exists (select 1 from updated_cache_entries uce where uce.content = rc.content)
        order by rc.content
      on conflict (content) do update
        set revision_paths = cache_content_revision.revision_paths || EXCLUDED.revision_paths
        where cache_content_revision.blacklisted = false;
    return;
  else
    return;
  end if;
end
$$;

COMMENT ON FUNCTION swh_cache_content_revision_add() IS 'Cache the revisions from tmp_bytea into cache_content_revision';
