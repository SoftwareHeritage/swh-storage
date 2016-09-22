-- SWH DB schema upgrade
-- from_version: 82
-- to_version: 83
-- description: make the cache_content_revision table have less churn

insert into dbversion(version, release, description)
      values(83, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_cache_content_revision_add(revision_id sha1_git) RETURNS void
    LANGUAGE plpgsql
    AS $$
declare
  rev sha1_git;
begin
    select revision
        from cache_content_revision_processed
        where revision=revision_id
        into rev;

    if rev is NULL then

      insert into cache_content_revision_processed (revision) VALUES (revision_id);

      with revision_contents as (
            select sha1_git as content, false as blacklisted, array_agg(ARRAY[revision_id::bytea, name::bytea]) as revision_paths
            from swh_directory_walk((select directory from revision where id=revision_id))
            where type='file'
            group by sha1_git
      ), updated_cache_entries as (
            update cache_content_revision ccr
            set revision_paths = ccr.revision_paths || rc.revision_paths
            from revision_contents rc
            where ccr.content = rc.content and ccr.blacklisted = false
            returning ccr.content
      ) insert into cache_content_revision
          select * from revision_contents rc
          where not exists (select 1 from updated_cache_entries uce where uce.content = rc.content)
          on conflict (content) do update
            set revision_paths = cache_content_revision.revision_paths || EXCLUDED.revision_paths
            where cache_content_revision.blacklisted = false;
      return;

    else
      return;
    end if;
end
$$;

COMMENT ON FUNCTION swh_cache_content_revision_add(revision_id sha1_git) IS 'Cache the specified revision directory contents into cache_content_revision';
