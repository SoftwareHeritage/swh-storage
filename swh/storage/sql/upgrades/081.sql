-- SWH DB schema upgrade
-- from_version: 80
-- to_version: 81
-- description:

insert into dbversion(version, release, description)
      values(81, now(), 'Work In Progress');

drop function swh_cache_content_get_by_batch(bytea, bigint);

create or replace function swh_cache_content_get()
       returns setof content_signature
       language sql
       stable
as $$
    SELECT DISTINCT c.sha1, c.sha1_git, c.sha256
    FROM cache_content_revision ccr
    INNER JOIN content as c
    ON ccr.content = c.sha1_git
$$;

COMMENT ON FUNCTION swh_cache_content_get() IS 'Retrieve all distinct cache contents';
