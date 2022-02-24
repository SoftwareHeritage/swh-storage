-- SWH DB schema upgrade
-- from_version: 85
-- to_version: 86
-- description: content cache: insert lines in deterministic order to reduce lock contention

insert into dbversion(version, release, description)
      values(86, now(), 'Work In Progress');

drop function swh_cache_content_get();

create type cache_content_signature as (
  sha1      sha1,
  sha1_git  sha1_git,
  sha256    sha256,
  revision_paths  bytea[][]
);

create or replace function swh_cache_content_get_all()
       returns setof cache_content_signature
       language sql
       stable
as $$
    SELECT c.sha1, c.sha1_git, c.sha256, ccr.revision_paths
    FROM cache_content_revision ccr
    INNER JOIN content as c
    ON ccr.content = c.sha1_git
$$;

COMMENT ON FUNCTION swh_cache_content_get_all() IS 'Retrieve batch of contents';


create or replace function swh_cache_content_get(target sha1_git)
       returns setof cache_content_signature
       language sql
       stable
as $$
    SELECT c.sha1, c.sha1_git, c.sha256, ccr.revision_paths
    FROM cache_content_revision ccr
    INNER JOIN content as c
    ON ccr.content = c.sha1_git
    where ccr.content = target
$$;

COMMENT ON FUNCTION swh_cache_content_get(sha1_git) IS 'Retrieve cache content information';
