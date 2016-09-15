-- SWH DB schema upgrade
-- from_version: 79
-- to_version: 80
-- description: Permit to retrieve distinct batch of cache contents

insert into dbversion(version, release, description)
      values(80, now(), 'Work In Progress');

create or replace function swh_cache_content_get_by_batch(last_content bytea, batch_limit bigint)
       returns setof content_signature
       language sql
       stable
as $$
    SELECT DISTINCT c.sha1, c.sha1_git, c.sha256
    FROM cache_content_revision ccr
    INNER JOIN content as c
    ON ccr.content = c.sha1_git
    WHERE c.sha1 > last_content
    ORDER BY c.sha1
    LIMIT batch_limit
$$;
