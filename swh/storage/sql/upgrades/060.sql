-- SWH DB schema upgrade
-- from_version: 59
-- to_version: 60
-- description: add swh_object_find_by_sha1_git

insert into dbversion(version, release, description)
      values(60, now(), 'Work In Progress');

create type object_found as (
    sha1_git   sha1_git,
    type       object_type,
    id         bytea,       -- sha1 or sha1_git depending on object_type
    object_id  bigint
);


CREATE OR REPLACE FUNCTION swh_object_find_by_sha1_git() RETURNS SETOF object_found
    LANGUAGE plpgsql
    AS $$
begin
    return query
    with known_objects as ((
        select id as sha1_git, 'release'::object_type as type, id, object_id from release r
        where exists (select 1 from tmp_bytea t where t.id = r.id)
    ) union all (
        select id as sha1_git, 'revision'::object_type as type, id, object_id from revision r
        where exists (select 1 from tmp_bytea t where t.id = r.id)
    ) union all (
        select id as sha1_git, 'directory'::object_type as type, id, object_id from directory d
        where exists (select 1 from tmp_bytea t where t.id = d.id)
    ) union all (
        select sha1_git as sha1_git, 'content'::object_type as type, sha1 as id, object_id from content c
        where exists (select 1 from tmp_bytea t where t.id = c.sha1_git)
    ))
    select t.id::sha1_git as sha1_git, k.type, k.id, k.object_id from tmp_bytea t
      left join known_objects k on t.id = k.sha1_git;
end
$$;
