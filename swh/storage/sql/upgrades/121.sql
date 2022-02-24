-- SWH DB schema upgrade
-- from_version: 120
-- to_version: 121
-- description: Drop unused function swh_release_get_by

insert into dbversion(version, release, description)
      values(121, now(), 'Work In Progress');

DROP FUNCTION swh_release_get_by(origin_id bigint);
