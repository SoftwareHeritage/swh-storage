-- SWH DB schema upgrade
-- from_version: 125
-- to_version: 126
-- description: drop useless function swh_revision_get_by

insert into dbversion(version, release, description)
      values(126, now(), 'Work In Progress');

DROP FUNCTION swh_revision_get_by(origin_id bigint, branch_name bytea, "date" timestamp with time zone);
