-- SWH DB schema upgrade
-- from_version: 121
-- to_version: 122
-- description: Remove some unused functions

insert into dbversion(version, release, description)
      values(122, now(), 'Work In Progress');

DROP FUNCTION swh_occurrence_by_origin_visit(origin_id bigint, visit_id bigint);

DROP FUNCTION swh_revision_find_occurrence(revision_id public.sha1_git);
