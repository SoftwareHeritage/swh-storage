-- SWH DB schema upgrade
-- from_version: 110
-- to_version: 111
-- description: Drop tables related to content provenance

insert into dbversion(version, release, description)
      values(111, now(), 'Work In Progress');

DROP FUNCTION swh_cache_content_get(target sha1_git);

DROP FUNCTION swh_cache_content_get_all();

DROP FUNCTION swh_cache_content_revision_add();

DROP FUNCTION swh_cache_revision_origin_add(origin_id bigint, visit_id bigint);

DROP FUNCTION swh_content_find_provenance(content_id sha1_git);

DROP FUNCTION swh_revision_from_target(target sha1_git, target_type object_type);

DROP TABLE cache_content_revision;

DROP TABLE cache_content_revision_processed;

DROP TABLE cache_revision_origin;

DROP TYPE cache_content_signature;

DROP TYPE content_provenance;
