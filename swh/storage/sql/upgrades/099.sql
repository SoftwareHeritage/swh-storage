-- SWH DB schema upgrade
-- from_version: 98
-- to_version: 99
-- description: Make schema match the production database

insert into dbversion(version, release, description)
      values(99, now(), 'Work In Progress');

DROP SEQUENCE content_ctags_indexer_configuration_id_seq;

DROP SEQUENCE content_fossology_license_indexer_configuration_id_seq;

DROP SEQUENCE content_language_indexer_configuration_id_seq;

DROP SEQUENCE content_mimetype_indexer_configuration_id_seq;

ALTER TABLE content_ctags
	ALTER COLUMN indexer_configuration_id DROP DEFAULT;

ALTER TABLE content_fossology_license
	ALTER COLUMN indexer_configuration_id DROP DEFAULT;

ALTER TABLE content_language
	ALTER COLUMN indexer_configuration_id DROP DEFAULT;

ALTER TABLE content_mimetype
	ALTER COLUMN indexer_configuration_id DROP DEFAULT;

ALTER TABLE "release"
	ALTER COLUMN target_type SET NOT NULL;
