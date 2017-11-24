-- SWH DB schema upgrade
-- from_version: 111
-- to_version: 112
-- description: make unique indexes unique and vice versa.

insert into dbversion(version, release, description)
      values(112, now(), 'Work In Progress');

ALTER INDEX content_object_id_idx rename to content_object_id_idx_2;

ALTER INDEX directory_object_id_idx rename to directory_object_id_idx_2;

ALTER INDEX release_object_id_idx rename to release_object_id_idx_2;

ALTER INDEX revision_object_id_idx rename to revision_object_id_idx_2;

ALTER INDEX skipped_content_object_id_idx rename to skipped_content_object_id_idx_2;

ALTER INDEX skipped_content_sha1_git_idx rename to skipped_content_sha1_git_idx_2;

ALTER INDEX skipped_content_sha1_idx rename to skipped_content_sha1_idx_2;

CREATE UNIQUE INDEX content_object_id_idx ON content USING btree (object_id);

CREATE UNIQUE INDEX directory_object_id_idx ON directory USING btree (object_id);

CREATE UNIQUE INDEX release_object_id_idx ON "release" USING btree (object_id);

CREATE UNIQUE INDEX revision_object_id_idx ON revision USING btree (object_id);

CREATE UNIQUE INDEX skipped_content_object_id_idx ON skipped_content USING btree (object_id);

CREATE INDEX skipped_content_sha1_git_idx ON skipped_content USING btree (sha1_git);

CREATE INDEX skipped_content_sha1_idx ON skipped_content USING btree (sha1);

DROP INDEX content_object_id_idx_2;

DROP INDEX directory_object_id_idx_2;

DROP INDEX release_object_id_idx_2;

DROP INDEX revision_object_id_idx_2;

DROP INDEX skipped_content_object_id_idx_2;

DROP INDEX skipped_content_sha1_git_idx_2;

DROP INDEX skipped_content_sha1_idx_2;

