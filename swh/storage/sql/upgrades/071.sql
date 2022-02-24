-- SWH DB schema upgrade
-- from_version: 70
-- to_version: 71
-- description: Create indexes for object_ids.

insert into dbversion(version, release, description)
      values(71, now(), 'Work In Progress');

CREATE INDEX content_object_id_idx ON content USING btree (object_id);

CREATE INDEX directory_object_id_idx ON directory USING btree (object_id);

CREATE INDEX occurrence_history_object_id_idx ON occurrence_history USING btree (object_id);

CREATE INDEX release_object_id_idx ON "release" USING btree (object_id);

CREATE INDEX revision_object_id_idx ON revision USING btree (object_id);

CREATE INDEX skipped_content_object_id_idx ON skipped_content USING btree (object_id);
