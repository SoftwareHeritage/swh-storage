-- SWH DB schema upgrade
-- from_version: 56
-- to_version: 57
-- description: Add not null and exclusion constraints on occurrence and occurrence_history

insert into dbversion(version, release, description)
      values(57, now(), 'Work In Progress');

ALTER TABLE occurrence_history
	ALTER COLUMN origin SET NOT NULL,
	ALTER COLUMN branch SET NOT NULL,
	ALTER COLUMN target SET NOT NULL,
	ALTER COLUMN target_type SET NOT NULL,
	ALTER COLUMN visits SET NOT NULL;

ALTER TABLE occurrence
	ALTER COLUMN target SET NOT NULL,
	ALTER COLUMN target_type SET NOT NULL;

CREATE UNIQUE INDEX if not exists occurrence_history_origin_branch_target_target_type_idx ON occurrence_history USING btree (origin, branch, target, target_type);
