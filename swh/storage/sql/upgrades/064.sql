-- SWH DB schema upgrade
-- from_version: 63
-- to_version: 64
-- description: Convert all json columns to jsonb

insert into dbversion(version, release, description)
      values(64, now(), 'Work In Progress');

ALTER TABLE fetch_history
	ALTER COLUMN "result" TYPE jsonb /* TYPE change - table: fetch_history original: json new: jsonb */;

ALTER TABLE list_history
	ALTER COLUMN "result" TYPE jsonb /* TYPE change - table: list_history original: json new: jsonb */;

ALTER TABLE listable_entity
	ALTER COLUMN list_params TYPE jsonb /* TYPE change - table: listable_entity original: json new: jsonb */;
