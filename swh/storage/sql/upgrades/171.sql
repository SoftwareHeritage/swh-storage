-- SWH DB schema upgrade
-- from_version: 170
-- to_version: 171
-- description: add raw_extrinsic_metadata.id

insert into dbversion(version, release, description)
  values(171, now(), 'Work In Progress');

-- 1. add the 'id' column

alter table raw_extrinsic_metadata
  add column id sha1_git;

-- 2. restart swh-storage, so that it starts writing the id (but does not read it)

-- 3. truncate the raw_extrinsic_metadata table

-- 4. make the id column not null, and index it

alter table raw_extrinsic_metadata
  alter column id set not null;

create unique index concurrently raw_extrinsic_metadata_pkey on raw_extrinsic_metadata(id);
alter table raw_extrinsic_metadata add primary key using index raw_extrinsic_metadata_pkey;

-- 5. backfill from kafka
