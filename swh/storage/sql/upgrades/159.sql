-- SWH DB schema upgrade
-- from_version: 158
-- to_version: 159
-- description: Rename "object_metadata" to "raw_extrinsic_metadata"

-- latest schema version
insert into dbversion(version, release, description)
      values(159, now(), 'Work Still In Progress');

alter table object_metadata
    rename to raw_extrinsic_metadata;

alter index object_metadata_content_authority_date_fetcher
    rename to raw_extrinsic_metadata_content_authority_date_fetcher;

alter table raw_extrinsic_metadata
    rename constraint object_metadata_authority_fkey
    to raw_extrinsic_metadata_authority_fkey;

alter table raw_extrinsic_metadata
    rename constraint object_metadata_fetcher_fkey
    to raw_extrinsic_metadata_fetcher_fkey;
