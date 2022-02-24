-- SWH DB schema upgrade
-- from_version: 171
-- to_version: 172
-- description: add raw_extrinsic_metadata.id

insert into dbversion(version, release, description)
  values(172, now(), 'Work In Progress');
-- 1. restart swh-storage, so that it stops relying on
--    raw_extrinsic_metadata_content_authority_date_fetcher being a unique index

-- 2. rename old index

alter index raw_extrinsic_metadata_content_authority_date_fetcher
  rename to raw_extrinsic_metadata_content_authority_date_fetcher_unique;

-- 3. create new (non-unique) index (excluding fetcher, because it was only needed
--    for uniqueness and not for accesses

create index concurrently raw_extrinsic_metadata_content_authority_date
  on raw_extrinsic_metadata(target, authority_id, discovery_date);

-- 4. remove old index

drop index raw_extrinsic_metadata_content_authority_date_fetcher_unique;

