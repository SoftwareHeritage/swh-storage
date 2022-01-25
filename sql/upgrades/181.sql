-- SWH DB schema upgrade
-- from_version: 180
-- to_version: 181
-- description: add *_raw_manifest_not_null indexes

insert into dbversion(version, release, description)
    values(181, now(), 'Work In Progress');

-- copied from 60-indexes.sql
select swh_get_dbflavor() = 'default' as dbflavor_default \gset

\if :dbflavor_default
  create index concurrently directory_raw_manifest_not_null on directory(id) where raw_manifest is not null;
  create index concurrently revision_raw_manifest_not_null on revision(id) where raw_manifest is not null;
  create index concurrently release_raw_manifest_not_null on release(id) where raw_manifest is not null;
\endif
