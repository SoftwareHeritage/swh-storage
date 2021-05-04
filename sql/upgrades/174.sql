-- SWH DB schema upgrade
-- from_version: 173
-- to_version: 174
-- description: remove authority and fetcher metadata

insert into dbversion(version, release, description)
  values(174, now(), 'Work In Progress');

alter table metadata_authority
  drop column metadata;
alter table metadata_fetcher
  drop column metadata;
