-- SWH DB schema upgrade
-- from_version: 134
-- to_version: 135
-- description: Add an index on origin.url, drop the index on (origin.type, origin.url)

insert into dbversion(version, release, description)
      values(135, now(), 'Work In Progress');

create extension if not exists pg_trgm;

create index on origin using gin (url gin_trgm_ops);
create index on origin using hash (url);
drop index origin_type_url_idx;
