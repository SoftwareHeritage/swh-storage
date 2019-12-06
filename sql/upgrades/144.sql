-- SWH DB schema upgrade
-- from_version: 143
-- to_version: 144
-- description: add index on sha1(origin.url)

insert into dbversion(version, release, description)
      values(144, now(), 'Work In Progress');

create index concurrently on origin using btree(digest(url, 'sha1'));

