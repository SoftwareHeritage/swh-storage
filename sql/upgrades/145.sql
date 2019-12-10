-- SWH DB schema upgrade
-- from_version: 144
-- to_version: 145
-- description: Improve query on origin_visit

insert into dbversion(version, release, description)
      values(145, now(), 'Work In Progress');

create index concurrently on origin_visit(type, status, date);
