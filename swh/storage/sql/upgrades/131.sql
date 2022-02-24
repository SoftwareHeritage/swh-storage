-- SWH DB schema upgrade
-- from_version: 130
-- to_version: 131
-- description: Use sha1 instead of bigint as FK from origin_visit to snapshot (part 1: add new column)

insert into dbversion(version, release, description)
      values(131, now(), 'Work In Progress');

alter table origin_visit add column snapshot sha1_git;
comment on column origin_visit.snapshot is 'Origin snapshot at visit time';
