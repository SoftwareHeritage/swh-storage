-- SWH DB schema upgrade
-- from_version: 166
-- to_version: 167
-- description: Make origin_visit_status.type not null

insert into dbversion(version, release, description)
      values(167, now(), 'Work In Progress');

-- Data migrated, all values populated now
alter table origin_visit_status
  alter column type set not null;
