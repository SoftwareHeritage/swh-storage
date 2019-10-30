-- SWH DB schema upgrade
-- from_version: 141
-- to_version: 142
-- description: Remove origin.type

insert into dbversion(version, release, description)
      values(142, now(), 'Work In Progress');

alter table origin drop column type;

