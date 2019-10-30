-- SWH DB schema upgrade
-- from_version: 140
-- to_version: 141
-- description: Remove fetch history

insert into dbversion(version, release, description)
      values(141, now(), 'Work In Progress');

drop table fetch_history;

