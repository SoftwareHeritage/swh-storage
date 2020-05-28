-- SWH DB schema upgrade
-- from_version: 151
-- to_version: 152
-- description: Make content.blake2s256 not null

-- latest schema version
insert into dbversion(version, release, description)
      values(152, now(), 'Work In Progress');

alter table content
    alter column blake2s256 set not null;
