-- SWH DB schema upgrade
-- from_version: 176
-- to_version: 177
-- description: add cvs to revision_type values

insert into dbversion(version, release, description)
      values(177, now(), 'Work In Progress');

alter type revision_type add value 'cvs';
