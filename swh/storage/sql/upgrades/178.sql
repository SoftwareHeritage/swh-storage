-- SWH DB schema upgrade
-- from_version: 177
-- to_version: 178
-- description: add bzr to revision_type values

insert into dbversion(version, release, description)
      values(178, now(), 'Work In Progress');

alter type revision_type add value 'bzr';
