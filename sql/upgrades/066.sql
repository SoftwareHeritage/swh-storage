-- SWH DB schema upgrade
-- from_version: 65
-- to_version: 66
-- description: Add revision type svn

insert into dbversion(version, release, description)
      values(66, now(), 'Work In Progress');

ALTER TYPE revision_type
      ADD VALUE 'svn';  -- added at the end by default
