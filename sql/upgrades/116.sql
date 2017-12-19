-- SWH DB schema upgrade
-- from_version: 115
-- to_version: 116
-- description: Add hg revision type

insert into dbversion(version, release, description)
      values(116, now(), 'Work In Progress');

ALTER TYPE revision_type
ADD VALUE 'hg';  -- added at the end by default
