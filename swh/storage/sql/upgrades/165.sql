-- SWH DB schema upgrade
-- from_version: 164
-- to_version: 165
-- description: add type to origin_visit_status

insert into dbversion(version, release, description)
      values(165, now(), 'Work In Progress');

-- Adapt the origin_visit_status table for the new type column
alter table origin_visit_status add column type text;

comment on column origin_visit_status.type is 'Type of loader that did the visit (hg, git, ...)';
