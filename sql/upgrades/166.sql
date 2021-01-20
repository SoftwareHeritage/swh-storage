-- SWH DB schema upgrade
-- from_version: 165
-- to_version: 166
-- description: add not_found status

insert into dbversion(version, release, description)
      values(166, now(), 'Work In Progress');

alter type origin_visit_state add value 'not_found';
alter type origin_visit_state add value 'failed';
