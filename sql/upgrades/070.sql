-- SWH DB schema upgrade
-- from_version: 69
-- to_version: 70
-- description: Drop the archiver's related tables in main schema (move to its own database)

insert into dbversion(version, release, description)
      values(70, now(), 'Work In Progress');

drop domain archive_id;

drop tab le archives;

drop type archive_status;

drop table content_archive;
