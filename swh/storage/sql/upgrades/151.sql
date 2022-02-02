-- SWH DB schema upgrade
-- from_version: 150
-- to_version: 151
-- description: Drop unused functions

-- latest schema version
insert into dbversion(version, release, description)
      values(151, now(), 'Work In Progress');

drop function swh_content_find;
drop function swh_directory_missing;
drop function swh_revision_walk;
drop function swh_revision_list_children;
