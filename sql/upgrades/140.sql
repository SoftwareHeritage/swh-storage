-- SWH DB schema upgrade
-- from_version: 139
-- to_version: 140
-- description: Add constraint checking that release.author is null implies that release.date is null.

insert into dbversion(version, release, description)
      values(140, now(), 'Work In Progress');

alter table release add constraint release_author_date_check check ((date is null) or (author is not null)) not valid;
alter table release validate constraint release_author_date_check;

