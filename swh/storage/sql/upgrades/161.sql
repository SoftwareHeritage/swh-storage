-- SWH DB schema upgrade
-- from_version: 160
-- to_version: 161
-- description: Make revision.extra_headers not null

-- latest schema version
insert into dbversion(version, release, description)
      values(161, now(), 'Work Still In Progress');


update revision
    set extra_headers = ARRAY[]::bytea[][]
    where extra_headers is null;

alter table revision
    add constraint revision_extra_headers_not_null
    check (extra_headers is not null)
    not valid;

alter table revision
    validate constraint revision_extra_headers_not_null;
