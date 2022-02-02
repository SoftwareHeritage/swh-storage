-- SWH DB schema upgrade
-- from_version: 159
-- to_version: 160
-- description: Make neg_utc_offset not null

-- latest schema version
insert into dbversion(version, release, description)
      values(160, now(), 'Work Still In Progress');

update revision
    set date_neg_utc_offset=false
    where date is not null and date_neg_utc_offset is null;
update revision
    set committer_date_neg_utc_offset=false
    where committer_date is not null and committer_date_neg_utc_offset is null;

alter table revision
    add constraint revision_date_neg_utc_offset_not_null
    check (date is null or date_neg_utc_offset is not null)
    not valid;
alter table revision
    add constraint revision_committer_date_neg_utc_offset_not_null
    check (committer_date is null or committer_date_neg_utc_offset is not null)
    not valid;

alter table revision
    validate constraint revision_date_neg_utc_offset_not_null;
alter table revision
    validate constraint revision_committer_date_neg_utc_offset_not_null;

update release
    set date_neg_utc_offset=false
    where date is not null and date_neg_utc_offset is null;

alter table release
    add constraint release_date_neg_utc_offset_not_null
    check (date is null or date_neg_utc_offset is not null)
    not valid;

alter table release
    validate constraint release_date_neg_utc_offset_not_null;
