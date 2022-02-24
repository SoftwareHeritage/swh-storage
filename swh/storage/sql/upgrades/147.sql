-- SWH DB schema upgrade
-- from_version: 146
-- to_version: 147
-- description: Add origin_visit_status table
-- 1. Rename enum origin_visit_status to origin_visit_state
-- 2. Add new origin_visit_status table
-- 3. Migrate origin_visit data to new origin_visit_status data

-- latest schema version
insert into dbversion(version, release, description)
      values(147, now(), 'Work In Progress');

-- schema change

-- Rename old enum
alter type origin_visit_status rename to origin_visit_state;
comment on type origin_visit_state IS 'Possible visit status';

alter type origin_visit_state add value 'created' before 'ongoing';
comment on type origin_visit_state IS 'Possible origin visit state values';

-- Update origin visit comment on deprecated columns
comment on column origin_visit.status is '(Deprecated) Visit status';
comment on column origin_visit.metadata is '(Deprecated) Optional origin visit metadata';
comment on column origin_visit.snapshot is '(Deprecated) Optional, possibly partial, snapshot of the origin visit.';


-- Crawling history of software origin visits by Software Heritage. Each
-- visit see its history change through new origin visit status updates
create table origin_visit_status
(
  origin   bigint not null,
  visit    bigint not null,
  date     timestamptz not null,
  status   origin_visit_state not null,
  metadata jsonb,
  snapshot sha1_git
);

comment on column origin_visit_status.origin is 'Origin concerned by the visit update';
comment on column origin_visit_status.visit is 'Visit concerned by the visit update';
comment on column origin_visit_status.date is 'Visit update timestamp';
comment on column origin_visit_status.status is 'Visit status (ongoing, failed, full)';
comment on column origin_visit_status.metadata is 'Optional origin visit metadata';
comment on column origin_visit_status.snapshot is 'Optional, possibly partial, snapshot of the origin visit.';


-- origin_visit_status

create unique index origin_visit_status_pkey on origin_visit_status(origin, visit, date);
alter table origin_visit_status add primary key using index origin_visit_status_pkey;

alter table origin_visit_status
  add constraint origin_visit_status_origin_visit_fkey
  foreign key (origin, visit)
  references origin_visit(origin, visit) not valid;
alter table origin_visit_status validate constraint origin_visit_status_origin_visit_fkey;


-- data change

-- best approximation of the visit update date is the origin_visit's date
insert into origin_visit_status (origin, visit, date, status, metadata, snapshot)
select origin, visit, date, status, metadata, snapshot
from origin_visit
on conflict (origin, visit, date)
do nothing;
