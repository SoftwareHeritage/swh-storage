-- SWH DB schema upgrade
-- from_version: 166
-- to_version: 167
-- description: Make origin_visit_status.type not null

insert into dbversion(version, release, description)
      values(167, now(), 'Work In Progress');

-- (blocking) Data migrated, all values populated now, so we can add the constraint
-- alter table origin_visit_status alter column type set not null;

-- (unblocking) functionally equivalent constraint
alter table origin_visit_status
  add constraint type_not_null check (type is not null) not valid;

alter table origin_visit_status validate constraint type_not_null;
