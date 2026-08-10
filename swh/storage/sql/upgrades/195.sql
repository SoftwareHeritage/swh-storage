-- SWH DB schema upgrade
-- from_version: 194
-- to_version: 195
-- description: Drop unneeded foreign key on origin_visit_status table

alter table origin_visit_status
    drop constraint if exists origin_visit_status_origin_visit_fkey;
