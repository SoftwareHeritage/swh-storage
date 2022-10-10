-- SWH DB schema upgrade
-- from_version: 185
-- to_version: 186
-- description: Clean up indexes on origin_visit and add proper index for origin_visit_find_by_date

drop index if exists origin_visit_date_idx;
drop index if exists origin_visit_type_date;

create index concurrently if not exists origin_visit_origin_date_idx on origin_visit(origin, date);
