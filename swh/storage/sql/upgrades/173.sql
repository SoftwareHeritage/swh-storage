-- SWH DB schema upgrade
-- from_version: 172
-- to_version: 173
-- description: remove unicity on (extid_type, extid) and (target_type, target)

insert into dbversion(version, release, description)
  values(173, now(), 'Work In Progress');

-- At the time this migratinon runs, the table is empty,
-- so no need to bother about performance
drop index extid_extid_type_extid_idx;
drop index extid_target_type_target_idx;
create unique index concurrently on extid(extid_type, extid, target_type, target);
create index concurrently on extid(target_type, target);
