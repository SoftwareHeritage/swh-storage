-- SWH DB schema upgrade
-- from_version: 132
-- to_version: 133
-- description: Use sha1 instead of bigint as FK from origin_visit to snapshot (part 3: remove old column)

insert into dbversion(version, release, description)
      values(133, now(), 'Work In Progress');

drop index constraint origin_visit_snapshot_id_fkey;
alter table origin_visit drop column snapshot_id;

drop function swh_snapshot_get_by_origin_visit;
