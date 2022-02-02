-- SWH DB schema upgrade
-- from_version: 131
-- to_version: 132
-- description: Use sha1 instead of bigint as FK from origin_visit to snapshot (part 2: backfill)

insert into dbversion(version, release, description)
      values(132, now(), 'Work In Progress');

update origin_visit
    set snapshot=snapshot.id
    from snapshot
    where snapshot.object_id=origin_visit.snapshot_id;
