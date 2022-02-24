-- SWH DB schema upgrade
-- from_version: 163
-- to_version: 164
-- description: rename raw_extrinsic_metadata.id to raw_extrinsic_metadata.target

insert into dbversion(version, release, description)
      values(164, now(), 'Work In Progress');

alter table raw_extrinsic_metadata rename id to target;
