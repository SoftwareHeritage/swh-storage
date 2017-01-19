-- SWH DB schema upgrade
-- from_version: 97
-- to_version: 98
-- description: remove duplicate index on occurrence_history(object_id)

insert into dbversion(version, release, description)
      values(98, now(), 'Work In Progress');

DROP INDEX occurrence_history_object_id_idx;
