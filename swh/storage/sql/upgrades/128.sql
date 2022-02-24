-- SWH DB schema upgrade
-- from_version: 127
-- to_version: 128
-- description: Add snapshot trigger event on insertion

insert into dbversion(version, release, description)
      values(128, now(), 'Work In Progress');

-- Asynchronous notification of new snapshot insertions
create function notify_new_snapshot()
  returns trigger
  language plpgsql
as $$
  begin
    perform pg_notify('new_snapshot', json_build_object('id', encode(new.id, 'hex'))::text);
    return null;
  end;
$$;

create trigger notify_new_snapshot
  after insert on snapshot
  for each row
  execute procedure notify_new_snapshot();
