-- SWH DB schema upgrade
-- from_version: 128
-- to_version: 129
-- description: Update origin trigger to provide the full origin

insert into dbversion(version, release, description)
      values(129, now(), 'Work In Progress');

-- Asynchronous notification of new origin insertions
create or replace function notify_new_origin()
  returns trigger
  language plpgsql
as $$
  begin
    perform pg_notify('new_origin', json_build_object(
      'url', new.url::text,
      'type', new.type::text
    )::text);
    return null;
  end;
$$;

drop trigger notify_new_origin on origin;
create trigger notify_new_origin
  after insert on origin
  for each row
  execute procedure notify_new_origin();


create trigger notify_changed_origin_visit
  after update on origin_visit
  for each row
  execute procedure notify_new_origin_visit();
