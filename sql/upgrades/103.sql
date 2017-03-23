-- SWH DB schema upgrade
-- from_version: 102
-- to_version: 103
-- description: Update notify_new_* functions

insert into dbversion(version, release, description)
      values(103, now(), 'Work In Progress');

-- Asynchronous notification of new content insertions
create or replace function notify_new_content()
  returns trigger
  language plpgsql
as $$
  begin
    perform pg_notify('new_content', json_build_object(
      'sha1', encode(new.sha1, 'hex'),
      'sha1_git', encode(new.sha1_git, 'hex'),
      'sha256', encode(new.sha256, 'hex')
    )::text);
    return null;
  end;
$$;

-- Asynchronous notification of new origin insertions
create or replace function notify_new_origin()
  returns trigger
  language plpgsql
as $$
  begin
    perform pg_notify('new_origin', json_build_object('id', new.id)::text);
    return null;
  end;
$$;

-- Asynchronous notification of new skipped content insertions
create or replace function notify_new_skipped_content()
  returns trigger
  language plpgsql
as $$
  begin
    perform pg_notify('new_skipped_content', json_build_object(
      'sha1', encode(new.sha1, 'hex'),
      'sha1_git', encode(new.sha1_git, 'hex'),
      'sha256', encode(new.sha256, 'hex')
    )::text);
    return null;
  end;
$$;

-- Asynchronous notification of new directory insertions
create or replace function notify_new_directory()
  returns trigger
  language plpgsql
as $$
  begin
    perform pg_notify('new_directory', json_build_object('id', encode(new.id, 'hex'))::text);
    return null;
  end;
$$;

-- Asynchronous notification of new revision insertions
create or replace function notify_new_revision()
  returns trigger
  language plpgsql
as $$
  begin
    perform pg_notify('new_revision', json_build_object('id', encode(new.id, 'hex'))::text);
    return null;
  end;
$$;

-- Asynchronous notification of new origin visits
create or replace function notify_new_origin_visit()
  returns trigger
  language plpgsql
as $$
  begin
    perform pg_notify('new_origin_visit', json_build_object(
      'origin', new.origin,
      'visit', new.visit
    )::text);
    return null;
  end;
$$;

-- Asynchronous notification of new release insertions
create or replace function notify_new_release()
  returns trigger
  language plpgsql
as $$
  begin
    perform pg_notify('new_release', json_build_object('id', encode(new.id, 'hex'))::text);
    return null;
  end;
$$;
