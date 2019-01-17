-- Asynchronous notification of new content insertions
create function notify_new_content()
  returns trigger
  language plpgsql
as $$
  begin
    perform pg_notify('new_content', json_build_object(
      'sha1', encode(new.sha1, 'hex'),
      'sha1_git', encode(new.sha1_git, 'hex'),
      'sha256', encode(new.sha256, 'hex'),
      'blake2s256', encode(new.blake2s256, 'hex')
    )::text);
    return null;
  end;
$$;

create trigger notify_new_content
  after insert on content
  for each row
  execute procedure notify_new_content();


-- Asynchronous notification of new origin insertions
create function notify_new_origin()
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

create trigger notify_new_origin
  after insert on origin
  for each row
  execute procedure notify_new_origin();


-- Asynchronous notification of new skipped content insertions
create function notify_new_skipped_content()
  returns trigger
  language plpgsql
as $$
  begin
    perform pg_notify('new_skipped_content', json_build_object(
      'sha1', encode(new.sha1, 'hex'),
      'sha1_git', encode(new.sha1_git, 'hex'),
      'sha256', encode(new.sha256, 'hex'),
      'blake2s256', encode(new.blake2s256, 'hex')
    )::text);
    return null;
  end;
$$;

create trigger notify_new_skipped_content
  after insert on skipped_content
  for each row
  execute procedure notify_new_skipped_content();


-- Asynchronous notification of new directory insertions
create function notify_new_directory()
  returns trigger
  language plpgsql
as $$
  begin
    perform pg_notify('new_directory', json_build_object('id', encode(new.id, 'hex'))::text);
    return null;
  end;
$$;

create trigger notify_new_directory
  after insert on directory
  for each row
  execute procedure notify_new_directory();


-- Asynchronous notification of new revision insertions
create function notify_new_revision()
  returns trigger
  language plpgsql
as $$
  begin
    perform pg_notify('new_revision', json_build_object('id', encode(new.id, 'hex'))::text);
    return null;
  end;
$$;

create trigger notify_new_revision
  after insert on revision
  for each row
  execute procedure notify_new_revision();


-- Asynchronous notification of new origin visits
create function notify_new_origin_visit()
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

create trigger notify_new_origin_visit
  after insert on origin_visit
  for each row
  execute procedure notify_new_origin_visit();


create trigger notify_changed_origin_visit
  after update on origin_visit
  for each row
  execute procedure notify_new_origin_visit();


-- Asynchronous notification of new release insertions
create function notify_new_release()
  returns trigger
  language plpgsql
as $$
  begin
    perform pg_notify('new_release', json_build_object('id', encode(new.id, 'hex'))::text);
    return null;
  end;
$$;

create trigger notify_new_release
  after insert on release
  for each row
  execute procedure notify_new_release();


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
