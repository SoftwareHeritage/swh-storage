-- SWH DB schema upgrade
-- from_version: 73
-- to_version: 74
-- description: Add notifications for object creations in the whole database

insert into dbversion(version, release, description)
      values(74, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION notify_new_content() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
  begin
    perform pg_notify('new_content', encode(new.sha1, 'hex'));
    return null;
  end;
$$;

CREATE OR REPLACE FUNCTION notify_new_directory() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
  begin
    perform pg_notify('new_directory', encode(new.id, 'hex'));
    return null;
  end;
$$;

CREATE OR REPLACE FUNCTION notify_new_origin() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
  begin
    perform pg_notify('new_origin', new.id::text);
    return null;
  end;
$$;

CREATE OR REPLACE FUNCTION notify_new_origin_visit() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
  begin
    perform pg_notify('new_origin_visit', json_build_object(
      'origin', new.origin,
      'visit', new.visit
    )::text);
    return null;
  end;
$$;

CREATE OR REPLACE FUNCTION notify_new_release() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
  begin
    perform pg_notify('new_release', encode(new.id, 'hex'));
    return null;
  end;
$$;

CREATE OR REPLACE FUNCTION notify_new_revision() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
  begin
    perform pg_notify('new_revision', encode(new.id, 'hex'));
    return null;
  end;
$$;

CREATE OR REPLACE FUNCTION notify_new_skipped_content() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
  begin
  perform pg_notify('new_skipped_content', json_build_object(
      'sha1', encode(new.sha1, 'hex'),
      'sha1_git', encode(new.sha1_git, 'hex'),
      'sha256', encode(new.sha256, 'hex')
    )::text);
    return null;
  end;
$$;

CREATE TRIGGER notify_new_content
	AFTER INSERT ON content
	FOR EACH ROW
	EXECUTE PROCEDURE notify_new_content();

CREATE TRIGGER notify_new_directory
	AFTER INSERT ON directory
	FOR EACH ROW
	EXECUTE PROCEDURE notify_new_directory();

CREATE TRIGGER notify_new_origin_visit
	AFTER INSERT ON origin_visit
	FOR EACH ROW
	EXECUTE PROCEDURE notify_new_origin_visit();

CREATE TRIGGER notify_new_origin
	AFTER INSERT ON origin
	FOR EACH ROW
	EXECUTE PROCEDURE notify_new_origin();

CREATE TRIGGER notify_new_release
	AFTER INSERT ON "release"
	FOR EACH ROW
	EXECUTE PROCEDURE notify_new_release();

CREATE TRIGGER notify_new_revision
	AFTER INSERT ON revision
	FOR EACH ROW
	EXECUTE PROCEDURE notify_new_revision();

CREATE TRIGGER notify_new_skipped_content
	AFTER INSERT ON skipped_content
	FOR EACH ROW
	EXECUTE PROCEDURE notify_new_skipped_content();
