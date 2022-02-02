-- SWH DB schema upgrade
-- from_version: 19
-- to_version: 20
-- description: add table aliases to "where not exists" queries

insert into dbversion(version, release, description)
      values(20, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_directory_missing() RETURNS SETOF sha1_git
    LANGUAGE plpgsql
    AS $$
begin
    return query
	select id from tmp_directory t
	where not exists (
	    select 1 from directory d
	    where d.id = t.id);
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_release_missing() RETURNS SETOF sha1_git
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select id from tmp_release t
	where not exists (
	select 1 from release r
	where r.id = t.id);
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_revision_missing() RETURNS SETOF sha1_git
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select id from tmp_revision t
	where not exists (
	    select 1 from revision r
	    where r.id = t.id);
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_skipped_content_missing() RETURNS SETOF content_signature
    LANGUAGE plpgsql
    AS $$
begin
    return query
	select sha1, sha1_git, sha256 from tmp_skipped_content t
	where not exists
	(select 1 from skipped_content s where
	    s.sha1 is not distinct from t.sha1 and
	    s.sha1_git is not distinct from t.sha1_git and
	    s.sha256 is not distinct from t.sha256);
    return;
end
$$;
