-- SWH DB schema upgrade
-- from_version: 103
-- to_version: 104
-- description: Compute new hash blake2s256

insert into dbversion(version, release, description)
      values(104, now(), 'Work In Progress');

DROP FUNCTION swh_content_find(sha1 sha1, sha1_git sha1_git, sha256 sha256);

DROP INDEX content_sha256_idx;

DROP INDEX skipped_content_sha256_idx;

create domain blake2s256 as bytea check (length(value) = 32);

ALTER TABLE content
	ADD COLUMN blake2s256 blake2s256;

ALTER TABLE skipped_content
	ADD COLUMN blake2s256 blake2s256;

CREATE OR REPLACE FUNCTION notify_new_content() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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

CREATE OR REPLACE FUNCTION notify_new_skipped_content() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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

CREATE OR REPLACE FUNCTION swh_content_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    insert into content (sha1, sha1_git, sha256, blake2s256, length, status)
        select distinct sha1, sha1_git, sha256, blake2s256, length, status
	from tmp_content
	where (sha1, sha1_git, sha256) in (
            select sha1, sha1_git, sha256
            from swh_content_missing()
        );
        -- TODO XXX use postgres 9.5 "UPSERT" support here, when available.
        -- Specifically, using "INSERT .. ON CONFLICT IGNORE" we can avoid
        -- the extra swh_content_missing() query here.
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_content_find(sha1 sha1 = NULL::bytea, sha1_git sha1_git = NULL::bytea, sha256 sha256 = NULL::bytea, blake2s256 blake2s256 = NULL::bytea) RETURNS content
    LANGUAGE plpgsql
    AS $$
declare
    con content;
    filters text[] := array[] :: text[];  -- AND-clauses used to filter content
    q text;
begin
    if sha1 is not null then
        filters := filters || format('sha1 = %L', sha1);
    end if;
    if sha1_git is not null then
        filters := filters || format('sha1_git = %L', sha1_git);
    end if;
    if sha256 is not null then
        filters := filters || format('sha256 = %L', sha256);
    end if;
    if blake2s256 is not null then
        filters := filters || format('blake2s256 = %L', blake2s256);
    end if;

    if cardinality(filters) = 0 then
        return null;
    else
        q = format('select * from content where %s',
                   array_to_string(filters, ' and '));
        execute q into con;
	return con;
    end if;
end
$$;

drop type content_signature cascade;

create type content_signature as (
    sha1       sha1,
    sha1_git   sha1_git,
    sha256     sha256,
    blake2s256 blake2s256
);


CREATE OR REPLACE FUNCTION swh_content_missing() RETURNS SETOF content_signature
    LANGUAGE plpgsql
    AS $$
begin
    return query (
      select sha1, sha1_git, sha256, blake2s256 from tmp_content as tmp
      where not exists (
        select 1 from content as c
        where c.sha1 = tmp.sha1 and
              c.sha1_git = tmp.sha1_git and
              c.sha256 = tmp.sha256
      )
    );
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_skipped_content_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    insert into skipped_content (sha1, sha1_git, sha256, blake2s256, length, status, reason, origin)
        select distinct sha1, sha1_git, sha256, blake2s256, length, status, reason, origin
	from tmp_skipped_content
	where (coalesce(sha1, ''), coalesce(sha1_git, ''), coalesce(sha256, '') in (
             select coalesce(sha1, ''), coalesce(sha1_git, ''), coalesce(sha256, '')
             from swh_skipped_content_missing()
        );
        -- TODO XXX use postgres 9.5 "UPSERT" support here, when available.
        -- Specifically, using "INSERT .. ON CONFLICT IGNORE" we can avoid
        -- the extra swh_content_missing() query here.
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_skipped_content_missing() RETURNS SETOF content_signature
    LANGUAGE plpgsql
    AS $$
begin
    return query
	select sha1, sha1_git, sha256, blake2s256 from tmp_skipped_content t
	where not exists
	(select 1 from skipped_content s where
	    s.sha1 is not distinct from t.sha1 and
	    s.sha1_git is not distinct from t.sha1_git and
	    s.sha256 is not distinct from t.sha256);
    return;
end
$$;

CREATE INDEX content_blake2s256_idx ON content USING btree (blake2s256);

CREATE INDEX content_sha256_idx ON content USING btree (sha256);

CREATE INDEX skipped_content_blake2s256_idx ON skipped_content USING btree (blake2s256);

CREATE INDEX skipped_content_sha256_idx ON skipped_content USING btree (sha256);
