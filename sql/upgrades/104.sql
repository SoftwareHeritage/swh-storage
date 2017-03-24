-- SWH DB schema upgrade
-- from_version: 103
-- to_version: 104
-- description: Compute new hash blake2s256 for contents

insert into dbversion(version, release, description)
      values(104, now(), 'Work In Progress');

-- a blake2 checksum
create domain blake2s256 as bytea check (length(value) = 32);

alter table content add column blake2s256 blake2s256;

create unique index concurrently on content(blake2s256);

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

alter table skipped_content add column blake2s256 blake2s256;

create unique index concurrently on skipped_content(blake2s256);

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

create or replace function swh_content_add()
    returns void
    language plpgsql
as $$
begin
    insert into content (sha1, sha1_git, sha256, blake2s256, length, status)
        select distinct sha1, sha1_git, sha256, blake2s256, length, status
	from tmp_content
	where (sha1, sha1_git, sha256, blake2s256) in
	    (select * from swh_content_missing());
	    -- TODO XXX use postgres 9.5 "UPSERT" support here, when available.
	    -- Specifically, using "INSERT .. ON CONFLICT IGNORE" we can avoid
	    -- the extra swh_content_missing() query here.
    return;
end
$$;

create or replace function swh_skipped_content_add()
    returns void
    language plpgsql
as $$
begin
    insert into skipped_content (sha1, sha1_git, sha256, blake2s256, length, status, reason, origin)
        select distinct sha1, sha1_git, sha256, blake2s256, length, status, reason, origin
	from tmp_skipped_content
	where (coalesce(sha1, ''), coalesce(sha1_git, ''), coalesce(sha256, ''), coalesce(blake2s256)) in
	    (select coalesce(sha1, ''), coalesce(sha1_git, ''), coalesce(sha256, ''), coalesce(blake2s256, '') from swh_skipped_content_missing());
	    -- TODO XXX use postgres 9.5 "UPSERT" support here, when available.
	    -- Specifically, using "INSERT .. ON CONFLICT IGNORE" we can avoid
	    -- the extra swh_content_missing() query here.
    return;
end
$$;

drop type content_signature cascade;

create type content_signature as (
    sha1       sha1,
    sha1_git   sha1_git,
    sha256     sha256,
    blake2s256 blake2s256
);

create or replace function swh_content_missing()
    returns setof content_signature
    language plpgsql
as $$
begin
    return query (
      select sha1, sha1_git, sha256, blake2s256 from tmp_content as tmp
      where not exists (
        select 1 from content as c
        where c.sha1 = tmp.sha1 and
              c.sha1_git = tmp.sha1_git and
              c.sha256 = tmp.sha256 and
              c.blake2s256 = tmp.blake2s256
      )
    );
    return;
end
$$;

create or replace function swh_skipped_content_missing()
    returns setof content_signature
    language plpgsql
as $$
begin
    return query
	select sha1, sha1_git, sha256 from tmp_skipped_content t
	where not exists
	(select 1 from skipped_content s where
	    s.sha1 is not distinct from t.sha1 and
	    s.sha1_git is not distinct from t.sha1_git and
	    s.sha256 is not distinct from t.sha256 and
            s.blake2s256 is not distinct from t.blake2s256);
    return;
end
$$;

create or replace function swh_content_find(
    sha1       sha1       default NULL,
    sha1_git   sha1_git   default NULL,
    sha256     sha256     default NULL,
    blake2s256 blake2s256 default NULL
)
    returns content
    language plpgsql
as $$
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

create or replace function swh_skipped_content_add()
    returns void
    language plpgsql
as $$
begin
    insert into skipped_content (sha1, sha1_git, sha256, blake2s256, length, status, reason, origin)
        select distinct sha1, sha1_git, sha256, blake2s256, length, status, reason, origin
	from tmp_skipped_content
	where (coalesce(sha1, ''), coalesce(sha1_git, ''), coalesce(sha256, ''), coalesce(blake2s256, '')) in
	    (select coalesce(sha1, ''),
                    coalesce(sha1_git, ''),
                    coalesce(sha256, ''),
                    coalesce(blake2s256, '')
                    from swh_skipped_content_missing());
	    -- TODO XXX use postgres 9.5 "UPSERT" support here, when available.
	    -- Specifically, using "INSERT .. ON CONFLICT IGNORE" we can avoid
	    -- the extra swh_content_missing() query here.
    return;
end
$$;
