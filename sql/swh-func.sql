
-- create a temporary table isomorphic to the content table
--
-- Args:
--     tbl: name of the temporary table to be created
create or replace function swh_content_mktemp(tbl text)
    returns void
    language plpgsql
as $$
begin
    execute format('
            create temporary table %I (
		sha1      sha1,
                sha1_git  sha1_git,
		sha256    sha256,
		length    bigint
	    )', tbl);
    return;
end
$$;


-- a content signature is a set of cryptographic checksums that we use to
-- uniquely identify content, for the purpose of verifying if we already have
-- some content or not during content injection
create type content_signature as (
    sha1      sha1,
    sha1_git  sha1_git,
    sha256    sha256
);


-- check which entries of a given table are missing from the content table
--
-- operates in bulk: entries should first be COPY-ed to a temporary table; then
-- the name of the temporary table should be passed to this function
--
-- See swh_content_mktemp()
--
-- Args:
--     tbl: name of the table containing the rows to be checked
create or replace function swh_content_missing(tbl regclass)
    returns setof content_signature
    language plpgsql
as $$
begin
    return query execute format('
            select sha1, sha1_git, sha256 from %I
	    except
	    select sha1, sha1_git, sha256 from content
	    ', tbl);
    return;
end
$$;


-- Add entries to the content table, reading from a given table. Skip entries
-- that are already present.
--
-- operates in bulk: entries should first be COPY-ed to a temporary table; then
-- the name of the temporary table should be passed to this function
--
-- See swh_content_mktemp()
--
-- Args:
--     tbl: name of the table containing the rows to be added
create or replace function swh_content_add(
        tbl regclass,
        status content_status default 'visible')
    returns void
    language plpgsql
as $$
declare
    rows bigint;
begin
    execute format('
            insert into content (sha1, sha1_git, sha256, length, status)
            select sha1, sha1_git, sha256, length, %L
            from %I
	    where (sha1, sha1_git, sha256) in
	        (select * from swh_content_missing(''%I''))
            ', status, tbl, tbl);
	    -- TODO XXX use postgres 9.5 "UPSERT" support here, when available.
	    -- Specifically, using "INSERT .. ON CONFLICT IGNORE" we can avoid
	    -- the extra swh_content_missing() query here.
    return;
end
$$;
