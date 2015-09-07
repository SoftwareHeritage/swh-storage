
-- create the temporary table tmp_content, with all file metadata fields of the
-- content table (i.e., all checksums + length)
--
-- Args:
--     tbl: name of the temporary table to be created
create or replace function swh_content_mktemp()
    returns void
    language plpgsql
as $$
begin
    create temporary table tmp_content (
	sha1      sha1,
	sha1_git  sha1_git,
	sha256    sha256,
	length    bigint)
	on commit drop;
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


-- check which entries of tmp_content are missing from content
--
-- operates in bulk: 0. swh_content_mktemp(), 1. COPY to tmp_content,
-- 2. call this function
create or replace function swh_content_missing()
    returns setof content_signature
    language plpgsql
as $$
begin
    return query
	select sha1, sha1_git, sha256 from tmp_content
	except
	select sha1, sha1_git, sha256 from content;
    return;
end
$$;


-- add tmp_content entries to content, skipping duplicates
--
-- operates in bulk: 0. swh_content_mktemp(), 1. COPY to tmp_content,
-- 2. call this function
create or replace function swh_content_add(
        status content_status default 'visible')
    returns void
    language plpgsql
as $$
declare
    rows bigint;
begin
    insert into content (sha1, sha1_git, sha256, length, status)
	select distinct sha1, sha1_git, sha256, length, status
	from tmp_content
	where (sha1, sha1_git, sha256) in
	    (select * from swh_content_missing());
	    -- TODO XXX use postgres 9.5 "UPSERT" support here, when available.
	    -- Specifically, using "INSERT .. ON CONFLICT IGNORE" we can avoid
	    -- the extra swh_content_missing() query here.
    return;
end
$$;
