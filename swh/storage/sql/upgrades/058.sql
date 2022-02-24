-- SWH DB schema upgrade
-- from_version: 57
-- to_version: 58
-- description: Return missing contents from batch of contents uniquely based on sha1.

insert into dbversion(version, release, description)
      values(58, now(), 'Work In Progress');

-- create a temporary table called tmp_content_sha1, mimicking existing table
-- content with only the sha1 column
--
create or replace function swh_mktemp_content_sha1()
    returns void
    language sql
as $$
    create temporary table tmp_content_sha1
     (like content including defaults)
     on commit drop;
    alter table tmp_content_sha1 drop column if exists sha256;
    alter table tmp_content_sha1 drop column if exists sha1_git;
    alter table tmp_content_sha1 drop column if exists ctime;
    alter table tmp_content_sha1 drop column if exists length;
    alter table tmp_content_sha1 drop column if exists status;
    alter table tmp_content_sha1 drop column if exists object_id;
$$;



-- check which entries of tmp_content_sha1 are missing from content
--
-- operates in bulk: 0. swh_mktemp_content_sha1(), 1. COPY to tmp_content_sha1,
-- 2. call this function
create or replace function swh_content_missing_per_sha1()
    returns setof sha1
    language plpgsql
as $$
begin
    return query
           (select sha1
            from tmp_content_sha1 as tmp
            where not exists
            (select 1 from content as c where c.sha1=tmp.sha1));
end
$$;
