create or replace function swh_mktemp_content_archive()
    returns void
    language sql
as $$
    create temporary table tmp_content_archive (
        like content_archive including defaults
    ) on commit drop;
    alter table tmp_content_archive drop column copies;
    alter table tmp_content_archive drop column num_present;
$$;

COMMENT ON FUNCTION swh_mktemp_content_archive() IS 'Create temporary table content_archive';

create or replace function swh_content_archive_missing(backend_name text)
    returns setof sha1
    language plpgsql
as $$
begin
    return query
        select content_id
        from tmp_content_archive tmp where exists (
            select 1
            from content_archive c
            where tmp.content_id = c.content_id
                and (not c.copies ? backend_name
                     or c.copies @> jsonb_build_object(backend_name, '{"status": "missing"}'::jsonb))
        );
end
$$;

COMMENT ON FUNCTION swh_content_archive_missing(text) IS 'Filter missing data from a specific backend';

create or replace function swh_content_archive_unknown()
    returns setof sha1
    language plpgsql
as $$
begin
    return query
        select content_id
        from tmp_content_archive tmp where not exists (
            select 1
            from content_archive c
            where tmp.content_id = c.content_id
        );
end
$$;

COMMENT ON FUNCTION swh_content_archive_unknown() IS 'Retrieve list of unknown sha1s';

CREATE OR REPLACE FUNCTION count_copies(from_id bytea, to_id bytea) returns void language sql as $$
    with sample as (
        select content_id, copies from content_archive
        where content_id > from_id and content_id <= to_id
    ), data as (
        select substring(content_id from 19) as bucket, jbe.key as archive
        from sample
        join lateral jsonb_each(copies) jbe on true
        where jbe.value->>'status' = 'present'
    ), bucketed as (
        select bucket, archive, count(*) as count
        from data
        group by bucket, archive
    ) update content_archive_counts cac set
        count = cac.count + bucketed.count
      from bucketed
      where cac.archive = bucketed.archive and cac.bucket = bucketed.bucket;
$$;

comment on function count_copies(bytea, bytea) is 'Count the objects between from_id and to_id, add the results to content_archive_counts';

CREATE OR REPLACE FUNCTION init_content_archive_counts() returns void language sql as $$
    insert into content_archive_counts (
        select id, decode(lpad(to_hex(bucket), 4, '0'), 'hex')::bucket as bucket, 0 as count
        from archive join lateral generate_series(0, 65535) bucket on true
    ) on conflict (archive, bucket) do nothing;
$$;

comment on function init_content_archive_counts() is 'Initialize the content archive counts for the registered archives';

create type content_archive_count as (
  archive text,
  count bigint
);

create or replace function get_content_archive_counts() returns setof content_archive_count language sql as $$
    select archive, sum(count)::bigint
    from content_archive_counts
    group by archive
    order by archive;
$$;

comment on function get_content_archive_counts() is 'Get count for each archive';

-- create a temporary table called tmp_TBLNAME, mimicking existing table
-- TBLNAME
create or replace function swh_mktemp(tblname regclass)
    returns void
    language plpgsql
as $$
begin
    execute format('
	create temporary table tmp_%1$I
	    (like %1$I including defaults)
	    on commit drop;
	', tblname);
    return;
end
$$;

comment on function swh_mktemp(regclass) is 'Helper function to create a temporary table mimicking the existing one';

-- Helper function to insert new entries in content_archive from a
-- temporary table skipping duplicates.
create or replace function swh_content_archive_add()
    returns void
    language plpgsql
as $$
begin
    insert into content_archive (content_id, copies, num_present)
	select distinct content_id, copies, num_present
	from tmp_content_archive
        on conflict(content_id) do nothing;
    return;
end
$$;

comment on function swh_content_archive_add() is 'Helper function to insert new entry in content_archive';
