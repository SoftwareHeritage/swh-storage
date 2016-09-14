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
