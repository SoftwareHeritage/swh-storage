-- SWH DB schema upgrade
-- from_version: 4
-- to_version: 5
-- description: List unknown sha1s from content_archive

INSERT INTO dbversion(version, release, description)
VALUES(5, now(), 'Work In Progress');

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

COMMENT ON FUNCTION swh_content_archive_unknown() IS 'Retrieve list of unknown sha1';
