-- SWH DB schema upgrade
-- from_version: 6
-- to_version: 7
-- description: Add a function to compute archive counts

INSERT INTO dbversion(version, release, description)
VALUES(7, now(), 'Work In Progress');

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
