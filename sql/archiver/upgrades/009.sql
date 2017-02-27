-- SWH DB schema upgrade
-- from_version: 8
-- to_version: 9
-- description: Add helper function to create new entries in content_archive table

INSERT INTO dbversion(version, release, description)
VALUES(9, now(), 'Work In Progress');

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
