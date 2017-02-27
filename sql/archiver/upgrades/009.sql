-- SWH Archiver DB schema upgrade
-- from_version: 8
-- to_version: 9
-- description: Add helper functions to create temporary table and insert new entries in content_archive table

insert into dbversion(version, release, description)
values(9, now(), 'Work In Progress');

-- create a temporary table called tmp_TBLNAME, mimicking existing
-- table TBLNAME
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
