-- SWH DB schema upgrade
-- from_version: 145
-- to_version: 146
-- description: Improve query on origin_visit

-- latest schema version
insert into dbversion(version, release, description)
      values(146, now(), 'Work In Progress');

-- create a temporary table called tmp_TBLNAME, mimicking existing table
-- TBLNAME
--
-- Args:
--     tblname: name of the table to mimic
create or replace function swh_mktemp(tblname regclass)
    returns void
    language plpgsql
as $$
begin
    execute format('
	create temporary table if not exists tmp_%1$I
	    (like %1$I including defaults)
	    on commit delete rows;
      alter table tmp_%1$I drop column if exists object_id;
	', tblname);
    return;
end
$$;

-- create a temporary table for directory entries called tmp_TBLNAME,
-- mimicking existing table TBLNAME with an extra dir_id (sha1_git)
-- column, and dropping the id column.
--
-- This is used to create the tmp_directory_entry_<foo> tables.
--
-- Args:
--     tblname: name of the table to mimic
create or replace function swh_mktemp_dir_entry(tblname regclass)
    returns void
    language plpgsql
as $$
begin
    execute format('
	create temporary table if not exists tmp_%1$I
	    (like %1$I including defaults, dir_id sha1_git)
	    on commit delete rows;
        alter table tmp_%1$I drop column if exists id;
	', tblname);
    return;
end
$$;

-- create a temporary table for revisions called tmp_revisions,
-- mimicking existing table revision, replacing the foreign keys to
-- people with an email and name field
--
create or replace function swh_mktemp_revision()
    returns void
    language sql
as $$
    create temporary table if not exists tmp_revision (
        like revision including defaults,
        author_fullname bytea,
        author_name bytea,
        author_email bytea,
        committer_fullname bytea,
        committer_name bytea,
        committer_email bytea
    ) on commit delete rows;
    alter table tmp_revision drop column if exists author;
    alter table tmp_revision drop column if exists committer;
    alter table tmp_revision drop column if exists object_id;
$$;

-- create a temporary table for releases called tmp_release,
-- mimicking existing table release, replacing the foreign keys to
-- people with an email and name field
--
create or replace function swh_mktemp_release()
    returns void
    language sql
as $$
    create temporary table if not exists tmp_release (
        like release including defaults,
        author_fullname bytea,
        author_name bytea,
        author_email bytea
    ) on commit delete rows;
    alter table tmp_release drop column if exists author;
    alter table tmp_release drop column if exists object_id;
$$;

-- create a temporary table for the branches of a snapshot
create or replace function swh_mktemp_snapshot_branch()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_snapshot_branch (
      name bytea not null,
      target bytea,
      target_type snapshot_target
  ) on commit delete rows;
$$;

-- create a temporary table for the tools
create or replace function swh_mktemp_tool()
    returns void
    language sql
as $$
    create temporary table if not exists tmp_tool (
      like tool including defaults
    ) on commit delete rows;
    alter table tmp_tool drop column if exists id;
$$;
