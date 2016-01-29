-- SWH DB schema upgrade
-- from_version: 47
-- to_version: 48
-- description: Remove object_id from temporary tables

insert into dbversion(version, release, description)
      values(48, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_mktemp(tblname regclass) RETURNS void
    LANGUAGE plpgsql
    AS $_$
begin
    execute format('
	create temporary table tmp_%1$I
	    (like %1$I including defaults)
	    on commit drop;
      alter table tmp_%1$I drop column if exists object_id;
	', tblname);
    return;
end
$_$;

CREATE OR REPLACE FUNCTION swh_mktemp_dir_entry(tblname regclass) RETURNS void
    LANGUAGE plpgsql
    AS $_$
begin
    execute format('
	create temporary table tmp_%1$I
	    (like %1$I including defaults, dir_id sha1_git)
	    on commit drop;
        alter table tmp_%1$I drop column id;
	', tblname);
    return;
end
$_$;

CREATE OR REPLACE FUNCTION swh_mktemp_release() RETURNS void
    LANGUAGE sql
    AS $$
    create temporary table tmp_release (
        like release including defaults,
        author_name bytea not null default '',
        author_email bytea not null default ''
    ) on commit drop;
    alter table tmp_release drop column author;
    alter table tmp_release drop column object_id;
$$;

CREATE OR REPLACE FUNCTION swh_mktemp_revision() RETURNS void
    LANGUAGE sql
    AS $$
    create temporary table tmp_revision (
        like revision including defaults,
        author_name bytea not null default '',
        author_email bytea not null default '',
        committer_name bytea not null default '',
        committer_email bytea not null default ''
    ) on commit drop;
    alter table tmp_revision drop column author;
    alter table tmp_revision drop column committer;
    alter table tmp_revision drop column object_id;
$$;
