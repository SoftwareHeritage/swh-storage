-- SWH DB schema upgrade
-- from_version: 21
-- to_version: 22
-- description: Replace name and email in person with bytea

insert into dbversion(version, release, description)
      values(22, now(), 'Work In Progress');

ALTER TABLE person
        ALTER COLUMN name DROP DEFAULT,
        ALTER COLUMN name TYPE bytea using convert_to(name, 'utf-8'),
        ALTER COLUMN name SET DEFAULT '\x'::bytea,
        ALTER COLUMN email DROP DEFAULT,
        ALTER COLUMN email TYPE bytea using convert_to(email, 'utf-8'),
        ALTER COLUMN email SET DEFAULT '\x'::bytea;

alter type revision_log_entry
 alter attribute author_name type bytea,
 alter attribute author_email type bytea,
 alter attribute committer_name type bytea,
 alter attribute committer_email type bytea;


CREATE OR REPLACE FUNCTION swh_mktemp_release() RETURNS void
    LANGUAGE sql
    AS $$
    create temporary table tmp_release (
        like release including defaults,
        author_name bytea not null default '',
        author_email bytea not null default ''
    ) on commit drop;
    alter table tmp_release drop column author;
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
$$;
