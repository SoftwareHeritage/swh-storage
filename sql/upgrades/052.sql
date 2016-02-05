-- SWH DB schema upgrade
-- from_version: 51
-- to_version: 52
-- description: Add support for negative utc offsets in dates

insert into dbversion(version, release, description)
      values(52, now(), 'Work In Progress');

ALTER TABLE "release"
	ADD COLUMN date_neg_utc_offset boolean;

ALTER TABLE revision
	ADD COLUMN date_neg_utc_offset boolean,
	ADD COLUMN committer_date_neg_utc_offset boolean;

drop type revision_entry cascade;
create type revision_entry as
(
id                     sha1_git,
date                   timestamptz,
date_offset            smallint,
date_neg_utc_offset    boolean,
committer_date         timestamptz,
committer_date_offset  smallint,
committer_date_neg_utc_offset    boolean,
type                   revision_type,
directory              sha1_git,
message                bytea,
author_id              bigint,
author_name            bytea,
author_email           bytea,
committer_id           bigint,
committer_name         bytea,
committer_email        bytea,
metadata               jsonb,
synthetic              boolean,
parents                bytea[]
);

drop type release_entry cascade;
create type release_entry as
(
id           sha1_git,
target       sha1_git,
target_type  object_type,
date         timestamptz,
date_offset  smallint,
date_neg_utc_offset boolean,
name         bytea,
comment      bytea,
synthetic    boolean,
author_id    bigint,
author_name  bytea,
author_email bytea
);



CREATE OR REPLACE FUNCTION swh_release_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    perform swh_person_add_from_release();

    insert into release (id, target, target_type, date, date_offset, date_neg_utc_offset, name, comment, author, synthetic)
    select t.id, t.target, t.target_type, t.date, t.date_offset, t.date_neg_utc_offset, t.name, t.comment, a.id, t.synthetic
    from tmp_release t
    left join person a on a.name = t.author_name and a.email = t.author_email;
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_release_get() RETURNS SETOF release_entry
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select r.id, r.target, r.target_type, r.date, r.date_offset, r.date_neg_utc_offset, r.name, r.comment,
               r.synthetic, p.id as author_id, p.name as author_name, p.email as author_email
        from tmp_release_get t
        inner join release r on t.id = r.id
        inner join person p on p.id = r.author;
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_release_get_by(origin_id bigint) RETURNS SETOF release_entry
    LANGUAGE sql STABLE
    AS $$
   select r.id, r.target, r.target_type, r.date, r.date_offset, r.date_neg_utc_offset,
        r.name, r.comment, r.synthetic, a.id as author_id,
        a.name as author_name, a.email as author_email
    from release r
    inner join occurrence_history occ on occ.target = r.target
    left join person a on a.id = r.author
    where occ.origin = origin_id and occ.target_type = 'revision' and r.target_type = 'revision';
$$;

CREATE OR REPLACE FUNCTION swh_revision_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    perform swh_person_add_from_revision();

    insert into revision (id, date, date_offset, date_neg_utc_offset, committer_date, committer_date_offset, committer_date_neg_utc_offset, type, directory, message, author, committer, metadata, synthetic)
    select t.id, t.date, t.date_offset, t.date_neg_utc_offset, t.committer_date, t.committer_date_offset, t.committer_date_neg_utc_offset, t.type, t.directory, t.message, a.id, c.id, t.metadata, t.synthetic
    from tmp_revision t
    left join person a on a.name = t.author_name and a.email = t.author_email
    left join person c on c.name = t.committer_name and c.email = t.committer_email;
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_revision_get() RETURNS SETOF revision_entry
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select t.id, r.date, r.date_offset, r.date_neg_utc_offset,
               r.committer_date, r.committer_date_offset, r.committer_date_neg_utc_offset,
               r.type, r.directory, r.message,
               a.id, a.name, a.email, c.id, c.name, c.email, r.metadata, r.synthetic,
         array(select rh.parent_id::bytea from revision_history rh where rh.id = t.id order by rh.parent_rank)
                   as parents
        from tmp_revision t
        left join revision r on t.id = r.id
        left join person a on a.id = r.author
        left join person c on c.id = r.committer;
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_revision_get_by(origin_id bigint, branch_name bytea = NULL::bytea, "date" timestamp with time zone = NULL::timestamp with time zone) RETURNS SETOF revision_entry
    LANGUAGE sql STABLE
    AS $$
    select r.id, r.date, r.date_offset, r.date_neg_utc_offset,
        r.committer_date, r.committer_date_offset, r.committer_date_neg_utc_offset,
        r.type, r.directory, r.message,
        a.id, a.name, a.email, c.id, c.name, c.email, r.metadata, r.synthetic,
        array(select rh.parent_id::bytea
            from revision_history rh
            where rh.id = r.id
            order by rh.parent_rank
        ) as parents
    from swh_occurrence_get_by(origin_id, branch_name, date) as occ
    inner join revision r on occ.target = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;

CREATE OR REPLACE FUNCTION swh_revision_log(root_revisions bytea[], num_revs bigint = NULL::bigint) RETURNS SETOF revision_entry
    LANGUAGE sql STABLE
    AS $$
    select t.id, r.date, r.date_offset, r.date_neg_utc_offset,
           r.committer_date, r.committer_date_offset, r.committer_date_neg_utc_offset,
           r.type, r.directory, r.message,
           a.id, a.name, a.email,
           c.id, c.name, c.email,
           r.metadata, r.synthetic, t.parents
    from swh_revision_list(root_revisions, num_revs) as t
    left join revision r on t.id = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;
