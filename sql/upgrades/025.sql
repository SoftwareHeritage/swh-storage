-- SWH DB schema upgrade
-- from_version: 24
-- to_version: 25
-- description: Add synthetic flag to release and revision. Update gnu
-- project to appease apgdiff.

insert into dbversion(version, release, description)
      values(25, now(), 'Work In Progress');

ALTER TABLE "release"
	ADD COLUMN synthetic boolean DEFAULT false NOT NULL;

ALTER TABLE revision
	ADD COLUMN synthetic boolean DEFAULT false NOT NULL;

ALTER TYPE revision_log_entry
        ADD ATTRIBUTE synthetic boolean;

ALTER TYPE revision_entry
        DROP ATTRIBUTE parents,
        ADD ATTRIBUTE synthetic boolean,
	ADD ATTRIBUTE parents bytea[];

UPDATE organization SET description='GNU is not Unix!' where name='gnu';

CREATE OR REPLACE FUNCTION swh_release_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    perform swh_person_add_from_release();

    insert into release (id, revision, date, date_offset, name, comment, author, synthetic)
    select t.id, t.revision, t.date, t.date_offset, t.name, t.comment, a.id, t.synthetic
    from tmp_release t
    left join person a on a.name = t.author_name and a.email = t.author_email;
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_revision_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    perform swh_person_add_from_revision();

    insert into revision (id, date, date_offset, committer_date, committer_date_offset, type, directory, message, author, committer, synthetic)
    select t.id, t.date, t.date_offset, t.committer_date, t.committer_date_offset, t.type, t.directory, t.message, a.id, c.id, t.synthetic
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
        select t.id, r.date, r.date_offset,
               r.committer_date, r.committer_date_offset,
               r.type, r.directory, r.message,
               a.name, a.email, c.name, c.email, r.synthetic,
	       array_agg(rh.parent_id::bytea order by rh.parent_rank)
                   as parents
        from tmp_revision t
        left join revision r on t.id = r.id
        left join person a on a.id = r.author
        left join person c on c.id = r.committer
        left join revision_history rh on rh.id = r.id
        group by t.id, a.name, a.email, r.date, r.date_offset,
               c.name, c.email, r.committer_date, r.committer_date_offset,
               r.type, r.directory, r.message, r.synthetic;
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_revision_log(root_revision sha1_git) RETURNS SETOF revision_log_entry
    LANGUAGE sql STABLE
    AS $$
    select revision.id, date, date_offset,
	committer_date, committer_date_offset,
	type, directory, message,
	author.name as author_name, author.email as author_email,
	committer.name as committer_name, committer.email as committer_email,
        revision.synthetic
    from swh_revision_list(root_revision) as rev_list
    join revision on revision.id = rev_list
    join person as author on revision.author = author.id
    join person as committer on revision.committer = committer.id;
$$;
