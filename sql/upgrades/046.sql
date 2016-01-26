-- SWH DB schema upgrade
-- from_version: 45
-- to_version: 46
-- description: Rename revision to target and add target_type in tables release and occurrence_history

insert into dbversion(version, release, description)
      values(46, now(), 'Work In Progress');

ALTER TABLE occurrence_history
	DROP CONSTRAINT if exists occurrence_history_pkey;

ALTER TABLE occurrence
	DROP CONSTRAINT if exists occurrence_pkey;

ALTER TABLE occurrence_history
	DROP CONSTRAINT if exists occurrence_history_origin_branch_revision_authority_validi_excl;

DROP INDEX if exists occurrence_history_revision_idx;

DROP INDEX if exists release_revision_idx;

create type object_type as enum ('content', 'directory', 'revision', 'release');


ALTER TABLE occurrence_history
	RENAME COLUMN revision TO target;

ALTER TABLE occurrence_history
  ADD COLUMN target_type object_type NOT NULL DEFAULT 'revision';

ALTER TABLE occurrence_history
  ALTER COLUMN target_type DROP DEFAULT;

ALTER TABLE occurrence
  RENAME COLUMN revision TO target;

ALTER TABLE occurrence
  ADD COLUMN target_type object_type NOT NULL DEFAULT 'revision';

ALTER TABLE occurrence
  ALTER COLUMN target_type DROP DEFAULT;

ALTER TABLE "release"
  RENAME COLUMN revision TO target;

ALTER TABLE "release"
  ADD COLUMN target_type object_type NOT NULL DEFAULT 'revision';

ALTER TABLE "release"
  ALTER COLUMN target_type DROP DEFAULT;

drop type release_entry cascade;
create type release_entry as
(
  id          sha1_git,
  target      sha1_git,
  target_type object_type,
  date        timestamptz,
  date_offset smallint,
  name        text,
  comment     bytea,
  synthetic   boolean,
  author_name bytea,
  author_email bytea
);

drop type content_occurrence cascade;
create type content_occurrence as (
  origin_type	 text,
  origin_url	 text,
  branch	 text,
  target	 sha1_git,
  target_type	 object_type,
  path	 unix_path
);

CREATE OR REPLACE FUNCTION swh_content_find_occurrence(content_id sha1) RETURNS content_occurrence
    LANGUAGE plpgsql
    AS $$
declare
    dir content_dir;
    rev sha1_git;
    occ occurrence%ROWTYPE;
    coc content_occurrence;
begin
    -- each step could fail if no results are found, and that's OK
    select * from swh_content_find_directory(content_id)     -- look up directory
	into dir;
    if not found then return null; end if;

    select id from revision where directory = dir.directory  -- look up revision
	limit 1
	into rev;
    if not found then return null; end if;

    select * from swh_revision_find_occurrence(rev)	     -- look up occurrence
	into occ;
    if not found then return null; end if;

    select origin.type, origin.url, occ.branch, occ.target, occ.target_type, dir.path
    from origin
    where origin.id = occ.origin
    into coc;

    return coc;  -- might be NULL
end
$$;

CREATE OR REPLACE FUNCTION swh_occurrence_history_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    -- Update intervals we have the data to update
    with new_intervals as (
        select t.origin, t.branch, t.authority, t.validity,
	       o.validity - t.validity as new_validity
	from tmp_occurrence_history t
        left join occurrence_history o
        using (origin, branch, authority)
	where o.origin is not null),
    -- do not update intervals if they would become empty (perfect overlap)
    to_update as (
        select * from new_intervals
	where not isempty(new_validity))
    update occurrence_history o set validity = t.new_validity
    from to_update t
    where o.origin = t.origin and o.branch = t.branch and o.authority = t.authority;

    -- Now only insert intervals that aren't already present
    insert into occurrence_history (origin, branch, target, target_type, authority, validity)
	select distinct origin, branch, target, target_type, authority, validity
	from tmp_occurrence_history t
	where not exists (
	    select 1 from occurrence_history o
	    where o.origin = t.origin and o.branch = t.branch and
	          o.authority = t.authority and o.target = t.target and
            o.target_type = t.target_type and o.validity = t.validity);
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_release_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    perform swh_person_add_from_release();

    insert into release (id, target, target_type, date, date_offset, name, comment, author, synthetic)
    select t.id, t.target, t.target_type, t.date, t.date_offset, t.name, t.comment, a.id, t.synthetic
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
        select r.id, r.target, r.target_type, r.date, r.date_offset, r.name, r.comment,
               r.synthetic, p.name as author_name, p.email as author_email
        from tmp_release_get t
        inner join release r on t.id = r.id
        inner join person p on p.id = r.author;
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_release_get_by(origin_id bigint) RETURNS SETOF release_entry
    LANGUAGE sql STABLE
    AS $$
   select r.id, r.target, r.target_type, r.date, r.date_offset,
        r.name, r.comment, r.synthetic, a.name as author_name,
        a.email as author_email
    from release r
    inner join occurrence_history occ on occ.target = r.target
    left join person a on a.id = r.author
    where occ.origin = origin_id and occ.target_type = 'revision' and r.target_type = 'revision';
$$;

CREATE OR REPLACE FUNCTION swh_revision_find_occurrence(revision_id sha1_git) RETURNS occurrence
    LANGUAGE sql STABLE
    AS $$
	select origin, branch, target, target_type
  from swh_revision_list_children(ARRAY[revision_id] :: bytea[]) as rev_list
	left join occurrence_history occ_hist
  on rev_list.id = occ_hist.target
	where occ_hist.origin is not null and
        occ_hist.target_type = 'revision'
	order by upper(occ_hist.validity)  -- TODO filter by authority?
	limit 1;
$$;

CREATE OR REPLACE FUNCTION swh_revision_get_by(origin_id bigint, branch_name text = NULL::text, validity timestamp with time zone = NULL::timestamp with time zone) RETURNS SETOF revision_entry
    LANGUAGE sql STABLE
    AS $$
    select r.id, r.date, r.date_offset,
        r.committer_date, r.committer_date_offset,
        r.type, r.directory, r.message,
        a.name, a.email, c.name, c.email, r.metadata, r.synthetic,
        array(select rh.parent_id::bytea
            from revision_history rh
            where rh.id = r.id
            order by rh.parent_rank
        ) as parents
    from swh_occurrence_get_by(origin_id, branch_name, validity) as occ
    inner join revision r on occ.target = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;

ALTER TABLE occurrence_history
	ADD CONSTRAINT occurrence_history_pkey PRIMARY KEY (object_id);

ALTER TABLE occurrence
	ADD CONSTRAINT occurrence_pkey PRIMARY KEY (origin, branch);

CREATE INDEX occurrence_history_origin_branch_idx ON occurrence_history USING btree (origin, branch);

CREATE INDEX occurrence_history_target_target_type_idx ON occurrence_history USING btree (target, target_type);

CREATE INDEX release_target_target_type_idx ON "release" USING btree (target, target_type);
