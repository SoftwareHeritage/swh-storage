-- SWH DB schema upgrade
-- from_version: 114
-- to_version: 115
-- description: Add snapshot models

insert into dbversion(version, release, description)
      values(115, now(), 'Work In Progress');

CREATE SEQUENCE snapshot_branch_object_id_seq
	START WITH 1
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 1;

CREATE SEQUENCE snapshot_object_id_seq
	START WITH 1
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 1;

CREATE TYPE snapshot_target AS ENUM (
	'content',
	'directory',
	'revision',
	'release',
	'snapshot',
	'alias'
);

CREATE TYPE snapshot_result AS (
	snapshot_id sha1_git,
	name bytea,
	target bytea,
	target_type snapshot_target
);

CREATE TABLE snapshot (
	object_id bigint DEFAULT nextval('snapshot_object_id_seq'::regclass) NOT NULL,
	id sha1_git
);

CREATE TABLE snapshot_branch (
	object_id bigint DEFAULT nextval('snapshot_branch_object_id_seq'::regclass) NOT NULL,
	name bytea NOT NULL,
	target bytea,
	target_type snapshot_target
);

CREATE TABLE snapshot_branches (
	snapshot_id bigint NOT NULL,
	branch_id bigint NOT NULL
);

ALTER TABLE occurrence_history
	ADD COLUMN snapshot_branch_id bigint;

ALTER TABLE origin_visit
	ADD COLUMN snapshot_id bigint;

COMMENT ON COLUMN origin_visit.snapshot_id IS 'id of the snapshot associated with the visit';

ALTER SEQUENCE snapshot_branch_object_id_seq
	OWNED BY snapshot_branch.object_id;

ALTER SEQUENCE snapshot_object_id_seq
	OWNED BY snapshot.object_id;

CREATE OR REPLACE FUNCTION swh_mktemp_snapshot_branch() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_snapshot_branch (
      name bytea not null,
      target bytea,
      target_type snapshot_target
  ) on commit drop;
$$;

CREATE OR REPLACE FUNCTION swh_snapshot_add(origin bigint, visit bigint, snapshot_id sha1_git) RETURNS void
    LANGUAGE plpgsql
    AS $$
declare
  snapshot_object_id snapshot.object_id%type;
begin
  select object_id from snapshot where id = snapshot_id into snapshot_object_id;
  if snapshot_object_id is null then
     insert into snapshot (id) values (snapshot_id) returning object_id into snapshot_object_id;
     with all_branches(name, target_type, target) as (
       select name, target_type, target from tmp_snapshot_branch
     ), inserted as (
       insert into snapshot_branch (name, target_type, target)
       select name, target_type, target from all_branches
       on conflict do nothing
       returning object_id
     )
     insert into snapshot_branches (snapshot_id, branch_id)
     select snapshot_object_id, object_id as branch_id from inserted
     union all
     select snapshot_object_id, object_id as branch_id
       from all_branches ab
       join snapshot_branch sb
         on sb.name = ab.name
           and sb.target_type is not distinct from ab.target_type
           and sb.target is not distinct from ab.target;
  end if;
  update origin_visit ov
    set snapshot_id = snapshot_object_id
    where ov.origin=swh_snapshot_add.origin and ov.visit=swh_snapshot_add.visit;
end;
$$;

CREATE OR REPLACE FUNCTION swh_snapshot_get_by_id(id sha1_git) RETURNS SETOF snapshot_result
    LANGUAGE sql STABLE
    AS $$
  select
    swh_snapshot_get_by_id.id as snapshot_id, name, target, target_type
  from snapshot_branches
  inner join snapshot_branch on snapshot_branches.branch_id = snapshot_branch.object_id
  where snapshot_id = (select object_id from snapshot where snapshot.id = swh_snapshot_get_by_id.id)
$$;

CREATE OR REPLACE FUNCTION swh_snapshot_get_by_origin_visit(origin_id bigint, visit_id bigint) RETURNS sha1_git
    LANGUAGE sql STABLE
    AS $$
  select snapshot.id
  from origin_visit
  left join snapshot
  on snapshot.object_id = origin_visit.snapshot_id
  where origin_visit.origin=origin_id and origin_visit.visit=visit_id;
$$;

ALTER TABLE snapshot
	ADD CONSTRAINT snapshot_pkey PRIMARY KEY (object_id);

ALTER TABLE snapshot_branch
	ADD CONSTRAINT snapshot_branch_pkey PRIMARY KEY (object_id);

ALTER TABLE snapshot_branches
	ADD CONSTRAINT snapshot_branches_pkey PRIMARY KEY (snapshot_id, branch_id);

ALTER TABLE origin_visit
	ADD CONSTRAINT origin_visit_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES snapshot(object_id);

ALTER TABLE snapshot_branch
	ADD CONSTRAINT snapshot_branch_target_check CHECK (((target_type IS NULL) = (target IS NULL)));

ALTER TABLE snapshot_branch
	ADD CONSTRAINT snapshot_target_check CHECK (((target_type <> ALL (ARRAY['content'::snapshot_target, 'directory'::snapshot_target, 'revision'::snapshot_target, 'release'::snapshot_target, 'snapshot'::snapshot_target])) OR (length(target) = 20)));

ALTER TABLE snapshot_branches
	ADD CONSTRAINT snapshot_branches_branch_id_fkey FOREIGN KEY (branch_id) REFERENCES snapshot_branch(object_id);

ALTER TABLE snapshot_branches
	ADD CONSTRAINT snapshot_branches_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES snapshot(object_id);

CREATE UNIQUE INDEX snapshot_id_idx ON snapshot USING btree (id);

CREATE UNIQUE INDEX snapshot_branch_name_idx ON snapshot_branch USING btree (name) WHERE ((target_type IS NULL) AND (target IS NULL));

CREATE UNIQUE INDEX snapshot_branch_target_type_target_name_idx ON snapshot_branch USING btree (target_type, target, name);
