-- SWH DB schema upgrade
-- from_version: 48
-- to_version: 49
-- description: update the schema for occurrence and occurrence_history

insert into dbversion(version, release, description)
      values(49, now(), 'Work In Progress');

CREATE TABLE origin_visit (
  origin bigint NOT NULL,
  visit bigint NOT NULL,
  "date" timestamp with time zone NOT NULL
);

-- move occurrence_history to another table
alter table occurrence_history rename to old_occurrence_history;
alter index occurrence_history_pkey rename to old_occurrence_history_pkey;
alter index occurrence_history_origin_branch_idx rename to old_occurrence_history_origin_branch_idx;
alter index occurrence_history_target_target_type_idx rename to old_occurrence_history_target_target_type_idx;
alter table old_occurrence_history
  rename constraint occurrence_history_authority_fkey to old_occurrence_history_authority_fkey;
alter table old_occurrence_history
  rename constraint occurrence_history_origin_fkey to old_occurrence_history_origin_fkey;

create table occurrence_history
(
  origin       bigint,
  branch       bytea,        -- e.g., b"master" (for VCS), or b"sid" (for Debian)
  target       sha1_git,     -- ref target, e.g., commit id
  target_type  object_type,  -- ref target type
  visits       bigint[],
  object_id    bigserial     -- short object identifier
);

-- create origin_visit contents
with origins_visited as (
  select distinct origin, lower(validity) as date
  from old_occurrence_history
  where authority = '5f4d4c51-498a-4e28-88b3-b3e4e8396cba' -- swh
  order by origin, date
)
  insert into origin_visit (origin, date, visit)
  select origin, date, row_number() over (partition by origin)
  from origins_visited;

ALTER TABLE origin_visit
  ADD CONSTRAINT origin_visit_pkey PRIMARY KEY (origin, visit);

ALTER TABLE origin_visit
  ADD CONSTRAINT origin_visit_origin_fkey FOREIGN KEY (origin) REFERENCES origin(id);

CREATE INDEX origin_visit_date_idx ON origin_visit USING btree (date);

-- create new occurrence_history contents
insert into occurrence_history (origin, branch, target, target_type, object_id, visits)
  select ooh.origin, branch, target, target_type, object_id, array[visit]
  from old_occurrence_history ooh
  left join origin_visit ov on ov.origin = ooh.origin and ov.date = lower(ooh.validity)
  where ov.visit is not null;

ALTER TABLE occurrence_history
  ADD CONSTRAINT occurrence_history_pkey PRIMARY KEY (object_id),
  ADD CONSTRAINT occurrence_history_origin_fkey FOREIGN KEY (origin) REFERENCES origin(id);

CREATE INDEX on occurrence_history(target, target_type);
CREATE INDEX on occurrence_history(origin, branch);

-- drop table old_occurrence_history;

-- create new occurrence contents
alter table occurrence
  drop constraint occurrence_pkey,
  drop constraint occurrence_origin_fkey;

drop index if exists occurrence_target_target_type_idx;


create or replace function update_occurrence_for_origin(origin_id bigint) returns void language sql as $$
  delete from occurrence where origin = origin_id;
  insert into occurrence (origin, branch, target, target_type)
    select origin, branch, target, target_type from occurrence_history
    where origin = origin_id
      and (select visit from origin_visit
           where origin = origin_id
           order by date desc
           limit 1) = any(visits);
$$;

create or replace function update_occurrence() returns void
  language plpgsql as
$$
  declare
    origin_id origin.id%type;
  begin
    for origin_id in
      select distinct id from origin
    loop
      perform update_occurrence_for_origin(origin_id);
    end loop;
    return;
  end;
$$;

select update_occurrence();

ALTER TABLE occurrence
  ADD CONSTRAINT occurrence_pkey PRIMARY KEY (origin, branch),
  ADD CONSTRAINT occurrence_origin_fkey FOREIGN KEY (origin) REFERENCES origin(id);

CREATE INDEX occurrence_target_target_type_idx on occurrence(target, target_type);


CREATE OR REPLACE FUNCTION swh_mktemp_occurrence_history() RETURNS void
    LANGUAGE sql
    AS $$
    create temporary table tmp_occurrence_history(
        like occurrence_history including defaults,
        date timestamptz not null
    ) on commit drop;
    alter table tmp_occurrence_history
      drop column visits,
      drop column object_id;
$$;

DROP FUNCTION swh_occurrence_get_by(bigint,bytea,timestamp with time zone);
CREATE OR REPLACE FUNCTION swh_occurrence_get_by(origin_id bigint, branch_name bytea = NULL::bytea, "date" timestamp with time zone = NULL::timestamp with time zone) RETURNS SETOF occurrence_history
    LANGUAGE plpgsql
    AS $$
declare
    filters text[] := array[] :: text[];  -- AND-clauses used to filter content
    visit_id bigint;
    q text;
begin
    if origin_id is not null then
        filters := filters || format('origin = %L', origin_id);
    end if;
    if branch_name is not null then
        filters := filters || format('branch = %L', branch_name);
    end if;
    if date is not null then
        if origin_id is null then
            raise exception 'Needs an origin_id to filter by date.';
        end if;
        select visit from swh_visit_find_by_date(origin_id, date) into visit_id;
        if visit_id is null then
            return;
        end if;
        filters := filters || format('%L = any(visits)', visit_id);
    end if;

    if cardinality(filters) = 0 then
        raise exception 'At least one filter amongst (origin_id, branch_name, validity) is needed';
    else
        q = format('select * ' ||
                   'from occurrence_history ' ||
                   'where %s',
	        array_to_string(filters, ' and '));
        return query execute q;
    end if;
end
$$;

CREATE OR REPLACE FUNCTION swh_occurrence_history_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
declare
  origin_id origin.id%type;
begin
  -- Create new visits
  with current_visits as (
    select distinct origin, date from tmp_occurrence_history
  ),
  new_visits as (
      select origin, date, (select coalesce(max(visit), 0)
                            from origin_visit ov
                            where ov.origin = origin) +
                            row_number()
                            over(partition by origin
                                           order by origin, date)
        from current_visits cv
        where not exists (select 1 from origin_visit ov
                          where ov.origin = cv.origin and
                                ov.date = cv.date)
  )
  insert into origin_visit (origin, date, visit)
    select * from new_visits;

  -- Create or update occurrence_history
  with occurrence_history_id_visit as (
    select tmp_occurrence_history.*, object_id, visits, visit from tmp_occurrence_history
    left join occurrence_history using(origin, target, target_type)
    left join origin_visit using(origin, date)
  ),
  occurrences_to_update as (
    select object_id, visit from occurrence_history_id_visit where object_id is not null
  ),
  update_occurrences as (
    update occurrence_history
    set visits = array(select unnest(occurrence_history.visits) as e
                        union
                       select occurrences_to_update.visit as e
                       order by e)
    from occurrences_to_update
    where occurrence_history.object_id = occurrences_to_update.object_id
  )
  insert into occurrence_history (origin, branch, target, target_type, visits)
    select origin, branch, target, target_type, ARRAY[visit]
      from occurrence_history_id_visit
      where object_id is null;

  -- update occurrence
  for origin_id in
    select distinct origin from tmp_occurrence_history
  loop
    perform update_occurrence_for_origin(origin_id);
  end loop;

  return;
end
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
	limit 1;
$$;

DROP FUNCTION swh_revision_get_by(bigint,bytea,timestamp with time zone);
CREATE OR REPLACE FUNCTION swh_revision_get_by(origin_id bigint, branch_name bytea = NULL::bytea, "date" timestamp with time zone = NULL::timestamp with time zone) RETURNS SETOF revision_entry
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
    from swh_occurrence_get_by(origin_id, branch_name, date) as occ
    inner join revision r on occ.target = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;

CREATE OR REPLACE FUNCTION swh_visit_find_by_date(origin bigint, visit_date timestamp with time zone = now()) RETURNS origin_visit
    LANGUAGE sql STABLE
    AS $$
  with closest_two_visits as ((
    select origin_visit, (date - visit_date) as interval
    from origin_visit
    where date >= visit_date
    order by date asc
    limit 1
  ) union (
    select origin_visit, (visit_date - date) as interval
    from origin_visit
    where date < visit_date
    order by date desc
    limit 1
  )) select (origin_visit).* from closest_two_visits order by interval limit 1
$$;
