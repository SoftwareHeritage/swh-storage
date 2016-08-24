-- SWH DB schema upgrade
-- from_version: 75
-- to_version: 76
-- description: Add a metadata column for origin_visit

insert into dbversion(version, release, description)
      values(76, now(), 'Work In Progress');

ALTER TABLE origin_visit
	ADD COLUMN metadata jsonb;

COMMENT ON COLUMN origin_visit.metadata IS 'Metadata associated with the visit';

CREATE OR REPLACE FUNCTION swh_visit_get(origin bigint) RETURNS origin_visit
    LANGUAGE sql STABLE
    AS $$
    select *
    from origin_visit
    where origin=origin
    order by date desc
$$;
