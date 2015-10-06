-- SWH DB schema upgrade
-- from_version: 20
-- to_version: 21
-- description: Add function swh_occurrence_history_add

insert into dbversion(version, release, description)
      values(21, now(), 'Work In Progress');

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
    insert into occurrence_history (origin, branch, revision, authority, validity)
	select distinct origin, branch, revision, authority, validity
	from tmp_occurrence_history t
	where not exists (
	    select 1 from occurrence_history o
	    where o.origin = t.origin and o.branch = t.branch and
	          o.authority = t.authority and o.revision = t.revision and
		  o.validity = t.validity);
    return;
end
$$;
