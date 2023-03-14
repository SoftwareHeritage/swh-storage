-- SWH DB schema upgrade
-- from_version: 186
-- to_version: 187
-- description: Fix swh_visit_find_by_date to properly use the origin_visit(origin, date) index


-- Find the visit of origin closest to date visit_date
-- Breaks ties by selecting the largest visit id
create or replace function swh_visit_find_by_date(origin_url text, visit_date timestamptz default NOW())
    returns setof origin_visit
    language plpgsql
    stable
as $$
declare
  origin_id bigint;
begin
  select id into origin_id from origin where url=origin_url;
  return query
  -- first find the two closest dates (this does a one-row scan of the origin_visit
  -- (origin, date) index twice, forward and backward around the given visit_date)
  with closest_two_visit_dates as ((
    select date, (date - visit_date) as interval -- date >= visit_date so interval >= 0
    from origin_visit ov
    where ov.origin = origin_id
          and ov.date >= visit_date
    order by ov.date asc
    limit 1
  ) union (
    select date, (visit_date - date) as interval -- date < visit_date so interval > 0
    from origin_visit ov
    where ov.origin = origin_id
          and ov.date < visit_date
    order by ov.date desc
    limit 1
  ))
  -- then select the data of the visit at the closest date, with the highest visit id
  -- (this uses the origin_visit (origin, date) index a third time to find the highest
  -- visit id at the given date)
  select * from origin_visit
    where origin = origin_id
      and date = (select date from closest_two_visit_dates order by interval limit 1)
    order by visit desc
    limit 1;
end
$$;
