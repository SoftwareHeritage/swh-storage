-- SWH DB schema upgrade
-- from_version: 142
-- to_version: 143
-- description: Remove origin ids

insert into dbversion(version, release, description)
      values(143, now(), 'Work In Progress');

create or replace function swh_origin_visit_add(origin_url text, date timestamptz, type text)
    returns bigint
    language sql
as $$
  with origin_id as (
    select id
    from origin
    where url = origin_url
  ), last_known_visit as (
    select coalesce(max(visit), 0) as visit
    from origin_visit
    where origin = (select id from origin_id)
  )
  insert into origin_visit (origin, date, type, visit, status)
  values ((select id from origin_id), date, type,
          (select visit from last_known_visit) + 1, 'ongoing')
  returning visit;
$$;

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
  with closest_two_visits as ((
    select ov, (date - visit_date), visit as interval
    from origin_visit ov
    where ov.origin = origin_id
          and ov.date >= visit_date
    order by ov.date asc, ov.visit desc
    limit 1
  ) union (
    select ov, (visit_date - date), visit as interval
    from origin_visit ov
    where ov.origin = origin_id
          and ov.date < visit_date
    order by ov.date desc, ov.visit desc
    limit 1
  )) select (ov).* from closest_two_visits order by interval, visit limit 1;
end
$$;

drop function swh_visit_get;

alter type origin_metadata_signature
  rename attribute origin_id to origin_url;

alter type origin_metadata_signature
  alter attribute origin_url set data type text;

create or replace function swh_origin_metadata_get_by_origin(
       origin text)
    returns setof origin_metadata_signature
    language sql
    stable
as $$
    select om.id as id, o.url as origin_url, discovery_date, tool_id, om.metadata,
           mp.id as provider_id, provider_name, provider_type, provider_url
    from origin_metadata as om
    inner join metadata_provider mp on om.provider_id = mp.id
    inner join origin o on om.origin_id = o.id
    where o.url = origin
    order by discovery_date desc;
$$;

create or replace function swh_origin_metadata_get_by_provider_type(
       origin_url text,
       provider_type text)
    returns setof origin_metadata_signature
    language sql
    stable
as $$
    select om.id as id, o.url as origin_url, discovery_date, tool_id, om.metadata,
           mp.id as provider_id, provider_name, provider_type, provider_url
    from origin_metadata as om
    inner join metadata_provider mp on om.provider_id = mp.id
    inner join origin o on om.origin_id = o.id
    where o.url = origin_url
    and mp.provider_type = provider_type
    order by discovery_date desc;
$$;
