-- SWH DB schema upgrade
-- from_version: 41
-- to_version: 42
-- description: Open release get by origin

insert into dbversion(version, release, description)
      values(42, now(), 'Work In Progress');

-- Retrieve a release by occurrence criterion
create or replace function swh_release_get_by(
       origin_id bigint)
    returns setof release_entry
    language sql
    stable
as $$
   select r.id, r.revision, r.date, r.date_offset,
        r.name, r.comment, r.synthetic, a.name as author_name,
        a.email as author_email
    from release r
    inner join occurrence_history occ on occ.revision = r.revision
    left join person a on a.id = r.author
    where occ.origin = origin_id;
$$;
