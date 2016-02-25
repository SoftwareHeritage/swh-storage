-- SWH DB schema upgrade
-- from_version: 58
-- to_version: 59
-- description: Factor out swh_mktemp_content_sha1 and swh_mktemp_release_get to swh_mktemp_bytea.

insert into dbversion(version, release, description)
      values(59, now(), 'Work In Progress');

DROP FUNCTION swh_mktemp_content_sha1();

DROP FUNCTION swh_mktemp_release_get();

CREATE OR REPLACE FUNCTION swh_content_missing_per_sha1() RETURNS SETOF sha1
    LANGUAGE plpgsql
    AS $$
begin
    return query
           (select id::sha1
            from tmp_bytea as tmp
            where not exists
            (select 1 from content as c where c.sha1=tmp.id));
end
$$;

CREATE OR REPLACE FUNCTION swh_mktemp_bytea() RETURNS void
    LANGUAGE sql
    AS $$
    create temporary table tmp_bytea (
      id bytea
    ) on commit drop;
$$;

CREATE OR REPLACE FUNCTION swh_release_get() RETURNS SETOF release_entry
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select r.id, r.target, r.target_type, r.date, r.date_offset, r.date_neg_utc_offset, r.name, r.comment,
               r.synthetic, p.id as author_id, p.name as author_name, p.email as author_email
        from tmp_bytea t
        inner join release r on t.id = r.id
        inner join person p on p.id = r.author;
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_release_missing() RETURNS SETOF sha1_git
    LANGUAGE plpgsql
    AS $$
begin
  return query
    select id::sha1_git from tmp_bytea t
    where not exists (
      select 1 from release r
      where r.id = t.id);
end
$$;
