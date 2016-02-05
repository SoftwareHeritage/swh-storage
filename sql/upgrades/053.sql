-- SWH DB schema upgrade
-- from_version: 52
-- to_version: 53
-- description: Uniformize tmp_release and tmp_release_get tables to tmp_release

insert into dbversion(version, release, description)
      values(53, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_mktemp_release_get() RETURNS void
    LANGUAGE sql
    AS $$
    create temporary table tmp_release (
      id sha1_git primary key
    ) on commit drop;
$$;

CREATE OR REPLACE FUNCTION swh_release_get() RETURNS SETOF release_entry
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select r.id, r.target, r.target_type, r.date, r.date_offset, r.date_neg_utc_offset, r.name, r.comment,
               r.synthetic, p.id as author_id, p.name as author_name, p.email as author_email
        from tmp_release t
        inner join release r on t.id = r.id
        inner join person p on p.id = r.author;
    return;
end
$$;
