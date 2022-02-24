-- SWH DB schema upgrade
-- from_version: 66
-- to_version: 67
-- description: use tmp_bytea instead of tmp_revision to retrieve revision info

insert into dbversion(version, release, description)
      values(67, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_revision_get() RETURNS SETOF revision_entry
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select r.id, r.date, r.date_offset, r.date_neg_utc_offset,
               r.committer_date, r.committer_date_offset, r.committer_date_neg_utc_offset,
               r.type, r.directory, r.message,
               a.id, a.name, a.email, c.id, c.name, c.email, r.metadata, r.synthetic,
         array(select rh.parent_id::bytea from revision_history rh where rh.id = t.id order by rh.parent_rank)
                   as parents
        from tmp_bytea t
        left join revision r on t.id = r.id
        left join person a on a.id = r.author
        left join person c on c.id = r.committer;
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_revision_missing() RETURNS SETOF sha1_git
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select id::sha1_git from tmp_bytea t
	where not exists (
	    select 1 from revision r
	    where r.id = t.id);
    return;
end
$$;
