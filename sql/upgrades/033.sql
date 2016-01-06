-- SWH DB schema upgrade
-- from_version: 32
-- to_version: 33
-- description: revision_log 'parents' aware

insert into dbversion(version, release, description)
      values(33, now(), 'Work In Progress');

drop function swh_revision_log(sha1_git);

drop type revision_log_entry;

CREATE OR REPLACE FUNCTION swh_revision_log(root_revision sha1_git) RETURNS SETOF revision_entry
    LANGUAGE sql STABLE
    AS $$
    select t.id, r.date, r.date_offset,
           r.committer_date, r.committer_date_offset,
           r.type, r.directory, r.message,
           a.name, a.email, c.name, c.email, r.metadata, r.synthetic,
           array_agg(rh.parent_id::bytea order by rh.parent_rank) as parents
    from swh_revision_list(root_revision) as t(id)
    left join revision r on t.id = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer
    left join revision_history rh on rh.id = r.id
    group by t.id, a.name, a.email, r.date, r.date_offset,
             c.name, c.email, r.committer_date, r.committer_date_offset,
             r.type, r.directory, r.message, r.metadata, r.synthetic;
$$;
