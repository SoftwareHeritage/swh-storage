-- SWH DB schema upgrade
-- from_version: 22
-- to_version: 23
-- description: Add a function to list revisions from tmp_revision

insert into dbversion(version, release, description)
      values(23, now(), 'Work In Progress');

create type revision_entry as
(
  id                     sha1_git,
  date                   timestamptz,
  date_offset            smallint,
  committer_date         timestamptz,
  committer_date_offset  smallint,
  type                   revision_type,
  directory              sha1_git,
  message                bytea,
  author_name            bytea,
  author_email           bytea,
  committer_name         bytea,
  committer_email        bytea,
  parents                bytea[]
);

CREATE OR REPLACE FUNCTION swh_revision_get() RETURNS SETOF revision_entry
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select t.id, r.date, r.date_offset,
               r.committer_date, r.committer_date_offset,
               r.type, r.directory, r.message,
               a.name, a.email, c.name, c.email,
	       array_agg(rh.parent_id::bytea order by rh.parent_rank)
                   as parents
        from tmp_revision t
        left join revision r on t.id = r.id
        left join person a on a.id = r.author
        left join person c on c.id = r.committer
        left join revision_history rh on rh.id = r.id
        group by t.id, a.name, a.email, r.date, r.date_offset,
               c.name, c.email, r.committer_date, r.committer_date_offset,
               r.type, r.directory, r.message;
    return;
end
$$;
