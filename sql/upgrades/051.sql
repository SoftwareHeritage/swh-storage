-- SWH DB schema upgrade
-- from_version: 50
-- to_version: 51
-- description: Let the person identifier flow

insert into dbversion(version, release, description)
      values(51, now(), 'Work In Progres');

drop type revision_entry cascade;
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
  author_id              bigint,
  author_name            bytea,
  author_email           bytea,
  committer_id           bigint,
  committer_name         bytea,
  committer_email        bytea,
  metadata               jsonb,
  synthetic              boolean,
  parents                bytea[]
);

create or replace function swh_revision_log(root_revisions bytea[], num_revs bigint default NULL)
    returns setof revision_entry
    language sql
    stable
as $$
    select t.id, r.date, r.date_offset,
           r.committer_date, r.committer_date_offset,
           r.type, r.directory, r.message,
           a.id, a.name, a.email, c.id, c.name, c.email, r.metadata, r.synthetic,
           t.parents
    from swh_revision_list(root_revisions, num_revs) as t
    left join revision r on t.id = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;

create or replace function swh_revision_get()
    returns setof revision_entry
    language plpgsql
as $$
begin
    return query
        select t.id, r.date, r.date_offset,
               r.committer_date, r.committer_date_offset,
               r.type, r.directory, r.message,
               a.id, a.name, a.email, c.id, c.name, c.email, r.metadata, r.synthetic,
         array(select rh.parent_id::bytea from revision_history rh where rh.id = t.id order by rh.parent_rank)
                   as parents
        from tmp_revision t
        left join revision r on t.id = r.id
        left join person a on a.id = r.author
        left join person c on c.id = r.committer;
    return;
end
$$;

create or replace function swh_revision_get_by(
       origin_id bigint,
       branch_name bytea default NULL,
       date timestamptz default NULL)
    returns setof revision_entry
    language sql
    stable
as $$
    select r.id, r.date, r.date_offset,
        r.committer_date, r.committer_date_offset,
        r.type, r.directory, r.message,
        a.id, a.name, a.email, c.id, c.name, c.email, r.metadata, r.synthetic,
        array(select rh.parent_id::bytea
            from revision_history rh
            where rh.id = r.id
            order by rh.parent_rank
        ) as parents
    from swh_occurrence_get_by(origin_id, branch_name, date) as occ
    inner join revision r on occ.target = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;

drop type release_entry cascade;
create type release_entry as
(
  id           sha1_git,
  target       sha1_git,
  target_type  object_type,
  date         timestamptz,
  date_offset  smallint,
  name         bytea,
  comment      bytea,
  synthetic    boolean,
  author_id    bigint,
  author_name  bytea,
  author_email bytea
);

create or replace function swh_release_get()
    returns setof release_entry
    language plpgsql
as $$
begin
    return query
        select r.id, r.target, r.target_type, r.date, r.date_offset, r.name, r.comment,
               r.synthetic, p.id as author_id, p.name as author_name, p.email as author_email
        from tmp_release_get t
        inner join release r on t.id = r.id
        inner join person p on p.id = r.author;
    return;
end
$$;

create or replace function swh_release_get_by(
       origin_id bigint)
    returns setof release_entry
    language sql
    stable
as $$
   select r.id, r.target, r.target_type, r.date, r.date_offset,
        r.name, r.comment, r.synthetic, a.id as author_id,
        a.name as author_name, a.email as author_email
    from release r
    inner join occurrence_history occ on occ.target = r.target
    left join person a on a.id = r.author
    where occ.origin = origin_id and occ.target_type = 'revision' and r.target_type = 'revision';
$$;
