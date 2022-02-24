-- SWH DB schema upgrade
-- from_version: 178
-- to_version: 179
-- description: add {,committer_}date_offset_bytes to rev/rel + raw_manifest to dir/rev/rel, part 1

insert into dbversion(version, release, description)
    values(179, now(), 'Work In Progress');

-- step 1: add columns, update functions

alter table release
    add column date_offset_bytes bytea,
    add column raw_manifest bytea;
comment on column release.date_offset_bytes is 'Raw git representation of the timezone, as an offset from UTC. It should follow this format: ``+HHMM`` or ``-HHMM``';
comment on column release.raw_manifest is 'git manifest of the object, if it cannot be represented using only the other fields';


alter table revision
    add column date_offset_bytes bytea,
    add column committer_date_offset_bytes bytea,
    add column raw_manifest bytea;
comment on column revision.date_offset_bytes is 'Raw git representation of the timezone, as an offset from UTC. It should follow this format: ``+HHMM`` or ``-HHMM``';
comment on column revision.committer_date_offset_bytes is 'Raw git representation of the timezone, as an offset from UTC. It should follow this format: ``+HHMM`` or ``-HHMM``';
comment on column revision.raw_manifest is 'git manifest of the object, if it cannot be represented using only the other fields';

drop function swh_revision_log;
drop function swh_revision_list_by_object_id;
drop function swh_revision_add;
drop type revision_entry;
create type revision_entry as
(
  id                             sha1_git,
  date                           timestamptz,
  date_offset                    smallint,
  date_neg_utc_offset            boolean,
  date_offset_bytes              bytea,
  committer_date                 timestamptz,
  committer_date_offset          smallint,
  committer_date_neg_utc_offset  boolean,
  committer_date_offset_bytes    bytea,
  type                           revision_type,
  directory                      sha1_git,
  message                        bytea,
  author_id                      bigint,
  author_fullname                bytea,
  author_name                    bytea,
  author_email                   bytea,
  committer_id                   bigint,
  committer_fullname             bytea,
  committer_name                 bytea,
  committer_email                bytea,
  metadata                       jsonb,
  synthetic                      boolean,
  parents                        bytea[],
  object_id                      bigint,
  extra_headers                  bytea[][],
  raw_manifest                   bytea
);

alter table directory
    add column raw_manifest bytea;
comment on column directory.raw_manifest is 'git manifest of the object, if it cannot be represented using only the other fields';

create or replace function swh_directory_add()
    returns void
    language plpgsql
as $$
begin
    perform swh_directory_entry_add('file');
    perform swh_directory_entry_add('dir');
    perform swh_directory_entry_add('rev');

    insert into directory (id, dir_entries, file_entries, rev_entries, raw_manifest)
    select id, dir_entries, file_entries, rev_entries, raw_manifest from tmp_directory t
    where not exists (
        select 1 from directory d
	where d.id = t.id);

    return;
end
$$;

create or replace function swh_revision_log(root_revisions bytea[], num_revs bigint default NULL)
    returns setof revision_entry
    language sql
    stable
as $$
    select t.id, r.date, r.date_offset, r.date_neg_utc_offset, r.date_offset_bytes,
           r.committer_date, r.committer_date_offset, r.committer_date_neg_utc_offset, r.committer_date_offset_bytes,
           r.type, r.directory, r.message,
           a.id, a.fullname, a.name, a.email,
           c.id, c.fullname, c.name, c.email,
           r.metadata, r.synthetic, t.parents, r.object_id, r.extra_headers,
           r.raw_manifest
    from swh_revision_list(root_revisions, num_revs) as t
    left join revision r on t.id = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;

create or replace function swh_revision_list_by_object_id(
    min_excl bigint,
    max_incl bigint
)
    returns setof revision_entry
    language sql
    stable
as $$
    with revs as (
        select * from revision
        where object_id > min_excl and object_id <= max_incl
    )
    select r.id, r.date, r.date_offset, r.date_neg_utc_offset, r.date_offset_bytes,
           r.committer_date, r.committer_date_offset, r.committer_date_neg_utc_offset, r.committer_date_offset_bytes,
           r.type, r.directory, r.message,
           a.id, a.fullname, a.name, a.email, c.id, c.fullname, c.name, c.email, r.metadata, r.synthetic,
           array(select rh.parent_id::bytea from revision_history rh where rh.id = r.id order by rh.parent_rank)
               as parents, r.object_id, r.extra_headers, r.raw_manifest
    from revs r
    left join person a on a.id = r.author
    left join person c on c.id = r.committer
    order by r.object_id;
$$;

create or replace function swh_revision_add()
    returns void
    language plpgsql
as $$
begin
    perform swh_person_add_from_revision();

    insert into revision (id,   date,   date_offset,   date_neg_utc_offset,   date_offset_bytes,   committer_date,   committer_date_offset,   committer_date_neg_utc_offset,   committer_date_offset_bytes,   type,   directory,   message, author, committer,   metadata,   synthetic,   extra_headers,   raw_manifest)
    select              t.id, t.date, t.date_offset, t.date_neg_utc_offset, t.date_offset_bytes, t.committer_date, t.committer_date_offset, t.committer_date_neg_utc_offset, t.committer_date_offset_bytes, t.type, t.directory, t.message,   a.id,      c.id, t.metadata, t.synthetic, t.extra_headers, t.raw_manifest
    from tmp_revision t
    left join person a on a.fullname = t.author_fullname
    left join person c on c.fullname = t.committer_fullname;
    return;
end
$$;

create or replace function swh_release_add()
    returns void
    language plpgsql
as $$
begin
    perform swh_person_add_from_release();

    insert into release (id,   target,   target_type,   date,   date_offset,   date_neg_utc_offset,   date_offset_bytes,   name,   comment, author,   synthetic,    raw_manifest)
      select distinct  t.id, t.target, t.target_type, t.date, t.date_offset, t.date_neg_utc_offset, t.date_offset_bytes, t.name, t.comment,   a.id, t.synthetic, t.raw_manifest
        from tmp_release t
        left join person a on a.fullname = t.author_fullname
        where not exists (select 1 from release where t.id = release.id);
    return;
end
$$;

-- step 2: upgrade python code to start writing to them

-- data migrations in 180.sql
