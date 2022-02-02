-- SWH DB schema upgrade
-- from_version: 181
-- to_version: 182
-- description: add displayname field to person table

insert into dbversion(version, release, description)
    values(182, now(), 'Work In Progress');

alter table person add column displayname bytea;

comment on table person is 'Person, referenced in Revision author/committer or Release author';
comment on column person.id is 'Internal id';
comment on column person.name is 'Name (advisory, only present if parsed from fullname)';
comment on column person.email is 'Email (advisory, only present if parsed from fullname)';
comment on column person.fullname is 'Full name, usually of the form `Name <email>`, '
                                     'used in integrity computations';
comment on column person.displayname is 'Full name, usually of the form `Name <email>`, '
                                        'used for display queries';

create or replace function swh_revision_log(root_revisions bytea[], num_revs bigint default NULL, ignore_displayname boolean default false)
    returns setof revision_entry
    language sql
    stable
as $$
    -- when name and email are null, swh.storage.postgresql.converters.db_to_author()
    -- parses fullname to populate them, so we can just drop them here
    select t.id, r.date, r.date_offset, r.date_neg_utc_offset, r.date_offset_bytes,
           r.committer_date, r.committer_date_offset, r.committer_date_neg_utc_offset, r.committer_date_offset_bytes,
           r.type, r.directory, r.message,
           a.id,
           case when ignore_displayname or a.displayname is null then a.fullname else a.displayname end,
           case when ignore_displayname or a.displayname is null then a.name else null end,
           case when ignore_displayname or a.displayname is null then a.email else null end,
           c.id,
           case when ignore_displayname or c.displayname is null then c.fullname else c.displayname end,
           case when ignore_displayname or c.displayname is null then c.name else null end,
           case when ignore_displayname or c.displayname is null then c.email else null end,
           r.metadata, r.synthetic, t.parents, r.object_id, r.extra_headers,
           r.raw_manifest
    from swh_revision_list(root_revisions, num_revs) as t
    left join revision r on t.id = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;

comment on function swh_revision_log(root_revisions bytea[], num_revs bigint, ignore_displayname boolean)
  is '"git style" revision log. Similar to swh_revision_list(), but returning '
     'all information associated to each revision, and expanding authors/committers';

drop function if exists swh_revision_log(root_revisions bytea[], num_revs bigint);
