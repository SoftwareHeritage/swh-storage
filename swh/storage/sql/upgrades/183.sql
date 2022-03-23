-- SWH DB schema upgrade
-- from_version: 182
-- to_version: 183
-- description: add support for NULL as revision.author or revision.committer

insert into dbversion(version, release, description)
    values(183, now(), 'Work In Progress');

select swh_get_dbflavor() = 'read_replica' as dbflavor_read_replica \gset
select swh_get_dbflavor() != 'read_replica' as dbflavor_does_deduplication \gset
select swh_get_dbflavor() = 'mirror' as dbflavor_mirror \gset
select swh_get_dbflavor() = 'default' as dbflavor_default \gset

\if :dbflavor_does_deduplication
  -- if the author is null, then the date must be null
  alter table revision add constraint revision_author_date_check check ((date is null) or (author is not null)) not valid;
  alter table revision validate constraint revision_author_date_check;

  -- if the committer is null, then the committer_date must be null
  alter table revision add constraint revision_committer_date_check check ((committer_date is null) or (committer is not null)) not valid;
  alter table revision validate constraint revision_committer_date_check;
\endif

create or replace function swh_person_add_from_revision()
    returns void
    language plpgsql
as $$
begin
    with t as (
        select author_fullname as fullname, author_name as name, author_email as email from tmp_revision
    union
        select committer_fullname as fullname, committer_name as name, committer_email as email from tmp_revision
    ) insert into person (fullname, name, email)
    select distinct on (fullname) fullname, name, email from t
    where t.fullname is not null and not exists (
        select 1
        from person p
        where t.fullname = p.fullname
    );
    return;
end
$$;
