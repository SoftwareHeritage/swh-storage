-- SWH DB schema upgrade
-- from_version: 155
-- to_version: 156
-- description: Make swh_release_add properly idempotent

-- latest schema version
insert into dbversion(version, release, description)
      values(156, now(), 'Work In Progress');

-- Create entries in release from tmp_release
create or replace function swh_release_add()
    returns void
    language plpgsql
as $$
begin
    perform swh_person_add_from_release();

    insert into release (id, target, target_type, date, date_offset, date_neg_utc_offset, name, comment, author, synthetic)
      select distinct t.id, t.target, t.target_type, t.date, t.date_offset, t.date_neg_utc_offset, t.name, t.comment, a.id, t.synthetic
        from tmp_release t
        left join person a on a.fullname = t.author_fullname
        where not exists (select 1 from release where t.id = release.id);
    return;
end
$$;
