-- SWH DB schema upgrade
-- from_version: 175
-- to_version: 176
-- description: add storage of the extid.extid_version field

insert into dbversion(version, release, description)
      values(176, now(), 'Work In Progress');

alter table extid add column extid_version bigint not null default 0;

comment on column extid.extid_version is 'Version of the extid type for the given original object';

create or replace function swh_extid_add()
    returns void
    language plpgsql
as $$
begin
    insert into extid (extid_type, extid, extid_version, target_type, target)
        select distinct t.extid_type, t.extid, t.extid_version, t.target_type, t.target
        from tmp_extid t
		on conflict do nothing;
    return;
end
$$;

create unique index concurrently on extid(extid_type, extid, extid_version, target_type, target);
drop index extid_extid_type_extid_target_type_target_idx;
