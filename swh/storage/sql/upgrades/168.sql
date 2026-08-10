-- SWH DB schema upgrade
-- from_version: 167
-- to_version: 168
-- description: Add ExtID related tables

insert into dbversion(version, release, description)
      values(168, now(), 'Work In Progress');


-- The ExtID (typ. original VCS) <-> swhid relation table
create table extid
(
  extid_type  text not null,
  extid       bytea not null,
  target_type object_type not null,
  target      sha1_git not null
);

comment on table extid is 'Correspondance SWH object (SWHID) <-> original revision id (vcs id)';
comment on column extid.extid_type is 'ExtID type';
comment on column extid.extid is 'Intrinsic identifier of the object (e.g. hg revision)';
comment on column extid.target_type is 'Type of SWHID of the referenced SWH object';
comment on column extid.target is 'Value (hash) of SWHID of the referenced SWH object';

-- Create entries in extid from tmp_extid
-- operates in bulk: 0. swh_mktemp(extid), 1. COPY to tmp_extid,
-- 2. call this function
create or replace function swh_extid_add()
    returns void
    language plpgsql
as $$
begin
    insert into extid (extid_type, extid, target_type, target)
        select distinct t.extid_type, t.extid, t.target_type, t.target
        from tmp_extid t
		on conflict do nothing;
    return;
end
$$;

-- extid indexes
create unique index concurrently on extid(extid_type, extid);
create unique index concurrently on extid(target_type, target);
