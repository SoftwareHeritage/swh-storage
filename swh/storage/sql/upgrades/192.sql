-- SWH DB schema upgrade
-- from_version: 191
-- to_version: 192
-- description: Add payloads to ExtIDs

-- Add payload_type and payload columns.

alter table extid
  add column payload_type text;
comment on column extid.payload_type is 'The type of the payload object';

alter table extid
  add column payload sha1_git;
comment on column extid.payload is 'sha1_git of data associated with the ExtID';

-- Add payload constraint.

alter table extid
  add constraint extid_payload_check
  check ((payload_type is null) = (payload is null))
  not valid;

-- Update the unique index.

create unique index concurrently extid_no_payload_idx
  on extid(extid_type, extid, extid_version, target_type, target)
  where payload_type is null and payload is null;

create unique index concurrently extid_payload_idx
  on extid(extid_type, extid, extid_version, target_type, target, payload_type, payload)
  where payload_type is not null and payload is not null;

drop index if exists extid_extid_type_extid_extid_version_target_type_target_idx;

-- Update the swh_extid_add procedure to include the new columns.

create or replace function swh_extid_add()
    returns void
    language plpgsql
as $$
begin
    insert into extid (extid_type, extid, extid_version, target_type, target, payload_type, payload)
        select distinct t.extid_type, t.extid, t.extid_version, t.target_type, t.target, t.payload_type, t.payload
        from tmp_extid t
		on conflict do nothing;
    return;
end
$$;
