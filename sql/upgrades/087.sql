-- SWH DB schema upgrade
-- from_version: 86
-- to_version: 87
-- description: indexer: Add indexer's new content properties table

insert into dbversion(version, release, description)
      values(87, now(), 'Work In Progress');

-- Properties (mimetype, encoding, etc...)
create table content_mimetype (
id sha1 primary key references content(sha1) not null,
  mimetype bytea not null,
  encoding bytea not null
);

comment on table content_mimetype is 'Metadata associated to a raw content';
comment on column content_mimetype.mimetype is 'Raw content Mimetype';
comment on column content_mimetype.encoding is 'Raw content encoding';

-- check which entries of tmp_bytea are missing from content_mimetype
--
-- operates in bulk: 0. swh_mktemp_bytea(), 1. COPY to tmp_bytea,
-- 2. call this function
create or replace function swh_mimetype_missing()
    returns setof sha1
    language plpgsql
as $$
begin
    return query
	(select id::sha1 from tmp_bytea as tmp
	 where not exists
	     (select 1 from content_mimetype as c where c.id = tmp.id));
    return;
end
$$;

comment on function swh_mimetype_missing() IS 'Filter missing mimetype';

