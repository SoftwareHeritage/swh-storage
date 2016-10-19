-- SWH DB schema upgrade
-- from_version: 88
-- to_version: 89
-- description: indexer: Add content_ctags

insert into dbversion(version, release, description)
      values(89, now(), 'Work In Progress');

-- ctags metadata
create table content_ctags (
  id sha1 primary key references content(sha1) not null,
  ctags jsonb
);

-- add tmp_content_ctags entries to content_ctags, overwriting
-- duplicates if conflict_update is true, skipping duplicates otherwise.
--
-- If filtering duplicates is in order, the call to
-- swh_ctags_missing must take place before calling this function.
--
--
-- operates in bulk: 0. swh_mktemp(content_ctags), 1. COPY to tmp_content_ctags,
-- 2. call this function
create or replace function swh_content_ctags_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    if conflict_update then
        insert into content_ctags (id, ctags)
        select id, ctags
        from tmp_content_ctags
            on conflict(id)
              do update set ctags = excluded.ctags;
    else
        insert into content_ctags (id, ctags)
        select id, ctags
         from tmp_content_ctags
            on conflict do nothing;
    end if;
    return;
end
$$;

comment on function swh_content_ctags_add(boolean) IS 'Add new ctags symbols per content';

-- check which entries of tmp_bytea are missing from content_ctags
--
-- operates in bulk: 0. swh_mktemp_bytea(), 1. COPY to tmp_bytea,
-- 2. call this function
create or replace function swh_content_ctags_missing()
    returns setof sha1
    language plpgsql
as $$
begin
    return query
	(select id::sha1 from tmp_bytea as tmp
	 where not exists
	     (select 1 from content_ctags as c where c.id = tmp.id));
    return;
end
$$;

comment on function swh_content_ctags_missing() IS 'Filter missing content ctags';

-- Retrieve list of content ctags from the temporary table.
--
-- operates in bulk: 0. mktemp(tmp_bytea), 1. COPY to tmp_bytea, 2. call this function
create or replace function swh_content_ctags_get()
    returns setof content_ctags
    language plpgsql
as $$
begin
    return query
        select id::sha1, ctags
        from tmp_bytea t
        inner join content_ctags using(id);
    return;
end
$$;

comment on function swh_content_ctags_get() IS 'List content ctags';
