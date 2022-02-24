-- SWH DB schema upgrade
-- from_version: 87
-- to_version: 88
-- description: indexer: Permit to update existing data (language, mimetype)

insert into dbversion(version, release, description)
      values(88, now(), 'Work In Progress');

alter function swh_language_missing() rename to swh_content_language_missing;
alter function swh_mimetype_missing() rename to swh_content_mimetype_missing;

drop function swh_mimetype_add();
drop function swh_language_add();

-- add tmp_content_mimetype entries to content_mimetype, overwriting
-- duplicates if conflict_update is true, skipping duplicates otherwise.
--
-- If filtering duplicates is in order, the call to
-- swh_mimetype_missing must take place before calling this function.
--
--
-- operates in bulk: 0. swh_mktemp(content_mimetype), 1. COPY to tmp_content_mimetype,
-- 2. call this function
create or replace function swh_content_mimetype_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    if conflict_update then
        insert into content_mimetype (id, mimetype, encoding)
        select id, mimetype, encoding
        from tmp_content_mimetype
            on conflict(id)
              do update set mimetype = excluded.mimetype,
                            encoding = excluded.encoding;
    else
        insert into content_mimetype (id, mimetype, encoding)
        select id, mimetype, encoding
         from tmp_content_mimetype
            on conflict do nothing;
    end if;
    return;
end
$$;

comment on function swh_content_mimetype_add(boolean) IS 'Add new content mimetypes';

-- Retrieve list of content mimetype from the temporary table.
--
-- operates in bulk: 0. mktemp(tmp_bytea), 1. COPY to tmp_bytea, 2. call this function
create or replace function swh_content_mimetype_get()
    returns setof content_mimetype
    language plpgsql
as $$
begin
    return query
        select id::sha1, mimetype, encoding
        from tmp_bytea t
        inner join content_mimetype using(id);
    return;
end
$$;

comment on function swh_content_mimetype_get() IS 'List content mimetypes';

-- add tmp_content_language entries to content_language, overwriting
-- duplicates if conflict_update is true, skipping duplicates otherwise.
--
-- If filtering duplicates is in order, the call to
-- swh_mimetype_missing must take place before calling this function.
--
-- operates in bulk: 0. swh_mktemp(content_language), 1. COPY to tmp_content_language,
-- 2. call this function
create or replace function swh_content_language_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    if conflict_update then
        insert into content_language (id, lang)
        select id, lang
    	from tmp_content_language
            on conflict(id)
              do update set lang = excluded.lang;
    else
        insert into content_language (id, lang)
        select id, lang
    	from tmp_content_language
            on conflict do nothing;
    end if;
    return;
end
$$;

comment on function swh_content_language_add(boolean) IS 'Add new content languages';

-- Retrieve list of content language from the temporary table.
--
-- operates in bulk: 0. mktemp(tmp_bytea), 1. COPY to tmp_bytea, 2. call this function
create or replace function swh_content_language_get()
    returns setof content_language
    language plpgsql
as $$
begin
    return query
        select id::sha1, lang
        from tmp_bytea t
        inner join content_language using(id);
    return;
end
$$;

comment on function swh_content_language_get() IS 'List content languages';
