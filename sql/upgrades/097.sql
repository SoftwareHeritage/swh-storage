-- SWH DB schema upgrade
-- from_version: 96
-- to_version: 97
-- description: Update indexer configuration

insert into dbversion(version, release, description)
      values(97, now(), 'Work In Progress');

------------------------
-- Update Schema + data
------------------------

update indexer_configuration
set tool_configuration='{"command_line": "nomossa <filepath>"}'
where tool_name='nomos' and tool_version='3.1.0rc2-31-ga2cbb8c';

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('universal-ctags', '~git7859817b', '{"command_line": "ctags --fields=+lnz --sort=no --links=no --output-format=json <filepath>"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('pygments', '2.0.1+dfsg-1.1+deb8u1', '{"type": "library", "debian-package": "python3-pygments"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('file', '5.22', '{"command_line": "file --mime <filepath>"}');

-- ctags

create table content_ctags_new (
  id sha1 not null,
  name text not null,
  kind text not null,
  line bigint not null,
  lang ctags_languages not null,
  indexer_configuration_id bigserial not null
);

comment on table content_ctags_new is 'Ctags information on a raw content';
comment on column content_ctags_new.id is 'Content identifier';
comment on column content_ctags_new.name is 'Symbol name';
comment on column content_ctags_new.kind is 'Symbol kind (function, class, variable, const...)';
comment on column content_ctags_new.line is 'Symbol line';
comment on column content_ctags_new.lang is 'Language information for that content';
comment on column content_ctags_new.indexer_configuration_id is 'Tool used to compute the information';

insert into content_ctags_new(id, name, kind, line, lang, indexer_configuration_id)
select id, name, kind, line, lang, (select id from indexer_configuration where tool_name='universal-ctags' limit 1)
from content_ctags;

alter table content_ctags_new
add constraint content_ctags_id_idx
foreign key (id) references content(sha1);

alter table content_ctags_new
add constraint content_ctags_indexer_configuration_id_idx
foreign key (indexer_configuration_id) references indexer_configuration(id);

create index on content_ctags_new(id);
create unique index on content_ctags_new(id, hash_sha1(name), kind, line, lang, indexer_configuration_id);
create index on content_ctags_new(hash_sha1(name));

alter table content_ctags rename to content_ctags_old;
alter table content_ctags_new rename to content_ctags;
drop table content_ctags_old;

-- language

alter table content_language
  add column indexer_configuration_id bigserial;

comment on column content_language.indexer_configuration_id is 'Tool used to compute the information';

update content_language
set indexer_configuration_id = (select id from indexer_configuration where tool_name='pygments');

alter table content_language
  alter column indexer_configuration_id set not null;

alter table content_language
  add constraint content_language_indexer_configuration_id_idx
  foreign key (indexer_configuration_id) references indexer_configuration(id);

alter table content_language
  drop constraint content_language_pkey;

alter table content_language
  add primary key(id, indexer_configuration_id);

-- mimetype

alter table content_mimetype
  add column indexer_configuration_id bigserial;

comment on column content_mimetype.indexer_configuration_id is 'Tool used to compute the information';

update content_mimetype
set indexer_configuration_id = (select id from indexer_configuration where tool_name='file');

alter table content_mimetype
  alter column indexer_configuration_id
  set not null;

alter table content_mimetype
  add constraint content_mimetype_indexer_configuration_id_idx
  foreign key (indexer_configuration_id) references indexer_configuration(id);

alter table content_mimetype
  drop constraint content_mimetype_pkey;

alter table content_mimetype
  add primary key(id, indexer_configuration_id);

-- fossology-license

comment on column content_fossology_license.indexer_configuration_id is 'Tool used to compute the information';

alter table content_fossology_license
  alter column indexer_configuration_id
  set not null;

alter table content_fossology_license
  add primary key using index content_fossology_license_id_license_id_indexer_configurati_idx;

---------------------
-- Update functions
---------------------

-- ctags

-- create a temporary table for content_ctags missing routine
create or replace function swh_mktemp_content_ctags_missing()
    returns void
    language sql
as $$
  create temporary table tmp_content_ctags_missing (
    id           sha1,
    tool_name    text,
    tool_version text
  ) on commit drop;
$$;

comment on function swh_mktemp_content_ctags_missing() is 'Helper table to filter missing content ctags';

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
	(select id::sha1 from tmp_content_ctags_missing as tmp
	 where not exists
	     (select 1 from content_ctags as c
              inner join indexer_configuration i
              on (tmp.tool_name = i.tool_name and tmp.tool_version = i.tool_version)
              where c.id = tmp.id limit 1));
    return;
end
$$;

-- create a temporary table for content_ctags tmp_content_ctags,
create or replace function swh_mktemp_content_ctags()
    returns void
    language sql
as $$
  create temporary table tmp_content_ctags (
    like content_ctags including defaults
  ) on commit drop;
  alter table tmp_content_ctags
    drop column indexer_configuration_id,
    add column tool_name text,
    add column tool_version text;
$$;

comment on function swh_mktemp_content_ctags() is 'Helper table to add content ctags';

create or replace function swh_content_ctags_get()
    returns setof content_ctags_signature
    language plpgsql
as $$
begin
    return query
        select c.id, c.name, c.kind, c.line, c.lang, i.tool_name, i.tool_version
        from tmp_bytea t
        inner join content_ctags c using(id)
        inner join indexer_configuration i on i.id = c.indexer_configuration_id
        order by line;
    return;
end
$$;

comment on function swh_content_ctags_get() IS 'List content ctags';


-- add tmp_content_ctags entries to content_ctags, overwriting
-- duplicates if conflict_update is true, skipping duplicates otherwise.
--
-- operates in bulk: 0. swh_mktemp(content_ctags), 1. COPY to tmp_content_ctags,
-- 2. call this function
create or replace function swh_content_ctags_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    if conflict_update then
        delete from content_ctags
        where id in (select distinct id from tmp_content_ctags);
    end if;

    insert into content_ctags (id, name, kind, line, lang, indexer_configuration_id)
    select id, name, kind, line, lang,
           (select id from indexer_configuration
            where tool_name=tct.tool_name
            and tool_version=tct.tool_version)
    from tmp_content_ctags tct
        on conflict(id, hash_sha1(name), kind, line, lang, indexer_configuration_id)
        do nothing;
    return;
end
$$;

comment on function swh_content_ctags_add(boolean) IS 'Add new ctags symbols per content';

drop type content_ctags_signature cascade;
create type content_ctags_signature as (
  id sha1,
  name text,
  kind text,
  line bigint,
  lang ctags_languages,
  tool_name text,
  tool_version text
);


-- Retrieve list of content ctags from the temporary table.
--
-- operates in bulk: 0. mktemp(tmp_bytea), 1. COPY to tmp_bytea, 2. call this function
create or replace function swh_content_ctags_get()
    returns setof content_ctags_signature
    language plpgsql
as $$
begin
    return query
        select c.id, c.name, c.kind, c.line, c.lang, i.tool_name, i.tool_version
        from tmp_bytea t
        inner join content_ctags c using(id)
        inner join indexer_configuration i on i.id = c.indexer_configuration_id
        order by line;
    return;
end
$$;

comment on function swh_content_ctags_get() IS 'List content ctags';

-- Search within ctags content.
--
create or replace function swh_content_ctags_search(
       expression text,
       l integer default 10,
       last_sha1 sha1 default '\x0000000000000000000000000000000000000000')
    returns setof content_ctags_signature
    language sql
as $$
    select c.id, name, kind, line, lang, tool_name, tool_version
    from content_ctags c
    inner join indexer_configuration i on i.id = c.indexer_configuration_id
    where hash_sha1(name) = hash_sha1(expression)
    and c.id > last_sha1
    order by id
    limit l;
$$;

comment on function swh_content_ctags_search(text, integer, sha1) IS 'Equality search through ctags'' symbols';

-- mimetype

-- create a temporary table for content_ctags tmp_content_mimetype_missing,
create or replace function swh_mktemp_content_mimetype_missing()
    returns void
    language sql
as $$
  create temporary table tmp_content_mimetype_missing (
    id sha1 references content(sha1) not null,
    tool_name text not null,
    tool_version text not null
  ) on commit drop;
$$;

comment on function swh_mktemp_content_mimetype_missing() IS 'Helper table to filter existing mimetype information';

-- create a temporary table for content_ctags tmp_content_mimetype,
create or replace function swh_mktemp_content_mimetype()
    returns void
    language sql
as $$
  create temporary table tmp_content_mimetype (
    like content_mimetype including defaults
  ) on commit drop;
  alter table tmp_content_mimetype
    drop column indexer_configuration_id,
    add column tool_name text,
    add column tool_version text;
$$;

comment on function swh_mktemp_content_mimetype() is 'Helper table to add content ctags';

create or replace function swh_content_mimetype_missing()
    returns setof sha1
    language plpgsql
as $$
begin
    return query
	(select id::sha1 from tmp_content_mimetype_missing as tmp
	 where not exists
	     (select 1 from content_mimetype as c
              inner join indexer_configuration i
              on (tmp.tool_name = i.tool_name and tmp.tool_version = i.tool_version)
              where c.id = tmp.id));
    return;
end
$$;

comment on function swh_content_mimetype_missing() is 'Filter existing mimetype information';

create or replace function swh_content_mimetype_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    if conflict_update then
        insert into content_mimetype (id, mimetype, encoding, indexer_configuration_id)
        select id, mimetype, encoding,
               (select id from indexer_configuration
               where tool_name=tcm.tool_name
               and tool_version=tcm.tool_version)
        from tmp_content_mimetype tcm
            on conflict(id, indexer_configuration_id)
                do update set mimetype = excluded.mimetype,
                              encoding = excluded.encoding;

    else
        insert into content_mimetype (id, mimetype, encoding, indexer_configuration_id)
        select id, mimetype, encoding,
               (select id from indexer_configuration
               where tool_name=tcm.tool_name
               and tool_version=tcm.tool_version)
         from tmp_content_mimetype tcm
            on conflict(id, indexer_configuration_id)
            do nothing;
    end if;
    return;
end
$$;

comment on function swh_content_mimetype_add(boolean) IS 'Add new content mimetypes';

create type content_mimetype_signature as(
  id sha1,
  mimetype bytea,
  encoding bytea,
  tool_name text,
  tool_version text
);

drop function swh_content_mimetype_get();

create or replace function swh_content_mimetype_get()
    returns setof content_mimetype_signature
    language plpgsql
as $$
begin
    return query
        select c.id, mimetype, encoding, tool_name, tool_version
        from tmp_bytea t
        inner join content_mimetype c on c.id=t.id
        inner join indexer_configuration i on c.indexer_configuration_id=i.id;
    return;
end
$$;

comment on function swh_content_mimetype_get() IS 'List content''s mimetype';

-- language

-- create a temporary table for content_language tmp_content_language,
create or replace function swh_mktemp_content_language_missing()
    returns void
    language sql
as $$
  create temporary table tmp_content_language_missing (
    id sha1,
    lang languages,
    tool_name text,
    tool_version text
  ) on commit drop;
$$;

comment on function swh_mktemp_content_language_missing() is 'Helper table to filter missing language';

-- check which entries of tmp_bytea are missing from content_language
--
-- operates in bulk: 0. swh_mktemp_bytea(), 1. COPY to tmp_bytea,
-- 2. call this function
create or replace function swh_content_language_missing()
    returns setof sha1
    language plpgsql
as $$
begin
    return query
	select id::sha1 from tmp_content_language_missing as tmp
	where not exists
	    (select 1 from content_language as c
            inner join indexer_configuration i
            on (tmp.tool_name = i.tool_name and tmp.tool_version = i.tool_version)
            where c.id = tmp.id);
    return;
end
$$;

comment on function swh_content_language_missing() IS 'Filter missing content languages';

-- add tmp_content_language entries to content_language, overwriting
-- duplicates if conflict_update is true, skipping duplicates otherwise.
--
-- If filtering duplicates is in order, the call to
-- swh_content_language_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(content_language), 1. COPY to
-- tmp_content_language, 2. call this function
create or replace function swh_content_language_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    if conflict_update then
        insert into content_language (id, lang, indexer_configuration_id)
        select id, lang,
               (select id from indexer_configuration
               where tool_name=tcl.tool_name
               and tool_version=tcl.tool_version)
    	from tmp_content_language tcl
            on conflict(id, indexer_configuration_id)
                do update set lang = excluded.lang;

    else
        insert into content_language (id, lang, indexer_configuration_id)
        select id, lang,
               (select id from indexer_configuration
               where tool_name=tcl.tool_name
               and tool_version=tcl.tool_version)
    	from tmp_content_language tcl
            on conflict(id, indexer_configuration_id)
            do nothing;
    end if;
    return;
end
$$;

comment on function swh_content_language_add(boolean) IS 'Add new content languages';

-- create a temporary table for retrieving content_language
create or replace function swh_mktemp_content_language()
    returns void
    language sql
as $$
  create temporary table tmp_content_language (
    like content_language including defaults
  ) on commit drop;
  alter table tmp_content_language
    drop column indexer_configuration_id,
    add column tool_name text,
    add column tool_version text;
$$;

comment on function swh_mktemp_content_language() is 'Helper table to add content language';

create type content_language_signature as (
    id sha1,
    lang languages,
    tool_name text,
    tool_version text
);

drop function swh_content_language_get();

-- Retrieve list of content language from the temporary table.
--
-- operates in bulk: 0. mktemp(tmp_bytea), 1. COPY to tmp_bytea, 2. call this function
create or replace function swh_content_language_get()
    returns setof content_language_signature
    language plpgsql
as $$
begin
    return query
        select c.id, lang, tool_name, tool_version
        from tmp_bytea t
        inner join content_language c on c.id = t.id
        inner join indexer_configuration i on i.id=c.indexer_configuration_id;
    return;
end
$$;

comment on function swh_content_language_get() IS 'List content''s language';

-- license

-- create a temporary table for content_fossology_license_missing
create or replace function swh_mktemp_content_fossology_license_missing()
    returns void
    language sql
as $$
  create temporary table tmp_content_fossology_license_missing (
    id bytea,
    tool_name text,
    tool_version text
  ) on commit drop;
$$;

comment on function swh_mktemp_content_fossology_license_missing() is 'Helper table to add content license';

-- check which entries of tmp_content_fossology_license are missing from content_fossology_license
create or replace function swh_content_fossology_license_missing()
    returns setof sha1
    language plpgsql
as $$
begin
    return query
	(select id::sha1 from tmp_content_fossology_license_missing as tmp
	 where not exists
	     (select 1 from content_fossology_license as c
              inner join indexer_configuration i on i.id=c.indexer_configuration_id
              where c.id = tmp.id));
    return;
end
$$;

comment on function swh_content_fossology_license_missing() IS 'Filter missing content licenses';

-- create a temporary table for content_fossology_license tmp_content_fossology_license,
create or replace function swh_mktemp_content_fossology_license()
    returns void
    language sql
as $$
  create temporary table tmp_content_fossology_license (
    id           sha1,
    tool_name    text,
    tool_version text,
    license      text
  ) on commit drop;
$$;

comment on function swh_mktemp_content_fossology_license() is 'Helper table to add content license';

-- create a temporary table for checking licenses' name
create or replace function swh_mktemp_content_fossology_license_unknown()
    returns void
    language sql
as $$
  create temporary table tmp_content_fossology_license_unknown (
    name       text not null
  ) on commit drop;
$$;

comment on function swh_mktemp_content_fossology_license_unknown() is 'Helper table to list unknown licenses';
