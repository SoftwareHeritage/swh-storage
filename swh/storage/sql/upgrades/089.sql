-- SWH DB schema upgrade
-- from_version: 88
-- to_version: 89
-- description: indexer: Add content_ctags

insert into dbversion(version, release, description)
      values(89, now(), 'Work In Progress');

create type ctags_languages as enum (
  'Ada',
  'AnsiblePlaybook',
  'Ant',
  'Asm',
  'Asp',
  'Autoconf',
  'Automake',
  'Awk',
  'Basic',
  'BETA',
  'C',
  'C#',
  'C++',
  'Clojure',
  'Cobol',
  'CoffeeScript',
  'CSS',
  'ctags',
  'D',
  'DBusIntrospect',
  'Diff',
  'DosBatch',
  'DTS',
  'Eiffel',
  'Erlang',
  'Falcon',
  'Flex',
  'Fortran',
  'gdbinit',
  'Glade',
  'Go',
  'HTML',
  'Iniconf',
  'Java',
  'JavaProperties',
  'JavaScript',
  'JSON',
  'Lisp',
  'Lua',
  'M4',
  'Make',
  'man',
  'MatLab',
  'Maven2',
  'Myrddin',
  'ObjectiveC',
  'OCaml',
  'OldC',
  'OldC++',
  'Pascal',
  'Perl',
  'Perl6',
  'PHP',
  'PlistXML',
  'pod',
  'Protobuf',
  'Python',
  'PythonLoggingConfig',
  'R',
  'RelaxNG',
  'reStructuredText',
  'REXX',
  'RpmSpec',
  'Ruby',
  'Rust',
  'Scheme',
  'Sh',
  'SLang',
  'SML',
  'SQL',
  'SVG',
  'SystemdUnit',
  'SystemVerilog',
  'Tcl',
  'Tex',
  'TTCN',
  'Vera',
  'Verilog',
  'VHDL',
  'Vim',
  'WindRes',
  'XSLT',
  'YACC',
  'Yaml',
  'YumRepo',
  'Zephir'
);

-- ctags information per content
create table content_ctags (
  id sha1 references content(sha1) not null,
  name text not null,
  kind text not null,
  line bigint not null,
  lang ctags_languages not null
);

comment on table content_ctags is 'Ctags information on a raw content';
comment on column content_ctags.id is 'Content identifier';
comment on column content_ctags.name is 'Symbol name';
comment on column content_ctags.kind is 'Symbol kind (function, class, variable, const...)';
comment on column content_ctags.line is 'Symbol line';
comment on column content_ctags.lang is 'Language information for that content';

create index on content_ctags(id);
create unique index on content_ctags(id, md5(name), kind, line, lang);

-- add tmp_content_ctags entries to content_ctags, overwriting
-- duplicates if conflict_update is true, skipping duplicates otherwise.
--
-- If filtering duplicates is in order, the call to
-- swh_ctags_missing must take place before calling this function.
--
--
-- operates in bulk: 0. swh_mktemp(content_ctags), 1. COPY to tmp_content_ctags,
-- 2. call this function
create or replace function swh_content_ctags_add()
    returns void
    language plpgsql
as $$
begin
    insert into content_ctags (id, name, kind, line, lang)
    select id, name, kind, line, lang
    from tmp_content_ctags
        on conflict(id, md5(name), kind, line, lang)
        do nothing;
    return;
end
$$;

comment on function swh_content_ctags_add() IS 'Add new ctags symbols per content';

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
	     (select 1 from content_ctags as c where c.id = tmp.id limit 1));
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
        select id::sha1, name, kind, line, lang
        from tmp_bytea t
        inner join content_ctags using(id)
        order by line;
    return;
end
$$;

comment on function swh_content_ctags_get() IS 'List content ctags';
