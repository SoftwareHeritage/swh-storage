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

-- add tmp_content_mimetype entries to content_mimetype, skipping duplicates
--
-- operates in bulk: 0. swh_mktemp(content_mimetype), 1. COPY to tmp_content_mimetype,
-- 2. call this function
create or replace function swh_mimetype_add()
    returns void
    language plpgsql
as $$
begin
    insert into content_mimetype (id, mimetype, encoding)
	select id, mimetype, encoding
	from tmp_content_mimetype
        on conflict do nothing;
    return;
end
$$;

COMMENT ON FUNCTION swh_mimetype_add() IS 'Add new content mimetype';

create type languages as enum (
  'abap',
  'abnf',
  'actionscript',
  'actionscript-3',
  'ada',
  'adl',
  'agda',
  'alloy',
  'ambienttalk',
  'antlr',
  'antlr-with-actionscript-target',
  'antlr-with-c#-target',
  'antlr-with-cpp-target',
  'antlr-with-java-target',
  'antlr-with-objectivec-target',
  'antlr-with-perl-target',
  'antlr-with-python-target',
  'antlr-with-ruby-target',
  'apacheconf',
  'apl',
  'applescript',
  'arduino',
  'aspectj',
  'aspx-cs',
  'aspx-vb',
  'asymptote',
  'autohotkey',
  'autoit',
  'awk',
  'base-makefile',
  'bash',
  'bash-session',
  'batchfile',
  'bbcode',
  'bc',
  'befunge',
  'blitzbasic',
  'blitzmax',
  'bnf',
  'boo',
  'boogie',
  'brainfuck',
  'bro',
  'bugs',
  'c',
  'c#',
  'c++',
  'c-objdump',
  'ca65-assembler',
  'cadl',
  'camkes',
  'cbm-basic-v2',
  'ceylon',
  'cfengine3',
  'cfstatement',
  'chaiscript',
  'chapel',
  'cheetah',
  'cirru',
  'clay',
  'clojure',
  'clojurescript',
  'cmake',
  'cobol',
  'cobolfree',
  'coffeescript',
  'coldfusion-cfc',
  'coldfusion-html',
  'common-lisp',
  'component-pascal',
  'coq',
  'cpp-objdump',
  'cpsa',
  'crmsh',
  'croc',
  'cryptol',
  'csound-document',
  'csound-orchestra',
  'csound-score',
  'css',
  'css+django/jinja',
  'css+genshi-text',
  'css+lasso',
  'css+mako',
  'css+mozpreproc',
  'css+myghty',
  'css+php',
  'css+ruby',
  'css+smarty',
  'cuda',
  'cypher',
  'cython',
  'd',
  'd-objdump',
  'darcs-patch',
  'dart',
  'debian-control-file',
  'debian-sourcelist',
  'delphi',
  'dg',
  'diff',
  'django/jinja',
  'docker',
  'dtd',
  'duel',
  'dylan',
  'dylan-session',
  'dylanlid',
  'earl-grey',
  'easytrieve',
  'ebnf',
  'ec',
  'ecl',
  'eiffel',
  'elixir',
  'elixir-iex-session',
  'elm',
  'emacslisp',
  'embedded-ragel',
  'erb',
  'erlang',
  'erlang-erl-session',
  'evoque',
  'ezhil',
  'factor',
  'fancy',
  'fantom',
  'felix',
  'fish',
  'fortran',
  'fortranfixed',
  'foxpro',
  'fsharp',
  'gap',
  'gas',
  'genshi',
  'genshi-text',
  'gettext-catalog',
  'gherkin',
  'glsl',
  'gnuplot',
  'go',
  'golo',
  'gooddata-cl',
  'gosu',
  'gosu-template',
  'groff',
  'groovy',
  'haml',
  'handlebars',
  'haskell',
  'haxe',
  'hexdump',
  'html',
  'html+cheetah',
  'html+django/jinja',
  'html+evoque',
  'html+genshi',
  'html+handlebars',
  'html+lasso',
  'html+mako',
  'html+myghty',
  'html+php',
  'html+smarty',
  'html+twig',
  'html+velocity',
  'http',
  'hxml',
  'hy',
  'hybris',
  'idl',
  'idris',
  'igor',
  'inform-6',
  'inform-6-template',
  'inform-7',
  'ini',
  'io',
  'ioke',
  'irc-logs',
  'isabelle',
  'j',
  'jade',
  'jags',
  'jasmin',
  'java',
  'java-server-page',
  'javascript',
  'javascript+cheetah',
  'javascript+django/jinja',
  'javascript+genshi-text',
  'javascript+lasso',
  'javascript+mako',
  'javascript+mozpreproc',
  'javascript+myghty',
  'javascript+php',
  'javascript+ruby',
  'javascript+smarty',
  'jcl',
  'json',
  'json-ld',
  'julia',
  'julia-console',
  'kal',
  'kconfig',
  'koka',
  'kotlin',
  'lasso',
  'lean',
  'lesscss',
  'lighttpd-configuration-file',
  'limbo',
  'liquid',
  'literate-agda',
  'literate-cryptol',
  'literate-haskell',
  'literate-idris',
  'livescript',
  'llvm',
  'logos',
  'logtalk',
  'lsl',
  'lua',
  'makefile',
  'mako',
  'maql',
  'mask',
  'mason',
  'mathematica',
  'matlab',
  'matlab-session',
  'minid',
  'modelica',
  'modula-2',
  'moinmoin/trac-wiki-markup',
  'monkey',
  'moocode',
  'moonscript',
  'mozhashpreproc',
  'mozpercentpreproc',
  'mql',
  'mscgen',
  'msdos-session',
  'mupad',
  'mxml',
  'myghty',
  'mysql',
  'nasm',
  'nemerle',
  'nesc',
  'newlisp',
  'newspeak',
  'nginx-configuration-file',
  'nimrod',
  'nit',
  'nix',
  'nsis',
  'numpy',
  'objdump',
  'objdump-nasm',
  'objective-c',
  'objective-c++',
  'objective-j',
  'ocaml',
  'octave',
  'odin',
  'ooc',
  'opa',
  'openedge-abl',
  'pacmanconf',
  'pan',
  'parasail',
  'pawn',
  'perl',
  'perl6',
  'php',
  'pig',
  'pike',
  'pkgconfig',
  'pl/pgsql',
  'postgresql-console-(psql)',
  'postgresql-sql-dialect',
  'postscript',
  'povray',
  'powershell',
  'powershell-session',
  'praat',
  'prolog',
  'properties',
  'protocol-buffer',
  'puppet',
  'pypy-log',
  'python',
  'python-3',
  'python-3.0-traceback',
  'python-console-session',
  'python-traceback',
  'qbasic',
  'qml',
  'qvto',
  'racket',
  'ragel',
  'ragel-in-c-host',
  'ragel-in-cpp-host',
  'ragel-in-d-host',
  'ragel-in-java-host',
  'ragel-in-objective-c-host',
  'ragel-in-ruby-host',
  'raw-token-data',
  'rconsole',
  'rd',
  'rebol',
  'red',
  'redcode',
  'reg',
  'resourcebundle',
  'restructuredtext',
  'rexx',
  'rhtml',
  'roboconf-graph',
  'roboconf-instances',
  'robotframework',
  'rpmspec',
  'rql',
  'rsl',
  'ruby',
  'ruby-irb-session',
  'rust',
  's',
  'sass',
  'scala',
  'scalate-server-page',
  'scaml',
  'scheme',
  'scilab',
  'scss',
  'shen',
  'slim',
  'smali',
  'smalltalk',
  'smarty',
  'snobol',
  'sourcepawn',
  'sparql',
  'sql',
  'sqlite3con',
  'squidconf',
  'stan',
  'standard-ml',
  'supercollider',
  'swift',
  'swig',
  'systemverilog',
  'tads-3',
  'tap',
  'tcl',
  'tcsh',
  'tcsh-session',
  'tea',
  'termcap',
  'terminfo',
  'terraform',
  'tex',
  'text-only',
  'thrift',
  'todotxt',
  'trafficscript',
  'treetop',
  'turtle',
  'twig',
  'typescript',
  'urbiscript',
  'vala',
  'vb.net',
  'vctreestatus',
  'velocity',
  'verilog',
  'vgl',
  'vhdl',
  'viml',
  'x10',
  'xml',
  'xml+cheetah',
  'xml+django/jinja',
  'xml+evoque',
  'xml+lasso',
  'xml+mako',
  'xml+myghty',
  'xml+php',
  'xml+ruby',
  'xml+smarty',
  'xml+velocity',
  'xquery',
  'xslt',
  'xtend',
  'xul+mozpreproc',
  'yaml',
  'yaml+jinja',
  'zephir',
  'unknown'
);

-- Language metadata
create table content_language (
  id sha1 primary key references content(sha1) not null,
  lang languages not null
);

comment on table content_language is 'Language information on a raw content';
comment on column content_language.lang is 'Language information';

-- check which entries of tmp_bytea are missing from content_language
--
-- operates in bulk: 0. swh_mktemp_bytea(), 1. COPY to tmp_bytea,
-- 2. call this function
create or replace function swh_language_missing()
    returns setof sha1
    language plpgsql
as $$
begin
    return query
	(select id::sha1 from tmp_bytea as tmp
	 where not exists
	     (select 1 from content_language as c where c.id = tmp.id));
    return;
end
$$;

COMMENT ON FUNCTION swh_language_missing() IS 'Filter missing content language';


-- add tmp_content_language entries to content_language, skipping duplicates
--
-- operates in bulk: 0. swh_mktemp(content_language), 1. COPY to tmp_content_language,
-- 2. call this function
create or replace function swh_language_add()
    returns void
    language plpgsql
as $$
begin
    insert into content_language (id, lang)
	select id, lang
	from tmp_content_language
        on conflict do nothing;
    return;
end
$$;

COMMENT ON FUNCTION swh_language_add() IS 'Add new content language';
