---
--- Software Heritage Data Types
---

create type content_status as enum ('absent', 'visible', 'hidden');
comment on type content_status is 'Content visibility';

-- Types of entities.
--
-- - organization: a root entity, usually backed by a non-profit, a
-- company, or another kind of "association". (examples: Software
-- Heritage, Debian, GNU, GitHub)
--
-- - group_of_entities: used for hierarchies, doesn't need to have a
-- concrete existence. (examples: GNU hosting facilities, Debian
-- hosting facilities, GitHub users, ...)
--
-- - hosting: a hosting facility, can usually be listed to generate
-- other data. (examples: GitHub git hosting, alioth.debian.org,
-- snapshot.debian.org)
--
-- - group_of_persons: an entity representing a group of
-- persons. (examples: a GitHub organization, a Debian team)
--
-- - person: an entity representing a person. (examples:
-- a GitHub user, a Debian developer)
--
-- - project: an entity representing a software project. (examples: a
-- GitHub project, Apache httpd, a Debian source package, ...)
create type entity_type as enum (
  'organization',
  'group_of_entities',
  'hosting',
  'group_of_persons',
  'person',
  'project'
);
comment on type entity_type is 'Entity types';

create type revision_type as enum ('git', 'tar', 'dsc', 'svn');
comment on type revision_type is 'Possible revision types';

create type object_type as enum ('content', 'directory', 'revision', 'release');
comment on type object_type is 'Data object types stored in data model';

create type origin_visit_status as enum (
  'ongoing',
  'full',
  'partial'
);
comment on type origin_visit_status IS 'Possible visit status';

create type languages as enum ( 'abap', 'abnf', 'actionscript',
  'actionscript-3', 'ada', 'adl', 'agda', 'alloy', 'ambienttalk',
  'antlr', 'antlr-with-actionscript-target', 'antlr-with-c#-target',
  'antlr-with-cpp-target', 'antlr-with-java-target',
  'antlr-with-objectivec-target', 'antlr-with-perl-target',
  'antlr-with-python-target', 'antlr-with-ruby-target', 'apacheconf',
  'apl', 'applescript', 'arduino', 'aspectj', 'aspx-cs', 'aspx-vb',
  'asymptote', 'autohotkey', 'autoit', 'awk', 'base-makefile', 'bash',
  'bash-session', 'batchfile', 'bbcode', 'bc', 'befunge',
  'blitzbasic', 'blitzmax', 'bnf', 'boo', 'boogie', 'brainfuck',
  'bro', 'bugs', 'c', 'c#', 'c++', 'c-objdump', 'ca65-assembler',
  'cadl', 'camkes', 'cbm-basic-v2', 'ceylon', 'cfengine3',
  'cfstatement', 'chaiscript', 'chapel', 'cheetah', 'cirru', 'clay',
  'clojure', 'clojurescript', 'cmake', 'cobol', 'cobolfree',
  'coffeescript', 'coldfusion-cfc', 'coldfusion-html', 'common-lisp',
  'component-pascal', 'coq', 'cpp-objdump', 'cpsa', 'crmsh', 'croc',
  'cryptol', 'csound-document', 'csound-orchestra', 'csound-score',
  'css', 'css+django/jinja', 'css+genshi-text', 'css+lasso',
  'css+mako', 'css+mozpreproc', 'css+myghty', 'css+php', 'css+ruby',
  'css+smarty', 'cuda', 'cypher', 'cython', 'd', 'd-objdump',
  'darcs-patch', 'dart', 'debian-control-file', 'debian-sourcelist',
  'delphi', 'dg', 'diff', 'django/jinja', 'docker', 'dtd', 'duel',
  'dylan', 'dylan-session', 'dylanlid', 'earl-grey', 'easytrieve',
  'ebnf', 'ec', 'ecl', 'eiffel', 'elixir', 'elixir-iex-session',
  'elm', 'emacslisp', 'embedded-ragel', 'erb', 'erlang',
  'erlang-erl-session', 'evoque', 'ezhil', 'factor', 'fancy',
  'fantom', 'felix', 'fish', 'fortran', 'fortranfixed', 'foxpro',
  'fsharp', 'gap', 'gas', 'genshi', 'genshi-text', 'gettext-catalog',
  'gherkin', 'glsl', 'gnuplot', 'go', 'golo', 'gooddata-cl', 'gosu',
  'gosu-template', 'groff', 'groovy', 'haml', 'handlebars', 'haskell',
  'haxe', 'hexdump', 'html', 'html+cheetah', 'html+django/jinja',
  'html+evoque', 'html+genshi', 'html+handlebars', 'html+lasso',
  'html+mako', 'html+myghty', 'html+php', 'html+smarty', 'html+twig',
  'html+velocity', 'http', 'hxml', 'hy', 'hybris', 'idl', 'idris',
  'igor', 'inform-6', 'inform-6-template', 'inform-7', 'ini', 'io',
  'ioke', 'irc-logs', 'isabelle', 'j', 'jade', 'jags', 'jasmin',
  'java', 'java-server-page', 'javascript', 'javascript+cheetah',
  'javascript+django/jinja', 'javascript+genshi-text',
  'javascript+lasso', 'javascript+mako', 'javascript+mozpreproc',
  'javascript+myghty', 'javascript+php', 'javascript+ruby',
  'javascript+smarty', 'jcl', 'json', 'json-ld', 'julia',
  'julia-console', 'kal', 'kconfig', 'koka', 'kotlin', 'lasso',
  'lean', 'lesscss', 'lighttpd-configuration-file', 'limbo', 'liquid',
  'literate-agda', 'literate-cryptol', 'literate-haskell',
  'literate-idris', 'livescript', 'llvm', 'logos', 'logtalk', 'lsl',
  'lua', 'makefile', 'mako', 'maql', 'mask', 'mason', 'mathematica',
  'matlab', 'matlab-session', 'minid', 'modelica', 'modula-2',
  'moinmoin/trac-wiki-markup', 'monkey', 'moocode', 'moonscript',
  'mozhashpreproc', 'mozpercentpreproc', 'mql', 'mscgen',
  'msdos-session', 'mupad', 'mxml', 'myghty', 'mysql', 'nasm',
  'nemerle', 'nesc', 'newlisp', 'newspeak',
  'nginx-configuration-file', 'nimrod', 'nit', 'nix', 'nsis', 'numpy',
  'objdump', 'objdump-nasm', 'objective-c', 'objective-c++',
  'objective-j', 'ocaml', 'octave', 'odin', 'ooc', 'opa',
  'openedge-abl', 'pacmanconf', 'pan', 'parasail', 'pawn', 'perl',
  'perl6', 'php', 'pig', 'pike', 'pkgconfig', 'pl/pgsql',
  'postgresql-console-(psql)', 'postgresql-sql-dialect', 'postscript',
  'povray', 'powershell', 'powershell-session', 'praat', 'prolog',
  'properties', 'protocol-buffer', 'puppet', 'pypy-log', 'python',
  'python-3', 'python-3.0-traceback', 'python-console-session',
  'python-traceback', 'qbasic', 'qml', 'qvto', 'racket', 'ragel',
  'ragel-in-c-host', 'ragel-in-cpp-host', 'ragel-in-d-host',
  'ragel-in-java-host', 'ragel-in-objective-c-host',
  'ragel-in-ruby-host', 'raw-token-data', 'rconsole', 'rd', 'rebol',
  'red', 'redcode', 'reg', 'resourcebundle', 'restructuredtext',
  'rexx', 'rhtml', 'roboconf-graph', 'roboconf-instances',
  'robotframework', 'rpmspec', 'rql', 'rsl', 'ruby',
  'ruby-irb-session', 'rust', 's', 'sass', 'scala',
  'scalate-server-page', 'scaml', 'scheme', 'scilab', 'scss', 'shen',
  'slim', 'smali', 'smalltalk', 'smarty', 'snobol', 'sourcepawn',
  'sparql', 'sql', 'sqlite3con', 'squidconf', 'stan', 'standard-ml',
  'supercollider', 'swift', 'swig', 'systemverilog', 'tads-3', 'tap',
  'tcl', 'tcsh', 'tcsh-session', 'tea', 'termcap', 'terminfo',
  'terraform', 'tex', 'text-only', 'thrift', 'todotxt',
  'trafficscript', 'treetop', 'turtle', 'twig', 'typescript',
  'urbiscript', 'vala', 'vb.net', 'vctreestatus', 'velocity',
  'verilog', 'vgl', 'vhdl', 'viml', 'x10', 'xml', 'xml+cheetah',
  'xml+django/jinja', 'xml+evoque', 'xml+lasso', 'xml+mako',
  'xml+myghty', 'xml+php', 'xml+ruby', 'xml+smarty', 'xml+velocity',
  'xquery', 'xslt', 'xtend', 'xul+mozpreproc', 'yaml', 'yaml+jinja',
  'zephir', 'unknown'
);
comment on type languages is 'Languages recognized by language indexer';

create type ctags_languages as enum ( 'Ada', 'AnsiblePlaybook', 'Ant',
  'Asm', 'Asp', 'Autoconf', 'Automake', 'Awk', 'Basic', 'BETA', 'C',
  'C#', 'C++', 'Clojure', 'Cobol', 'CoffeeScript [disabled]', 'CSS',
  'ctags', 'D', 'DBusIntrospect', 'Diff', 'DosBatch', 'DTS', 'Eiffel',
  'Erlang', 'Falcon', 'Flex', 'Fortran', 'gdbinit [disabled]',
  'Glade', 'Go', 'HTML', 'Iniconf', 'Java', 'JavaProperties',
  'JavaScript', 'JSON', 'Lisp', 'Lua', 'M4', 'Make', 'man [disabled]',
  'MatLab', 'Maven2', 'Myrddin', 'ObjectiveC', 'OCaml', 'OldC
  [disabled]', 'OldC++ [disabled]', 'Pascal', 'Perl', 'Perl6', 'PHP',
  'PlistXML', 'pod', 'Protobuf', 'Python', 'PythonLoggingConfig', 'R',
  'RelaxNG', 'reStructuredText', 'REXX', 'RpmSpec', 'Ruby', 'Rust',
  'Scheme', 'Sh', 'SLang', 'SML', 'SQL', 'SVG', 'SystemdUnit',
  'SystemVerilog', 'Tcl', 'Tex', 'TTCN', 'Vera', 'Verilog', 'VHDL',
  'Vim', 'WindRes', 'XSLT', 'YACC', 'Yaml', 'YumRepo', 'Zephir'
);
comment on type ctags_languages is 'Languages recognized by ctags indexer';
