-- SWH DB schema upgrade
-- from_version: 95
-- to_version: 96
-- description: Use strict equality for searching symbols

insert into dbversion(version, release, description)
      values(96, now(), 'Work In Progress');

drop trigger if exists content_ctags_tsvectorupdate on content_ctags;

alter table content_ctags drop column searchable_symbol;

drop function swh_content_ctags_search(text, integer, integer);

create or replace function swh_content_ctags_search(
       expression text,
       l integer default 10,
       last_sha1 sha1 default '\x0000000000000000000000000000000000000000')
    returns setof content_ctags_signature
    language sql
as $$
    select id, name, kind, line, lang
    from content_ctags
    where hash_sha1(name) = hash_sha1(expression)
    and id > last_sha1
    order by id
    limit l;
$$;

comment on function swh_content_ctags_search(text, integer, sha1) IS 'Equality search through ctags'' symbols';

create or replace function hash_sha1(text)
    returns text
as $$
    select encode(digest($1, 'sha1'), 'hex')
$$ language sql strict immutable;

create index on content_ctags(hash_sha1(name));
