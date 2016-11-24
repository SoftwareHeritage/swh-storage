-- SWH DB schema upgrade
-- from_version: 93
-- to_version: 94
-- description: Full text search on content_ctags with pagination

insert into dbversion(version, release, description)
      values(94, now(), 'Work In Progress');

drop function swh_content_ctags_search(text);

-- Search within ctags content.
--
create or replace function swh_content_ctags_search(expression text, l integer, o integer)
    returns setof content_ctags_signature
    language sql
as $$
    select id, name, kind, line, lang
    from content_ctags
    where searchable_symbol @@ to_tsquery(expression)
    order by id
    limit l
    offset o;
$$;

comment on function swh_content_ctags_search(text, integer, integer) IS 'Search through ctags'' symbols';
