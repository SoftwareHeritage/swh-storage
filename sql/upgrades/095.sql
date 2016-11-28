-- SWH DB schema upgrade
-- from_version: 94
-- to_version: 95
-- description: Fix edge case on content_ctags search function

insert into dbversion(version, release, description)
      values(95, now(), 'Work In Progress');

create or replace function swh_content_ctags_search(expression text, l integer, o integer)
    returns setof content_ctags_signature
    language plpgsql
as $$
begin
    return query
        select id, name, kind, line, lang
        from content_ctags
        where searchable_symbol @@ to_tsquery(expression)
        order by id
        limit l
        offset o;
exception
    when sqlstate '42000' then  -- syntax error
        raise exception 'Bad syntax in expression ''%''', expression;
end
$$;
