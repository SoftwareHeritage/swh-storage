-- SWH DB schema upgrade
-- from_version: 92
-- to_version: 93
-- description: Full text search on content_ctags

insert into dbversion(version, release, description)
      values(93, now(), 'Work In Progress');

alter table content_ctags add column searchable_symbol tsvector;

comment on column content_ctags.searchable_symbol is 'Searchable symbol derived from name column';

update content_ctags set searchable_symbol = to_tsvector('english', name);

create trigger content_ctags_tsvectorupdate before insert or update
on content_ctags for each row execute procedure
tsvector_update_trigger(searchable_symbol, 'pg_catalog.english', name);

create index searchable_symbol_idx ON content_ctags USING GIN (searchable_symbol);

create type content_ctags_signature as (
  id sha1,
  name text,
  kind text,
  line bigint,
  lang ctags_languages
);

drop function swh_content_ctags_get();

-- Retrieve list of content ctags from the temporary table.
--
-- operates in bulk: 0. mktemp(tmp_bytea), 1. COPY to tmp_bytea, 2. call this function
create or replace function swh_content_ctags_get()
    returns setof content_ctags_signature
    language plpgsql
as $$
begin
    return query
        select c.id, c.name, c.kind, c.line, c.lang
        from tmp_bytea t
        inner join content_ctags c using(id)
        order by line;
    return;
end
$$;

comment on function swh_content_ctags_get() IS 'List content ctags';

-- Search within ctags content.
--
create or replace function swh_content_ctags_search(expression text)
    returns setof content_ctags_signature
    language sql
as $$
    select id, name, kind, line, lang
    from content_ctags
    where searchable_symbol @@ to_tsquery(expression);
$$;

comment on function swh_content_ctags_search(text) IS 'Search through ctags'' symbols';
