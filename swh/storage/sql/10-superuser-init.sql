-- require being Postgres super user

create extension if not exists btree_gist;
create extension if not exists pgcrypto;
create extension if not exists pg_trgm;

-- courtesy of  Andreas 'ads' Scherbaum in
-- https://andreas.scherbaum.la/blog/archives/346-create-language-if-not-exist.html
create or replace function public.create_plpgsql_language ()
    returns text
    as $$
        create language plpgsql;
        select 'language plpgsql created'::text;
    $$
language 'sql';

select case when
    (select true::boolean
       from pg_language
       where lanname='plpgsql')
    then
      (select 'language already installed'::text)
    else
      (select public.create_plpgsql_language())
    end;

drop function public.create_plpgsql_language ();
