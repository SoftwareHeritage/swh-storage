-- require being Postgres super user

create extension if not exists btree_gist;
create extension if not exists pgcrypto;
create extension if not exists pg_trgm;

create or replace language plpgsql;
