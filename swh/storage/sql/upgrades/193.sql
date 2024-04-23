-- SWH DB schema upgrade
-- from_version: 192
-- to_version: 193
-- description: Remove the only_masking db flavor
--              This will fail if the db actually uses the only_masking flavor

-- cannot remove a value from a enum, so we have to recreate it


-- (re)create the database flavor type
create type database_flavor_new as enum (
  'default', -- default: full index availability for deduplication and read queries
  'mirror', -- mirror: reduced indexes to allow for out of order insertions
  'read_replica' -- read replica: minimal indexes to allow read queries
);
comment on type database_flavor_new is 'Flavor of the current database';
-- and the flavor database
create table dbflavor_new (
  flavor      database_flavor_new,
  single_row  char(1) primary key default 'x',
  check       (single_row = 'x')
);
comment on table dbflavor_new is 'Database flavor storage';
comment on column dbflavor_new.flavor is 'Database flavor currently deployed';
comment on column dbflavor_new.single_row is 'Bogus column to force the table to have a single row';

-- fill dbflavor_new from dbflavor

insert into dbflavor_new select cast(flavor::text AS database_flavor_new) from dbflavor;

drop function if exists swh_get_dbflavor;


-- then drop old versions of the flavor table and type
drop table dbflavor;
drop type database_flavor;

-- move flavor stuff to alt names
alter type database_flavor_new rename to database_flavor;
alter table dbflavor_new rename to dbflavor;

create or replace function swh_get_dbflavor() returns database_flavor language sql stable as $$
  select coalesce((select flavor from dbflavor), 'default');
$$;

comment on function swh_get_dbflavor is 'Get the flavor of the database currently deployed';
