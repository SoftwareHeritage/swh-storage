-- database flavor
create type database_flavor as enum (
  'default', -- default: full index availability for deduplication and read queries
  'mirror', -- mirror: reduced indexes to allow for out of order insertions
  'read_replica' -- read replica: minimal indexes to allow read queries
);
comment on type database_flavor is 'Flavor of the current database';

create table dbflavor (
  flavor      database_flavor,
  single_row  char(1) primary key default 'x',
  check       (single_row = 'x')
);
comment on table dbflavor is 'Database flavor storage';
comment on column dbflavor.flavor is 'Database flavor currently deployed';
comment on column dbflavor.single_row is 'Bogus column to force the table to have a single row';

create or replace function swh_get_dbflavor() returns database_flavor language sql stable as $$
  select coalesce((select flavor from dbflavor), 'default');
$$;

comment on function swh_get_dbflavor is 'Get the flavor of the database currently deployed';
