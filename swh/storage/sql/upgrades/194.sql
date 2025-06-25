-- SWH DB schema upgrade
-- from_version: 193
-- to_version: 194
-- description: Allow hash colliding objects in storage

select swh_get_dbflavor() != 'read_replica' as dbflavor_does_deduplication \gset

create index concurrently content_sha1_idx on content(sha1);
create unique index concurrently content_pkey_multi on content(sha256, sha1, sha1_git, blake2s256);
alter table content drop constraint content_pkey;
alter table content add primary key using index content_pkey_multi;
alter index content_pkey_multi rename to content_pkey;

\if :dbflavor_does_deduplication
   create index concurrently content_sha1_git_new on content(sha1_git);
   drop index if exists content_sha1_git_idx;
   alter index content_sha1_git_new rename to content_sha1_git_idx;
\endif

drop index if exists content_sha256_idx;

drop function if exists swh_content_add();
