-- SWH DB schema upgrade
-- from_version: 69
-- to_version: 70
-- description: Add cache table

insert into dbversion(version, release, description)
      values(70, now(), 'Work In Progress');

-- Cache to permit to crawl an origin and see the latest revision seen
-- The data in that table can be lost.
-- Its intended use is a simple cache to ease the crawling of huge
-- source, e.g.:
-- - load a huge svn, cvs, ... repository (e.g
-- - http://svn.apache.org/repos/asf is ~1.75M revisions)
--
create table origin_revision_cache
(
  id        bigserial primary key,
  origin    bigint references origin(id),
  revision  sha1_git references revision(id),
  metadata  jsonb -- extra metadata (tarball checksums, extra commit information, etc...)
);

create index on origin_revision_cache(id);
create index on origin_revision_cache(origin, revision);
