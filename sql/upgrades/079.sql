-- SWH DB schema upgrade
-- from_version: 78
-- to_version: 79
-- description: Given a content, provide provenance information for it.

insert into dbversion(version, release, description)
      values(79, now(), 'Work In Progress');

drop function swh_content_find_occurrence(sha1);
drop type content_occurrence;

create type content_provenance as (
    content  sha1_git,
    revision sha1_git,
    origin   bigint,
    visit    bigint,
    path     unix_path
);

COMMENT ON TYPE content_provenance IS 'Provenance information on content';

create or replace function swh_content_find_provenance(content_id sha1_git)
    returns setof content_provenance
    language sql
as $$
    select ccr.content, ccr.revision, cro.origin, cro.visit, ccr.path
    from cache_content_revision ccr
    inner join cache_revision_origin cro using(revision)
    where ccr.content=content_id
$$;

COMMENT ON FUNCTION swh_content_find_provenance(sha1_git) IS 'Given a content, provide provenance information on it';
