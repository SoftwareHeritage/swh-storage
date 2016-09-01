-- SWH DB schema upgrade
-- from_version: 78
-- to_version: 79
-- description: Add the means to list the revision's directory

insert into dbversion(version, release, description)
      values(79, now(), 'Work In Progress');

drop function swh_content_find_occurrence(sha1);
drop type content_occurrence;

create type occurrence_visit as (
  origin      bigint,
  visit       bigint,
  branch      bytea,
  target      sha1_git,
  target_type object_type,
  object_id   bigint
);
COMMENT ON TYPE occurrence_visit IS 'Occurrence visit information';

create or replace function swh_occurrence_history_per_origin(origin_id bigint,
                                                             type_target object_type default 'revision')
    returns setof occurrence_visit
    language sql
as $$
    select origin, unnest(visits) as visit, branch, target, target_type, object_id
    from occurrence_history
    where origin=origin_id and target_type=type_target
$$;

COMMENT ON FUNCTION swh_occurrence_history_per_origin(bigint, object_type) IS 'List occurrence_history per origin and target_type';

create type content_provenance as (
    origin_url  text,
    origin_type text,
    date        timestamptz,
    branch      bytea,
    target      sha1_git,
    target_type object_type,
    path        unix_path
);

COMMENT ON TYPE content_provenance IS 'Provenance information on content';

create or replace function swh_content_find_provenance(content_id sha1_git)
    returns setof content_provenance
    language sql
as $$
    with content_partial_provenance_info as (
        select ccr.content, ccr.revision, cro.origin, cro.visit, ccr.path
        from cache_content_revision ccr
        inner join cache_revision_origin cro using(revision)
        where ccr.content=content_id
    )
    select ori.type as origin_type, ori.url as origin_url, ov.date, occ.branch, occ.target, occ.target_type, info.path
    from content_partial_provenance_info as info
    inner join origin ori on ori.id = info.origin
    inner join origin_visit ov on ori.id = ov.origin and ov.visit = info.visit
    inner join lateral swh_occurrence_history_per_origin(ori.id) as occ on (occ.origin=ov.origin and occ.visit=ov.visit);
$$;

COMMENT ON FUNCTION swh_content_find_provenance(sha1_git) IS 'Find a provenance information for a content';
