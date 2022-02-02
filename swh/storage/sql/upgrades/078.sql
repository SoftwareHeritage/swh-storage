-- SWH DB schema upgrade
-- from_version: 77
-- to_version: 78
-- description: Add the means to list the revision's directory

insert into dbversion(version, release, description)
      values(78, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_cache_content_revision_add(revision_id sha1_git) RETURNS void
    LANGUAGE plpgsql
    AS $$
declare
  rev sha1_git;
begin
    select revision from cache_content_revision where revision=revision_id limit 1
    into rev;

    if rev is NULL then

      with contents_to_cache as (
          select sha1_git, name
          from swh_directory_walk((select directory from revision where id=revision_id))
          where type='file'
      )
      insert into cache_content_revision (content, revision, path)
      select sha1_git, revision_id, name
      from contents_to_cache;
      return;

    else
      return;
    end if;
end
$$;

COMMENT ON FUNCTION swh_cache_content_revision_add(revision_id sha1_git) IS 'Cache the specified revision directory contents into cache_content_revision';

CREATE OR REPLACE FUNCTION swh_cache_revision_origin_add(origin_id bigint, visit_id bigint) RETURNS SETOF sha1_git
    LANGUAGE plpgsql
    AS $$
declare
    visit_exists bool;
begin
  select true from origin_visit where origin = origin_id and visit = visit_id into visit_exists;

  if not visit_exists then
      return;
  end if;

  visit_exists := null;

  select true from cache_revision_origin where origin = origin_id and visit = visit_id limit 1 into visit_exists;

  if visit_exists then
      return;
  end if;

  return query with new_pointed_revs as (
    select swh_revision_from_target(target, target_type) as id
    from swh_occurrence_by_origin_visit(origin_id, visit_id)
  ),
  old_pointed_revs as (
    select swh_revision_from_target(target, target_type) as id
    from swh_occurrence_by_origin_visit(origin_id,
      (select visit from origin_visit where origin = origin_id and visit < visit_id order by visit desc limit 1))
  ),
  new_revs as (
    select distinct id
    from swh_revision_list(array(select id::bytea from new_pointed_revs where id is not null))
  ),
  old_revs as (
    select distinct id
    from swh_revision_list(array(select id::bytea from old_pointed_revs where id is not null))
  )
  insert into cache_revision_origin (revision, origin, visit)
  select n.id as revision, origin_id, visit_id from new_revs n
    where not exists (
    select 1 from old_revs o
    where o.id = n.id)
   returning revision;
end
$$;

CREATE OR REPLACE FUNCTION swh_occurrence_by_origin_visit(origin_id bigint, visit_id bigint) RETURNS SETOF occurrence
    LANGUAGE sql STABLE
    AS $$
  select origin, branch, target, target_type from occurrence_history
  where origin = origin_id and visit_id = ANY(visits);
$$;

CREATE OR REPLACE FUNCTION swh_revision_from_target(target sha1_git, target_type object_type) RETURNS sha1_git
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_variable
begin
   while target_type = 'release' loop
       select r.target, r.target_type from release r where r.id = target into target, target_type;
   end loop;
   if target_type = 'revision' then
       return target;
   else
       return null;
   end if;
end
$$;

CREATE OR REPLACE FUNCTION swh_revision_walk(revision_id sha1_git) RETURNS SETOF directory_entry
    LANGUAGE sql STABLE
    AS $$
  select dir_id, type, target, name, perms, status, sha1, sha1_git, sha256
  from swh_directory_walk((select directory from revision where id=revision_id))
$$;

COMMENT ON FUNCTION swh_revision_walk(revision_id sha1_git) IS 'Recursively list the revision targeted directory arborescence';
