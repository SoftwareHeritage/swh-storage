-- SWH DB schema upgrade
-- from_version: 77
-- to_version: 78
-- description: Add the means to list the revision's directory

insert into dbversion(version, release, description)
      values(78, now(), 'Work In Progress');

create or replace function swh_cache_content_revision(revision_id sha1_git)
    returns void
    language plpgsql
as $$
declare
  rev sha1_git;
begin
    select revision from cache_content_revision where revision=revision_id
    into rev;

    if rev then
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

COMMENT ON FUNCTION swh_cache_content_revision(sha1_git) IS 'Cache the specified revision directory contents into cache_content_revision';
