-- SWH DB schema upgrade
-- from_version: 81
-- to_version: 82
-- description: refactor cache_content_revision

insert into dbversion(version, release, description)
      values(82, now(), 'Work In Progress');

drop function swh_cache_content_revision_add(revision_id sha1_git);
drop function swh_content_find_provenance(content_id sha1_git);

DROP TABLE cache_content_revision;

create table cache_content_revision (
    content         sha1_git not null,
    blacklisted     boolean default false,
    revision_paths  bytea[][]
);

CREATE TABLE cache_content_revision_processed (
	revision sha1_git NOT NULL
);

CREATE OR REPLACE FUNCTION swh_cache_content_revision_add(revision_id sha1_git) RETURNS void
    LANGUAGE plpgsql
    AS $$
declare
  rev sha1_git;
begin
    select revision
        from cache_content_revision_processed
        where revision=revision_id
        into rev;

    if rev is NULL then

      insert into cache_content_revision_processed (revision) VALUES (revision_id);

      insert into cache_content_revision
          select sha1_git as content, false as blacklisted, array_agg(ARRAY[revision_id::bytea, name::bytea]) as revision_paths
          from swh_directory_walk((select directory from revision where id=revision_id))
          where type='file'
          group by sha1_git
      on conflict (content) do update
          set revision_paths = cache_content_revision.revision_paths || EXCLUDED.revision_paths
          where cache_content_revision.blacklisted = false;
      return;

    else
      return;
    end if;
end
$$;

COMMENT ON FUNCTION swh_cache_content_revision_add(revision_id sha1_git) IS 'Cache the specified revision directory contents into cache_content_revision';

CREATE OR REPLACE FUNCTION swh_content_find_provenance(content_id sha1_git) RETURNS SETOF content_provenance
    LANGUAGE sql
    AS $$
    with subscripted_paths as (
        select content, revision_paths, generate_subscripts(revision_paths, 1) as s
        from cache_content_revision
        where content = content_id
    ),
    cleaned_up_contents as (
        select content, revision_paths[s][1]::sha1_git as revision, revision_paths[s][2]::unix_path as path
        from subscripted_paths
    )
    select cuc.content, cuc.revision, cro.origin, cro.visit, cuc.path
    from cleaned_up_contents cuc
    inner join cache_revision_origin cro using(revision)
$$;

COMMENT ON FUNCTION swh_content_find_provenance(content_id sha1_git) IS 'Given a content, provide provenance information on it';

ALTER TABLE cache_content_revision
	ADD CONSTRAINT cache_content_revision_pkey PRIMARY KEY (content);

ALTER TABLE cache_content_revision
	ADD CONSTRAINT cache_content_revision_content_fkey FOREIGN KEY (content) REFERENCES content(sha1_git);

ALTER TABLE cache_content_revision_processed
	ADD CONSTRAINT cache_content_revision_processed_pkey PRIMARY KEY (revision);

ALTER TABLE cache_content_revision_processed
	ADD CONSTRAINT cache_content_revision_processed_revision_fkey FOREIGN KEY (revision) REFERENCES revision(id);
