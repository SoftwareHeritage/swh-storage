-- SWH DB schema upgrade
-- from_version: 118
-- to_version: 119
-- description: Drop unused functions using temporary tables

insert into dbversion(version, release, description)
      values(119, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_content_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    insert into content (sha1, sha1_git, sha256, blake2s256, length, status)
        select distinct sha1, sha1_git, sha256, blake2s256, length, status from tmp_content;
    return;
end
$$;

DROP FUNCTION swh_content_missing_per_sha1();

DROP FUNCTION swh_object_find_by_sha1_git();

DROP FUNCTION swh_content_missing();

DROP FUNCTION swh_release_get();

DROP FUNCTION swh_release_missing();

DROP FUNCTION swh_revision_get();

DROP FUNCTION swh_revision_missing();

DROP FUNCTION swh_mktemp_bytea();

DROP TYPE object_found;

