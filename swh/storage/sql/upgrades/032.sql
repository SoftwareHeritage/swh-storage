-- SWH DB schema upgrade
-- from_version: 30
-- to_version: 32
-- description: Reading data improvement on directory and release data.

insert into dbversion(version, release, description)
      values(32, now(), 'Work In Progress');

CREATE FUNCTION swh_mktemp_release_get()
    returns void
    language sql
as $$
    create temporary table tmp_release_get(
      id sha1_git primary key
    ) on commit drop;
$$;

-- Detailed entry for a release
CREATE TYPE release_entry AS
(
  id          sha1_git,
  revision    sha1_git,
  date        timestamptz,
  date_offset smallint,
  name        text,
  comment     bytea,
  synthetic   boolean,
  author_name bytea,
  author_email bytea
);

-- Detailed entry for release
CREATE OR REPLACE FUNCTION swh_release_get()
    returns setof release_entry
    language plpgsql
as $$
begin
    return query
        select r.id, r.revision, r.date, r.date_offset, r.name, r.comment,
               r.synthetic, p.name as author_name, p.email as author_email
        from tmp_release_get t
        inner join release r on t.id = r.id
        inner join person p on p.id = r.author;
    return;
end
$$;

DROP TYPE IF EXISTS directory_entry CASCADE;

-- a directory listing entry with all the metadata
--
-- can be used to list a directory, and retrieve all the data in one go.
CREATE TYPE directory_entry AS
(
  dir_id   sha1_git,     -- id of the parent directory
  type     directory_entry_type,  -- type of entry
  target   sha1_git,     -- id of target
  name     unix_path,    -- path name, relative to containing dir
  perms    file_perms,   -- unix-like permissions
  status   content_status,  -- visible or absent
  sha1     sha1,            -- content if sha1 if type is not dir
  sha1_git sha1_git,        -- content's sha1 git if type is not dir
  sha256   sha256           -- content's sha256 if type is not dir
);

-- List a single level of directory walked_dir_id
-- FIXME: order by name is not correct. For git, we need to order by
-- lexicographic order but as if a trailing / is present in directory
-- name
create or replace function swh_directory_walk_one(walked_dir_id sha1_git)
    returns setof directory_entry
    language sql
    stable
as $$
    with dir as (
	select id as dir_id, dir_entries, file_entries, rev_entries
	from directory
	where id = walked_dir_id),
    ls_d as (select dir_id, unnest(dir_entries) as entry_id from dir),
    ls_f as (select dir_id, unnest(file_entries) as entry_id from dir),
    ls_r as (select dir_id, unnest(rev_entries) as entry_id from dir)
    (select dir_id, 'dir'::directory_entry_type as type,
            e.target, e.name, e.perms, NULL::content_status,
            NULL::sha1, NULL::sha1_git, NULL::sha256
     from ls_d
     left join directory_entry_dir e on ls_d.entry_id = e.id)
    union
    (select dir_id, 'file'::directory_entry_type as type,
            e.target, e.name, e.perms, c.status,
            c.sha1, c.sha1_git, c.sha256
     from ls_f
     left join directory_entry_file e on ls_f.entry_id = e.id
     left join content c on e.target = c.sha1_git)
    union
    (select dir_id, 'rev'::directory_entry_type as type,
            e.target, e.name, e.perms, NULL::content_status,
            NULL::sha1, NULL::sha1_git, NULL::sha256
     from ls_r
     left join directory_entry_rev e on ls_r.entry_id = e.id)
    order by name;
$$;

-- List recursively the content of a directory
create or replace function swh_directory_walk(walked_dir_id sha1_git)
    returns setof directory_entry
    language sql
    stable
as $$
    with recursive entries as (
        select dir_id, type, target, name, perms, status, sha1, sha1_git,
               sha256
        from swh_directory_walk_one(walked_dir_id)
        union all
        select dir_id, type, target, (dirname || '/' || name)::unix_path as name,
               perms, status, sha1, sha1_git, sha256
        from (select (swh_directory_walk_one(dirs.target)).*, dirs.name as dirname
              from (select target, name from entries where type = 'dir') as dirs) as with_parent
    )
    select dir_id, type, target, name, perms, status, sha1, sha1_git, sha256
    from entries
$$;
