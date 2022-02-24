-- SWH DB schema upgrade
-- from_version: 36
-- to_version: 37
-- description: Permit directory entry lookup per path

insert into dbversion(version, release, description)
      values(37, now(), 'Work In Progress');

-- Find a directory entry by its path
create or replace function swh_find_directory_entry_by_path(walked_dir_id sha1_git,
                                                            dir_or_content_path unix_path)
    returns directory_entry
    language sql
    stable
as $$
    select dir_id, type, target, name, perms, status, sha1, sha1_git, sha256
    from swh_directory_walk(walked_dir_id)
    where name = dir_or_content_path
$$;
