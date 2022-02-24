-- SWH DB schema upgrade
-- from_version: 18
-- to_version: 19
-- description: improve performance of swh_{directory,revision,release}_missing

insert into dbversion(version, release, description)
      values(19, now(), 'Work In Progress');


create or replace function swh_directory_missing()
    returns setof sha1_git
    language plpgsql
as $$
begin
    return query
	select id from tmp_directory
	where not exists (
	    select 1 from directory d
	    where d.id = id);
    return;
end
$$;

create or replace function swh_revision_missing()
    returns setof sha1_git
    language plpgsql
as $$
begin
    return query
        select id from tmp_revision
	where not exists (
	    select 1 from revision r
	    where r.id = id);
    return;
end
$$;

create or replace function swh_release_missing()
    returns setof sha1_git
    language plpgsql
as $$
begin
    return query
        select id from tmp_release
	where not exists (
	select 1 from release r
	where r.id = id);
    return;
end
$$;
