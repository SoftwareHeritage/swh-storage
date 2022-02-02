-- SWH DB schema upgrade
-- from_version: 16
-- to_version: 17
-- description: improve swh_content_missing, generic shw_directory_entry_add,
--   LANGUAGE sql where possible, better API for swh_content_find_*

create or replace function swh_content_missing()
    returns setof content_signature
    language plpgsql
as $$
begin
    return query
	(select sha1, sha1_git, sha256 from tmp_content as tmp
	 where not exists
	     (select 1 from content as c where c.sha1 = tmp.sha1))
	union
	(select sha1, sha1_git, sha256 from tmp_content as tmp
	 where not exists
	     (select 1 from content as c where c.sha1_git = tmp.sha1_git))
	union
	(select sha1, sha1_git, sha256 from tmp_content as tmp
	 where not exists
	     (select 1 from content as c where c.sha256 = tmp.sha256));
    return;
end
$$;

create or replace function swh_directory_entry_add(typ directory_entry_type)
    returns void
    language plpgsql
as $$
begin
    execute format('
    insert into directory_entry_%1$s (target, name, perms, atime, mtime, ctime)
    select distinct t.target, t.name, t.perms, t.atime, t.mtime, t.ctime
    from tmp_directory_entry_%1$s t
    where not exists (
    select 1
    from directory_entry_%1$s i
    where t.target = i.target and t.name = i.name and t.perms = i.perms and
       t.atime is not distinct from i.atime and
       t.mtime is not distinct from i.mtime and
       t.ctime is not distinct from i.ctime)
   ', typ);

    execute format('
    with new_entries as (
	select t.dir_id, array_agg(i.id) as entries
	from tmp_directory_entry_%1$s t
	inner join directory_entry_%1$s i
	on t.target = i.target and t.name = i.name and t.perms = i.perms and
	   t.atime is not distinct from i.atime and
	   t.mtime is not distinct from i.mtime and
	   t.ctime is not distinct from i.ctime
	group by t.dir_id
    )
    update directory as d
    set %1$s_entries = new_entries.entries
    from new_entries
    where d.id = new_entries.dir_id
    ', typ);

    return;
end
$$;

drop function swh_directory_entry_dir_add ();
drop function swh_directory_entry_file_add ();
drop function swh_directory_entry_rev_add ();

create or replace function swh_mktemp_revision()
    returns void
    language sql
as $$
    create temporary table tmp_revision (
        like revision including defaults,
        author_name text not null default '',
        author_email text not null default '',
        committer_name text not null default '',
        committer_email text not null default ''
    ) on commit drop;
    alter table tmp_revision drop column author;
    alter table tmp_revision drop column committer;
$$;

create or replace function swh_mktemp_release()
    returns void
    language sql
as $$
    create temporary table tmp_release (
        like release including defaults,
        author_name text not null default '',
        author_email text not null default ''
    ) on commit drop;
    alter table tmp_release drop column author;
$$;

create or replace function swh_directory_walk_one(walked_dir_id sha1_git)
    returns setof directory_entry
    language sql
as $$
    with dir as (
	select id as dir_id, dir_entries, file_entries, rev_entries
	from directory
	where id = walked_dir_id),
    ls_d as (select dir_id, unnest(dir_entries) as entry_id from dir),
    ls_f as (select dir_id, unnest(file_entries) as entry_id from dir),
    ls_r as (select dir_id, unnest(rev_entries) as entry_id from dir)
    (select dir_id, 'dir'::directory_entry_type as type,
	    target, name, perms, atime, mtime, ctime
     from ls_d
     left join directory_entry_dir d on ls_d.entry_id = d.id)
    union
    (select dir_id, 'file'::directory_entry_type as type,
	    target, name, perms, atime, mtime, ctime
     from ls_f
     left join directory_entry_file d on ls_f.entry_id = d.id)
    union
    (select dir_id, 'rev'::directory_entry_type as type,
	    target, name, perms, atime, mtime, ctime
     from ls_r
     left join directory_entry_rev d on ls_r.entry_id = d.id)
    order by name;
$$;

create or replace function swh_revision_list(root_revision sha1_git)
    returns setof sha1_git
    language sql
as $$
    with recursive rev_list(id) as (
	(select id from revision where id = root_revision)
	union
	(select parent_id
	 from revision_history as h
	 join rev_list on h.id = rev_list.id)
    )
    select * from rev_list;
$$;

create or replace function swh_revision_log(root_revision sha1_git)
    returns setof revision_log_entry
    language sql
as $$
    select revision.id, date, date_offset,
	committer_date, committer_date_offset,
	type, directory, message,
	author.name as author_name, author.email as author_email,
	committer.name as committer_name, committer.email as committer_email
    from swh_revision_list(root_revision) as rev_list
    join revision on revision.id = rev_list
    join person as author on revision.author = author.id
    join person as committer on revision.committer = committer.id;
$$;

create or replace function swh_content_find_directory(content_id sha1)
    returns content_dir
    language sql
as $$
    with recursive path as (
	(select dir.id as dir_id, dir_entry_f.name as name, 0 as depth
	 from directory_entry_file as dir_entry_f
	 join content on content.sha1_git = dir_entry_f.target
	 join directory as dir on dir.file_entries @> array[dir_entry_f.id]
	 where content.sha1 = content_id
	 limit 1)
	union all
	(select dir.id as dir_id,
		(dir_entry_d.name || '/' || path.name)::unix_path as name,
		path.depth + 1
	 from path
	 join directory_entry_dir as dir_entry_d on dir_entry_d.target = path.dir_id
	 join directory as dir on dir.dir_entries @> array[dir_entry_d.id]
	 limit 1)
    )
    select dir_id, name from path order by depth desc limit 1;
$$;

create or replace function swh_content_find_occurrence(content_id sha1)
    returns content_occurrence
    language plpgsql
as $$
declare
    dir content_dir;
    rev sha1_git;
    occ occurrence%ROWTYPE;
    coc content_occurrence;
begin
    select * from swh_content_find_directory(content_id)
	into dir;
    if not found then return null; end if;

    select id from revision where directory = dir.directory
	limit 1
	into rev;
    if not found then return null; end if;

    select * from swh_revision_find_occurrence(rev)
	into occ;
    if not found then return null; end if;

    select origin.type, origin.url, occ.branch, rev, dir.path
    from origin
    where origin.id = occ.origin
    into coc;

    return coc;
end
$$;
