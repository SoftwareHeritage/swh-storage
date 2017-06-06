create or replace function swh_mktemp_content()
    returns void
    language plpgsql
as $$
  begin
    create temporary table tmp_content (
        sha1 sha1 not null
    ) on commit drop;
    return;
  end
$$;

create or replace function swh_content_copies_from_temp(archive_names text[])
    returns void
    language plpgsql
as $$
  begin
    with existing_content_ids as (
        select id
        from content
        inner join tmp_content on content.sha1 = tmp.sha1
    ), created_content_ids as (
        insert into content (sha1)
        select sha1 from tmp_content
        on conflict do nothing
        returning id
    ), content_ids as (
        select * from existing_content_ids
        union all
        select * from created_content_ids
    ), archive_ids as (
        select id from archive
        where name = any(archive_names)
    ) insert into content_copies (content_id, archive_id, mtime, status)
    select content_ids.id, archive_ids.id, now(), 'present'
    from content_ids cross join archive_ids
    on conflict (content_id, archive_id) do update
      set mtime = excluded.mtime, status = excluded.status;
  end
$$;
