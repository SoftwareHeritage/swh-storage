-- SWH DB schema upgrade
-- from_version: 5
-- to_version: 6
-- description: Create a bucketed count of contents in the archive.

INSERT INTO dbversion(version, release, description)
VALUES(6, now(), 'Work In Progress');

-- a bucket for which we count items
CREATE DOMAIN bucket AS bytea CHECK (LENGTH(VALUE) = 2);

CREATE TABLE content_archive_counts (
  archive text not null references archive(id),
  bucket bucket not null,
  count bigint,
  primary key (archive, bucket)
);

comment on table content_archive_counts is 'Bucketed count of archive contents';
comment on column content_archive_counts.archive is 'the archive for which we''re counting';
comment on column content_archive_counts.bucket is 'the bucket of items we''re counting';
comment on column content_archive_counts.count is 'the number of items counted in the given bucket';


CREATE FUNCTION count_copies(from_id bytea, to_id bytea) returns void language sql as $$
    with sample as (
        select content_id, copies from content_archive
        where content_id > from_id and content_id <= to_id
    ), data as (
        select substring(content_id from 19) as bucket, jbe.key as archive
        from sample
        join lateral jsonb_each(copies) jbe on true
        where jbe.value->>'status' = 'present'
    ), bucketed as (
        select bucket, archive, count(*) as count
        from data
        group by bucket, archive
    ) update content_archive_counts cac set
        count = cac.count + bucketed.count
      from bucketed
      where cac.archive = bucketed.archive and cac.bucket = bucketed.bucket;
$$;

comment on function count_copies(bytea, bytea) is 'Count the objects between from_id and to_id, add the results to content_archive_counts';

CREATE FUNCTION init_content_archive_counts() returns void language sql as $$
    insert into content_archive_counts (
        select id, decode(lpad(to_hex(bucket), 4, '0'), 'hex')::bucket as bucket, 0 as count
        from archive join lateral generate_series(0, 65535) bucket on true
    ) on conflict (archive, bucket) do nothing;
$$;

comment on function init_content_archive_counts() is 'Initialize the content archive counts for the registered archives';

-- keep the content_archive_counts updated
CREATE FUNCTION update_content_archive_counts() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
    DECLARE
        content_id sha1;
        content_bucket bucket;
        copies record;
        old_row content_archive;
        new_row content_archive;
    BEGIN
      -- default values for old or new row depending on trigger type
      if tg_op = 'INSERT' then
          old_row := (null::sha1, '{}'::jsonb, 0);
      else
          old_row := old;
      end if;
      if tg_op = 'DELETE' then
          new_row := (null::sha1, '{}'::jsonb, 0);
      else
          new_row := new;
      end if;

      -- get the content bucket
      content_id := coalesce(old_row.content_id, new_row.content_id);
      content_bucket := substring(content_id from 19)::bucket;

      -- compare copies present in old and new row for each archive type
      FOR copies IN
        select coalesce(o.key, n.key) as archive, o.value->>'status' as old_status, n.value->>'status' as new_status
            from jsonb_each(old_row.copies) o full outer join lateral jsonb_each(new_row.copies) n on o.key = n.key
      LOOP
        -- the count didn't change
        CONTINUE WHEN copies.old_status is distinct from copies.new_status OR
                      (copies.old_status != 'present' AND copies.new_status != 'present');

        update content_archive_counts cac
            set count = count + (case when copies.old_status = 'present' then -1 else 1 end)
            where archive = copies.archive and bucket = content_bucket;
      END LOOP;
      return null;
    END;
$$;

create trigger update_content_archive_counts
   AFTER INSERT OR UPDATE OR DELETE ON content_archive
   FOR EACH ROW
   EXECUTE PROCEDURE update_content_archive_counts();
