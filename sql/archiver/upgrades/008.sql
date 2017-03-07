-- SWH DB schema upgrade
-- from_version: 7
-- to_version: 8
-- description: Fix silly bug in update_content_archive_counts

INSERT INTO dbversion(version, release, description)
VALUES(8, now(), 'Work In Progress');

-- keep the content_archive_counts updated
CREATE OR REPLACE FUNCTION update_content_archive_counts() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
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
        CONTINUE WHEN copies.old_status is not distinct from copies.new_status OR
                      (copies.old_status != 'present' AND copies.new_status != 'present');

        update content_archive_counts cac
            set count = count + (case when copies.old_status = 'present' then -1 else 1 end)
            where archive = copies.archive and bucket = content_bucket;
      END LOOP;
      return null;
    END;
$$;
