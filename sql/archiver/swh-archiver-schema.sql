-- In order to archive the content of the object storage, add
-- some tables to keep trace of what have already been archived.

create table dbversion
(
  version     int primary key,
  release     timestamptz,
  description text
);

comment on table dbversion is 'Schema update tracking';

INSERT INTO dbversion(version, release, description)
VALUES(8, now(), 'Work In Progress');

CREATE TABLE archive (
  id text PRIMARY KEY
);

comment on table archive is 'Possible archives';
comment on column archive.id is 'Short identifier for the archive';

CREATE TYPE archive_status AS ENUM (
  'missing',
  'ongoing',
  'present',
  'corrupted'
);

comment on type archive_status is 'Status of a given archive';

-- a SHA1 checksum (not necessarily originating from Git)
CREATE DOMAIN sha1 AS bytea CHECK (LENGTH(VALUE) = 20);

-- a bucket for which we count items
CREATE DOMAIN bucket AS bytea CHECK (LENGTH(VALUE) = 2);

CREATE TABLE content_archive (
  content_id sha1 primary key,
  copies jsonb,
  num_present int default null
);

create index on content_archive(num_present);

comment on table content_archive is 'Referencing the status and whereabouts of a content';
comment on column content_archive.content_id is 'content identifier';
comment on column content_archive.copies is 'map archive_id -> { "status": archive_status, "mtime": epoch timestamp }';
comment on column content_archive.num_present is 'Number of copies marked as present (cache updated via trigger)';

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

-- Keep the num_copies cache updated
CREATE FUNCTION update_num_present() RETURNS TRIGGER AS $$
    BEGIN
    NEW.num_present := (select count(*) from jsonb_each(NEW.copies) where value->>'status' = 'present');
    RETURN new;
    END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER update_num_present
    BEFORE INSERT OR UPDATE OF copies ON content_archive
    FOR EACH ROW
    EXECUTE PROCEDURE update_num_present();

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

create trigger update_content_archive_counts
   AFTER INSERT OR UPDATE OR DELETE ON content_archive
   FOR EACH ROW
   EXECUTE PROCEDURE update_content_archive_counts();
