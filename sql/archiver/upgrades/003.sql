-- SWH DB schema upgrade
-- from_version: 2
-- to_version: 3
-- description: Add a 'num_present' cache column into the archive_content status


alter table content_archive add column num_present int default null;
comment on column content_archive.num_present is 'Number of copies marked as present (cache updated via trigger)';

create index concurrently on content_archive(num_present);

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

