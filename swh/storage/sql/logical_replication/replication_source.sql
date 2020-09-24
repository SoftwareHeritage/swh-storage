-- This file contains the instructions to create a replication source for
-- PostgreSQL logical replication to another database.

CREATE PUBLICATION softwareheritage;

ALTER PUBLICATION softwareheritage ADD TABLE content;
ALTER PUBLICATION softwareheritage ADD TABLE skipped_content;
ALTER PUBLICATION softwareheritage ADD TABLE directory;
ALTER PUBLICATION softwareheritage ADD TABLE directory_entry_file;
ALTER PUBLICATION softwareheritage ADD TABLE directory_entry_dir;
ALTER PUBLICATION softwareheritage ADD TABLE directory_entry_rev;
ALTER PUBLICATION softwareheritage ADD TABLE person;
ALTER PUBLICATION softwareheritage ADD TABLE revision;
ALTER PUBLICATION softwareheritage ADD TABLE revision_history;
ALTER PUBLICATION softwareheritage ADD TABLE release;
ALTER PUBLICATION softwareheritage ADD TABLE snapshot;
ALTER PUBLICATION softwareheritage ADD TABLE snapshot_branch;
ALTER PUBLICATION softwareheritage ADD TABLE snapshot_branches;
ALTER PUBLICATION softwareheritage ADD TABLE origin;
ALTER PUBLICATION softwareheritage ADD TABLE origin_visit;
ALTER PUBLICATION softwareheritage ADD TABLE origin_visit_status;
ALTER PUBLICATION softwareheritage ADD TABLE metadata_fetcher;
ALTER PUBLICATION softwareheritage ADD TABLE metadata_authority;
ALTER PUBLICATION softwareheritage ADD TABLE raw_extrinsic_metadata;
ALTER PUBLICATION softwareheritage ADD TABLE object_counts;
