-- SWH DB schema upgrade
-- from_version: 136
-- to_version: 137
-- description: Add comment columns to all tables

insert into dbversion(version, release, description)
      values(137, now(), 'Work In Progress');

-- comment for columns of dbversion table
comment on table dbversion is 'Details of current db version';
comment on column dbversion.version is 'SQL schema version';
comment on column dbversion.release is 'Version deployment timestamp';
comment on column dbversion.description is 'Release description';

-- comment for columns of content table
comment on table content is 'Checksums of file content which is actually stored externally';
comment on column content.sha1 is 'Content sha1 hash';
comment on column content.sha1_git is 'Git object sha1 hash';
comment on column content.sha256 is 'Content Sha256 hash';
comment on column content.blake2s256 is 'Content blake2s hash';
comment on column content.length is 'Content length';
comment on column content.ctime is 'First seen time';
comment on column content.status is 'Content status (absent, visible, hidden)';
comment on column content.object_id is 'Content identifier';

-- comment for columns of origin table
comment on column origin.id is 'Artifact origin id';
comment on column origin.type is 'Type of origin';
comment on column origin.url is 'URL of origin';

-- comment for columns of skipped_content
comment on table skipped_content is 'Content blobs observed, but not ingested in the archive';
comment on column skipped_content.sha1 is 'Skipped content sha1 hash';
comment on column skipped_content.sha1_git is 'Git object sha1 hash';
comment on column skipped_content.sha256 is 'Skipped content sha256 hash';
comment on column skipped_content.blake2s256 is 'Skipped content blake2s hash';
comment on column skipped_content.length is 'Skipped content length';
comment on column skipped_content.ctime is 'First seen time';
comment on column skipped_content.status is 'Skipped content status (absent, visible, hidden)';
comment on column skipped_content.reason is 'Reason for skipping';
comment on column skipped_content.origin is 'Origin table identifier';
comment on column skipped_content.object_id is 'Skipped content identifier';

-- comment for columns of fetch_history
comment on table fetch_history is 'Log of all origin fetches';
comment on column fetch_history.id is 'Identifier for fetch history';
comment on column fetch_history.origin is 'Origin table identifier';
comment on column fetch_history.date is 'Fetch start time';
comment on column fetch_history.status is 'True indicates successful fetch';
comment on column fetch_history.result is 'Detailed return values, times etc';
comment on column fetch_history.stdout is 'Standard output of fetch operation';
comment on column fetch_history.stderr is 'Standard error of fetch operation';
comment on column fetch_history.duration is 'Time taken to complete fetch, NULL if ongoing';

-- comment for columns of directory
comment on table directory is 'Contents of a directory, synonymous to tree (git)';
comment on column directory.id is 'Git object sha1 hash';
comment on column directory.dir_entries is 'Sub-directories, reference directory_entry_dir';
comment on column directory.file_entries is 'Contained files, reference directory_entry_file';
comment on column directory.rev_entries is 'Mounted revisions, reference directory_entry_rev';
comment on column directory.object_id is 'Short object identifier';

-- comment for columns of directory_entry_dir
comment on table directory_entry_dir is 'Directory entry for directory';
comment on column directory_entry_dir.id is 'Directory identifier';
comment on column directory_entry_dir.target is 'Target directory identifier';
comment on column directory_entry_dir.name is 'Path name, relative to containing directory';
comment on column directory_entry_dir.perms is 'Unix-like permissions';

-- comment for columns of directory_entry_file
comment on table directory_entry_file is 'Directory entry for file';
comment on column directory_entry_file.id is 'File identifier';
comment on column directory_entry_file.target is 'Target file identifier';
comment on column directory_entry_file.name is 'Path name, relative to containing directory';
comment on column directory_entry_file.perms is 'Unix-like permissions';

-- comment for columns of directory_entry_rev
comment on table directory_entry_rev is 'Directory entry for revision';
comment on column directory_entry_dir.id is 'Revision identifier';
comment on column directory_entry_dir.target is 'Target revision in identifier';
comment on column directory_entry_dir.name is 'Path name, relative to containing directory';
comment on column directory_entry_dir.perms is 'Unix-like permissions';

-- comment for columns of person
comment on table person is 'Person referenced in code artifact release metadata';
comment on column person.id is 'Person identifier';
comment on column person.name is 'Name';
comment on column person.email is 'Email';
comment on column person.fullname is 'Full name (raw name)';

-- comment for columns of revision
comment on table revision is 'Revision represents the state of a source code tree at a
 specific point in time';
comment on column revision.id is 'Git id of sha1 checksum';
comment on column revision.date is 'Timestamp when revision was authored';
comment on column revision.date_offset is 'Authored timestamp offset from UTC';
comment on column revision.committer_date is 'Timestamp when revision was committed';
comment on column revision.committer_date_offset is 'Committed timestamp offset from UTC';
comment on column revision.type is 'Possible revision types (''git'', ''tar'', ''dsc'', ''svn'', ''hg'')';
comment on column revision.directory is 'Directory identifier';
comment on column revision.message is 'Revision message';
comment on column revision.author is 'Author identifier';
comment on column revision.committer is 'Committer identifier';
comment on column revision.synthetic is 'true iff revision has been created by Software Heritage';
comment on column revision.metadata is 'extra metadata (tarball checksums, extra commit information, etc...)';
comment on column revision.object_id is 'Object identifier';
comment on column revision.date_neg_utc_offset is 'True indicates -0 UTC offset for author timestamp';
comment on column revision.committer_date_neg_utc_offset is 'True indicates -0 UTC offset for committer timestamp';

-- comment for columns of revision_history
comment on table revision_history is 'Sequence of revision history with parent and position in history';
comment on column revision_history.id is 'Revision history git object sha1 checksum';
comment on column revision_history.parent_id is 'Parent revision git object identifier';
comment on column revision_history.parent_rank is 'Parent position in merge commits, 0-based';

-- comment for columns of snapshot
comment on table snapshot is 'State of a software origin as crawled by Software Heritage';
comment on column snapshot.object_id is 'Internal object identifier';
comment on column snapshot.id is 'Intrinsic snapshot identifier';

-- comment for columns of snapshot_branch
comment on table snapshot_branch is 'Associates branches with objects in Heritage Merkle DAG';
comment on column snapshot_branch.object_id is 'Internal object identifier';
comment on column snapshot_branch.name is 'Branch name';
comment on column snapshot_branch.target is 'Target object identifier';
comment on column snapshot_branch.target_type is 'Target object type';

-- comment for columns of snapshot_branches
comment on table snapshot_branches is 'Mapping between snapshot and their branches';
comment on column snapshot_branches.snapshot_id is 'Snapshot identifier';
comment on column snapshot_branches.branch_id is 'Branch identifier';

-- comment for columns of release
comment on table release is 'Details of a software release, synonymous with
 a tag (git) or version number (tarball)';
comment on column release.id is 'Release git identifier';
comment on column release.target is 'Target git identifier';
comment on column release.date is 'Release timestamp';
comment on column release.date_offset is 'Timestamp offset from UTC';
comment on column release.name is 'Name';
comment on column release.comment is 'Comment';
comment on column release.author is 'Author';
comment on column release.synthetic is 'Indicates if created by Software Heritage';
comment on column release.object_id is 'Object identifier';
comment on column release.target_type is 'Object type (''content'', ''directory'', ''revision'',
 ''release'', ''snapshot'')';
comment on column release.date_neg_utc_offset is 'True indicates -0 UTC offset for release timestamp';

-- comment for columns of object_counts
comment on table object_counts is 'Cache of object counts';
comment on column object_counts.object_type is 'Object type (''content'', ''directory'', ''revision'',
 ''release'', ''snapshot'')';
comment on column object_counts.value is 'Count of objects in the table';
comment on column object_counts.last_update is 'Last update for object count';
comment on column object_counts.single_update is 'standalone (true) or bucketed counts (false)';

-- comment for columns of object_counts_bucketed
comment on table object_counts_bucketed is 'Bucketed count for objects ordered by type';
comment on column object_counts_bucketed.line is 'Auto incremented identifier value';
comment on column object_counts_bucketed.object_type is 'Object type (''content'', ''directory'', ''revision'',
 ''release'', ''snapshot'')';
comment on column object_counts_bucketed.identifier is 'Common identifier for bucketed objects';
comment on column object_counts_bucketed.bucket_start is 'Lower bound (inclusive) for the bucket';
comment on column object_counts_bucketed.bucket_end is 'Upper bound (exclusive) for the bucket';
comment on column object_counts_bucketed.value is 'Count of objects in the bucket';
comment on column object_counts_bucketed.last_update is 'Last update for the object count in this bucket';
