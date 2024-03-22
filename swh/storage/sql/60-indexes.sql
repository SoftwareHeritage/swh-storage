-- psql variables to get the current database flavor

select swh_get_dbflavor() = 'read_replica' as dbflavor_read_replica \gset
select swh_get_dbflavor() != 'read_replica' as dbflavor_does_deduplication \gset
select swh_get_dbflavor() = 'mirror' as dbflavor_mirror \gset
select swh_get_dbflavor() = 'default' as dbflavor_default \gset

-- content

create unique index concurrently content_pkey on content(sha1);
alter table content add primary key using index content_pkey;

\if :dbflavor_does_deduplication
  create unique index concurrently on content(sha1_git);
\else
  create index concurrently on content(sha1_git);
\endif

create index concurrently on content(sha256);
create index concurrently on content(blake2s256);

\if :dbflavor_default
  create unique index concurrently on content(object_id); -- to be reviewed
  create index concurrently on content(ctime);            -- to be reviewed
\endif

-- origin

create unique index concurrently origin_pkey on origin(id);
alter table origin add primary key using index origin_pkey;

\if :dbflavor_does_deduplication
  create unique index concurrently on origin using btree(url);
\else
  create index concurrently on origin using btree(url);
\endif

create index concurrently on origin using gin (url gin_trgm_ops);
create index concurrently on origin using btree(digest(url, 'sha1'));


-- skipped_content

\if :dbflavor_does_deduplication
  alter table skipped_content add constraint skipped_content_sha1_sha1_git_sha256_key unique (sha1, sha1_git, sha256);
\endif

create index concurrently on skipped_content(sha1);
create index concurrently on skipped_content(sha1_git);
create index concurrently on skipped_content(sha256);
create index concurrently on skipped_content(blake2s256);
create unique index concurrently on skipped_content(object_id);

\if :dbflavor_default
  alter table skipped_content add constraint skipped_content_origin_fkey foreign key (origin) references origin(id) not valid;
  alter table skipped_content validate constraint skipped_content_origin_fkey;
\endif

-- directory
create unique index concurrently directory_pkey on directory(id);
alter table directory add primary key using index directory_pkey;

\if :dbflavor_default
  create index concurrently on directory using gin (dir_entries);   -- to be reviewed
  create index concurrently on directory using gin (file_entries);  -- to be reviewed
  create index concurrently on directory using gin (rev_entries);   -- to be reviewed
  create unique index concurrently on directory(object_id);         -- to be reviewed
  create index concurrently directory_raw_manifest_not_null on directory(id) where raw_manifest is not null;   -- allows keeping track of the few directories that could not be parsed
\endif

-- directory_entry_dir

create unique index concurrently directory_entry_dir_pkey on directory_entry_dir(id);
alter table directory_entry_dir add primary key using index directory_entry_dir_pkey;

\if :dbflavor_does_deduplication
  create unique index concurrently on directory_entry_dir(target, name, perms);
\endif

-- directory_entry_file

create unique index concurrently directory_entry_file_pkey on directory_entry_file(id);
alter table directory_entry_file add primary key using index directory_entry_file_pkey;

\if :dbflavor_does_deduplication
  create unique index concurrently on directory_entry_file(target, name, perms);
\endif

-- directory_entry_rev

create unique index concurrently directory_entry_rev_pkey on directory_entry_rev(id);
alter table directory_entry_rev add primary key using index directory_entry_rev_pkey;

\if :dbflavor_does_deduplication
  create unique index concurrently on directory_entry_rev(target, name, perms);
\endif


-- person
create unique index concurrently person_pkey on person(id);
alter table person add primary key using index person_pkey;

\if :dbflavor_does_deduplication
  create unique index concurrently on person(fullname);
\else
  create index concurrently on person(fullname);  -- to be reviewed
\endif

\if :dbflavor_default
  create index concurrently on person(name);   -- to be reviewed
  create index concurrently on person(email);  -- to be reviewed
\endif

-- revision
create unique index concurrently revision_pkey on revision(id);
alter table revision add primary key using index revision_pkey;

\if :dbflavor_does_deduplication
  alter table revision add constraint revision_author_fkey foreign key (author) references person(id) not valid;
  alter table revision validate constraint revision_author_fkey;
  alter table revision add constraint revision_committer_fkey foreign key (committer) references person(id) not valid;
  alter table revision validate constraint revision_committer_fkey;

  alter table revision
    add constraint revision_date_neg_utc_offset_not_null
    check (date is null or date_neg_utc_offset is not null)
    not valid;

  alter table revision
    add constraint revision_committer_date_neg_utc_offset_not_null
    check (committer_date is null or committer_date_neg_utc_offset is not null)
    not valid;

  alter table revision
    add constraint revision_date_offset_not_null
    check (date is null or date_offset_bytes is not null)
    not valid;

  alter table revision
    add constraint revision_committer_date_offset_not_null
    check (committer_date is null or committer_date_offset_bytes is not null)
    not valid;

  alter table revision
    validate constraint revision_date_neg_utc_offset_not_null;
  alter table revision
    validate constraint revision_committer_date_neg_utc_offset_not_null;
  alter table revision
    validate constraint revision_date_offset_not_null;
  alter table revision
    validate constraint revision_committer_date_offset_not_null;

  -- if the author is null, then the date must be null
  alter table revision add constraint revision_author_date_check check ((date is null) or (author is not null)) not valid;
  alter table revision validate constraint revision_author_date_check;

  -- if the committer is null, then the committer_date must be null
  alter table revision add constraint revision_committer_date_check check ((committer_date is null) or (committer is not null)) not valid;
  alter table revision validate constraint revision_committer_date_check;
\endif

\if :dbflavor_default
  create index concurrently on revision(directory);          -- to be reviewed
  create unique index concurrently on revision(object_id);   -- to be reviewed
  create index concurrently revision_raw_manifest_not_null on revision(id) where raw_manifest is not null;   -- allows keeping track of the few revisions that could not be parsed
\endif

-- revision_history
create unique index concurrently revision_history_pkey on revision_history(id, parent_rank);
alter table revision_history add primary key using index revision_history_pkey;

\if :dbflavor_default
  create index concurrently on revision_history(parent_id);  -- to be reviewed
\endif

\if :dbflavor_does_deduplication
  alter table revision_history add constraint revision_history_id_fkey foreign key (id) references revision(id) not valid;
  alter table revision_history validate constraint revision_history_id_fkey;
\endif

-- snapshot
create unique index concurrently snapshot_pkey on snapshot(object_id);
alter table snapshot add primary key using index snapshot_pkey;

\if :dbflavor_does_deduplication
  create unique index concurrently on snapshot(id);
\else
  create index concurrently on snapshot(id);
\endif

-- snapshot_branch
create unique index concurrently snapshot_branch_pkey on snapshot_branch(object_id);
alter table snapshot_branch add primary key using index snapshot_branch_pkey;

\if :dbflavor_does_deduplication
  create unique index concurrently on snapshot_branch (target_type, target, name);
  alter table snapshot_branch add constraint snapshot_branch_target_check check ((target_type is null) = (target is null)) not valid;
  alter table snapshot_branch validate constraint snapshot_branch_target_check;
  alter table snapshot_branch add constraint snapshot_target_check check (target_type not in ('content', 'directory', 'revision', 'release', 'snapshot') or length(target) = 20) not valid;
  alter table snapshot_branch validate constraint snapshot_target_check;

  create unique index concurrently on snapshot_branch (name) where target_type is null and target is null;
\endif

-- snapshot_branches
create unique index concurrently snapshot_branches_pkey on snapshot_branches(snapshot_id, branch_id);
alter table snapshot_branches add primary key using index snapshot_branches_pkey;

\if :dbflavor_does_deduplication
  alter table snapshot_branches add constraint snapshot_branches_snapshot_id_fkey foreign key (snapshot_id) references snapshot(object_id) not valid;
  alter table snapshot_branches validate constraint snapshot_branches_snapshot_id_fkey;

  alter table snapshot_branches add constraint snapshot_branches_branch_id_fkey foreign key (branch_id) references snapshot_branch(object_id) not valid;
  alter table snapshot_branches validate constraint snapshot_branches_branch_id_fkey;
\endif

-- origin_visit
create unique index concurrently origin_visit_pkey on origin_visit(origin, visit);
alter table origin_visit add primary key using index origin_visit_pkey;

create index concurrently origin_visit_origin_date_idx on origin_visit (origin, date);

\if :dbflavor_does_deduplication
  alter table origin_visit add constraint origin_visit_origin_fkey foreign key (origin) references origin(id) not valid;
  alter table origin_visit validate constraint origin_visit_origin_fkey;
\endif

-- origin_visit_status

create unique index concurrently origin_visit_status_pkey on origin_visit_status(origin, visit, date);
alter table origin_visit_status add primary key using index origin_visit_status_pkey;

\if :dbflavor_default
  alter table origin_visit_status
    add constraint origin_visit_status_origin_visit_fkey
    foreign key (origin, visit)
    references origin_visit(origin, visit) not valid;
  alter table origin_visit_status validate constraint origin_visit_status_origin_visit_fkey;
\endif

-- release
create unique index concurrently release_pkey on release(id);
alter table release add primary key using index release_pkey;

\if :dbflavor_default
  create index concurrently on release(target, target_type);  -- to be reviewed
  create unique index concurrently on release(object_id);     -- to be reviewed
  create index concurrently release_raw_manifest_not_null on release(id) where raw_manifest is not null;   -- allows keeping track of the few releases that could not be parsed
\endif

\if :dbflavor_does_deduplication
  alter table release add constraint release_author_fkey foreign key (author) references person(id) not valid;
  alter table release validate constraint release_author_fkey;

  alter table release
    add constraint release_date_neg_utc_offset_not_null
    check (date is null or date_neg_utc_offset is not null)
    not valid;

  alter table release
    add constraint release_date_offset_not_null
    check (date is null or date_offset_bytes is not null)
    not valid;

  alter table release
    validate constraint release_date_neg_utc_offset_not_null;
  alter table release
    validate constraint release_date_offset_not_null;

  -- if the author is null, then the date must be null
  alter table release add constraint release_author_date_check check ((date is null) or (author is not null)) not valid;
  alter table release validate constraint release_author_date_check;
\endif

-- metadata_fetcher
create unique index metadata_fetcher_pkey on metadata_fetcher(id);
alter table metadata_fetcher add primary key using index metadata_fetcher_pkey;

\if :dbflavor_does_deduplication
  create unique index metadata_fetcher_name_version on metadata_fetcher(name, version);
\else
  create index metadata_fetcher_name_version on metadata_fetcher(name, version);
\endif

-- metadata_authority
create unique index concurrently metadata_authority_pkey on metadata_authority(id);
alter table metadata_authority add primary key using index metadata_authority_pkey;

\if :dbflavor_does_deduplication
  create unique index concurrently metadata_authority_type_url on metadata_authority(type, url);
\else
  create index concurrently metadata_authority_type_url on metadata_authority(type, url);
\endif


-- raw_extrinsic_metadata

create unique index concurrently raw_extrinsic_metadata_pkey on raw_extrinsic_metadata(id);
alter table raw_extrinsic_metadata add primary key using index raw_extrinsic_metadata_pkey;

create index concurrently raw_extrinsic_metadata_content_authority_date on raw_extrinsic_metadata(target, authority_id, discovery_date);

\if :dbflavor_default
  alter table raw_extrinsic_metadata add constraint raw_extrinsic_metadata_authority_fkey foreign key (authority_id) references metadata_authority(id) not valid;
  alter table raw_extrinsic_metadata validate constraint raw_extrinsic_metadata_authority_fkey;

  alter table raw_extrinsic_metadata add constraint raw_extrinsic_metadata_fetcher_fkey foreign key (fetcher_id) references metadata_fetcher(id) not valid;
  alter table raw_extrinsic_metadata validate constraint raw_extrinsic_metadata_fetcher_fkey;
\endif

-- object_counts
create unique index concurrently object_counts_pkey on object_counts(object_type);
alter table object_counts add primary key using index object_counts_pkey;

-- object_counts_bucketed
create unique index concurrently object_counts_bucketed_pkey on object_counts_bucketed(line);
alter table object_counts_bucketed add primary key using index object_counts_bucketed_pkey;

-- extid

-- used to query by (extid_type, extid) + to deduplicate the whole row
create unique index concurrently on extid(extid_type, extid, extid_version, target_type, target);
create index concurrently on extid(target_type, target);
