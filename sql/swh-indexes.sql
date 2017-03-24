-- content

create unique index concurrently content_pkey on content(sha1);
create unique index concurrently on content(sha1_git);
create unique index concurrently on content(sha256);
create unique index concurrently on content(blake2s256);
create index concurrently on content(ctime);  -- TODO use a BRIN index here (postgres >= 9.5)
create index concurrently on content(object_id);

alter table content add primary key using index content_pkey;

alter table content add constraint content_sha1_sha1_git_sha256_blake2s256_key unique (sha1, sha1_git, sha256, blake2s256);

-- entity_history

create unique index concurrently entity_history_pkey on entity_history(id);
create index concurrently on entity_history(uuid);
create index concurrently on entity_history(name);

alter table entity_history add primary key using index entity_history_pkey;

-- entity

create unique index concurrently entity_pkey on entity(uuid);

create index concurrently on entity(name);
create index concurrently on entity using gin(lister_metadata jsonb_path_ops);

alter table entity add primary key using index entity_pkey;
alter table entity add constraint entity_parent_fkey foreign key (parent) references entity(uuid) deferrable initially deferred not valid;
alter table entity validate constraint entity_parent_fkey;
alter table entity add constraint entity_last_id_fkey foreign key (last_id) references entity_history(id) not valid;
alter table entity validate constraint entity_last_id_fkey;

-- entity_equivalence

create unique index concurrently entity_equivalence_pkey on entity_equivalence(entity1, entity2);
alter table entity_equivalence add primary key using index entity_equivalence_pkey;


alter table entity_equivalence add constraint "entity_equivalence_entity1_fkey" foreign key (entity1) references entity(uuid) not valid;
alter table entity_equivalence validate constraint entity_equivalence_entity1_fkey;
alter table entity_equivalence add constraint "entity_equivalence_entity2_fkey" foreign key (entity2) references entity(uuid) not valid;
alter table entity_equivalence validate constraint entity_equivalence_entity2_fkey;
alter table entity_equivalence add constraint "order_entities" check (entity1 < entity2) not valid;
alter table entity_equivalence validate constraint order_entities;

-- listable_entity

create unique index concurrently listable_entity_pkey on listable_entity(uuid);
alter table listable_entity add primary key using index listable_entity_pkey;

alter table listable_entity add constraint listable_entity_uuid_fkey foreign key (uuid) references entity(uuid) not valid;
alter table listable_entity validate constraint listable_entity_uuid_fkey;

-- list_history

create unique index concurrently list_history_pkey on list_history(id);
alter table list_history add primary key using index list_history_pkey;

alter table list_history add constraint list_history_entity_fkey foreign key (entity) references listable_entity(uuid) not valid;
alter table list_history validate constraint list_history_entity_fkey;

-- origin
create unique index concurrently origin_pkey on origin(id);
alter table origin add primary key using index origin_pkey;

create index concurrently on origin(type, url);

alter table origin add constraint origin_lister_fkey foreign key (lister) references listable_entity(uuid) not valid;
alter table origin validate constraint origin_lister_fkey;

alter table origin add constraint origin_project_fkey foreign key (project) references entity(uuid) not valid;
alter table origin validate constraint origin_project_fkey;

-- skipped_content

alter table skipped_content add constraint skipped_content_sha1_sha1_git_sha256_blake2s256_key unique (sha1, sha1_git, sha256, blake2s256);

create unique index concurrently on skipped_content(sha1);
create unique index concurrently on skipped_content(sha1_git);
create unique index concurrently on skipped_content(sha256);
create unique index concurrently on skipped_content(blake2s256);
create index concurrently on skipped_content(object_id);

alter table skipped_content add constraint skipped_content_origin_fkey foreign key (origin) references origin(id) not valid;
alter table skipped_content validate constraint skipped_content_origin_fkey;

-- fetch_history

create unique index concurrently fetch_history_pkey on fetch_history(id);
alter table fetch_history add primary key using index fetch_history_pkey;

alter table fetch_history add constraint fetch_history_origin_fkey foreign key (origin) references origin(id) not valid;
alter table fetch_history validate constraint fetch_history_origin_fkey;

-- directory

create unique index concurrently directory_pkey on directory(id);
alter table directory add primary key using index directory_pkey;

create index concurrently on directory using gin (dir_entries);
create index concurrently on directory using gin (file_entries);
create index concurrently on directory using gin (rev_entries);
create index concurrently on directory(object_id);

-- directory_entry_dir

create unique index concurrently directory_entry_dir_pkey on directory_entry_dir(id);
alter table directory_entry_dir add primary key using index directory_entry_dir_pkey;

create unique index concurrently on directory_entry_dir(target, name, perms);

-- directory_entry_file

create unique index concurrently directory_entry_file_pkey on directory_entry_file(id);
alter table directory_entry_file add primary key using index directory_entry_file_pkey;

create unique index concurrently on directory_entry_file(target, name, perms);

-- directory_entry_rev

create unique index concurrently directory_entry_rev_pkey on directory_entry_rev(id);
alter table directory_entry_rev add primary key using index directory_entry_rev_pkey;

create unique index concurrently on directory_entry_rev(target, name, perms);

-- person
create unique index concurrently person_pkey on person(id);
alter table person add primary key using index person_pkey;

create unique index concurrently on person(fullname);
create index concurrently on person(name);
create index concurrently on person(email);

-- revision
create unique index concurrently revision_pkey on revision(id);
alter table revision add primary key using index revision_pkey;

alter table revision add constraint revision_author_fkey foreign key (author) references person(id) not valid;
alter table revision validate constraint revision_author_fkey;
alter table revision add constraint revision_committer_fkey foreign key (committer) references person(id) not valid;
alter table revision validate constraint revision_committer_fkey;

create index concurrently on revision(directory);
create index concurrently on revision(object_id);

-- revision_history
create unique index concurrently revision_history_pkey on revision_history(id, parent_rank);
alter table revision_history add primary key using index revision_history_pkey;

create index concurrently on revision_history(parent_id);

alter table revision_history add constraint revision_history_id_fkey foreign key (id) references revision(id) not valid;
alter table revision_history validate constraint revision_history_id_fkey;

-- origin_visit
create unique index concurrently origin_visit_pkey on origin_visit(origin, visit);
alter table origin_visit add primary key using index origin_visit_pkey;

create index concurrently on origin_visit(date);

alter table origin_visit add constraint origin_visit_origin_fkey foreign key (origin) references origin(id) not valid;
alter table origin_visit validate constraint origin_visit_origin_fkey;

-- occurrence_history
create unique index concurrently occurrence_history_pkey on occurrence_history(object_id);
alter table occurrence_history add primary key using index occurrence_history_pkey;

create index concurrently on occurrence_history(target, target_type);
create index concurrently on occurrence_history(origin, branch);
create unique index concurrently on occurrence_history(origin, branch, target, target_type);

alter table occurrence_history add constraint occurrence_history_origin_fkey foreign key (origin) references origin(id) not valid;
alter table occurrence_history validate constraint occurrence_history_origin_fkey;

-- occurrence
create unique index concurrently occurrence_pkey on occurrence(origin, branch);
alter table occurrence add primary key using index occurrence_pkey;

alter table occurrence add constraint occurrence_origin_fkey foreign key (origin) references origin(id) not valid;
alter table occurrence validate constraint occurrence_origin_fkey;


-- release
create unique index concurrently release_pkey on release(id);
alter table release add primary key using index release_pkey;

create index concurrently on release(target, target_type);
create index concurrently on release(object_id);

alter table release add constraint release_author_fkey foreign key (author) references person(id) not valid;
alter table release validate constraint release_author_fkey;

-- cache_content_revision
create unique index concurrently cache_content_revision_pkey on cache_content_revision(content);
alter table cache_content_revision add primary key using index cache_content_revision_pkey;

alter table cache_content_revision add constraint cache_content_revision_content_fkey foreign key (content) references content(sha1_git) not valid;
alter table cache_content_revision validate constraint cache_content_revision_content_fkey;

-- cache_content_revision_processed
create unique index concurrently cache_content_revision_processed_pkey on cache_content_revision_processed(revision);
alter table cache_content_revision_processed add primary key using index cache_content_revision_processed_pkey;

alter table cache_content_revision_processed add constraint cache_content_revision_processed_revision_fkey foreign key (revision) references revision(id) not valid;
alter table cache_content_revision_processed validate constraint cache_content_revision_processed_revision_fkey;

-- cache_revision_origin
create unique index concurrently cache_revision_origin_pkey on cache_revision_origin(revision, origin, visit);
alter table cache_revision_origin add primary key using index cache_revision_origin_pkey;

alter table cache_revision_origin add constraint cache_revision_origin_revision_fkey foreign key (revision) references revision(id) not valid;
alter table cache_revision_origin validate constraint cache_revision_origin_revision_fkey;

alter table cache_revision_origin add constraint cache_revision_origin_origin_fkey foreign key (origin, visit) references origin_visit(origin, visit) not valid;
alter table cache_revision_origin validate constraint cache_revision_origin_origin_fkey;

create index concurrently on cache_revision_origin(revision);

-- indexer_configuration
create unique index concurrently indexer_configuration_pkey on indexer_configuration(id);
alter table indexer_configuration add primary key using index indexer_configuration_pkey;

create unique index on indexer_configuration(tool_name, tool_version);

-- content_mimetype
create unique index concurrently content_mimetype_pkey on content_mimetype(id, indexer_configuration_id);
alter table content_mimetype add primary key using index content_mimetype_pkey;

alter table content_mimetype add constraint content_mimetype_id_fkey foreign key (id) references content(sha1) not valid;
alter table content_mimetype validate constraint content_mimetype_id_fkey;

alter table content_mimetype add constraint content_mimetype_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table content_mimetype validate constraint content_mimetype_indexer_configuration_id_fkey;

-- content_language
create unique index concurrently content_language_pkey on content_language(id, indexer_configuration_id);
alter table content_language add primary key using index content_language_pkey;

alter table content_language add constraint content_language_id_fkey foreign key (id) references content(sha1) not valid;
alter table content_language validate constraint content_language_id_fkey;

alter table content_language add constraint content_language_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table content_language validate constraint content_language_indexer_configuration_id_fkey;

-- content_ctags
create index concurrently on content_ctags(id);
create index concurrently on content_ctags(hash_sha1(name));
create unique index concurrently on content_ctags(id, hash_sha1(name), kind, line, lang, indexer_configuration_id);

alter table content_ctags add constraint content_ctags_id_fkey foreign key (id) references content(sha1) not valid;
alter table content_ctags validate constraint content_ctags_id_fkey;

alter table content_ctags add constraint content_ctags_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table content_ctags validate constraint content_ctags_indexer_configuration_id_fkey;

-- fossology_license
create unique index concurrently fossology_license_pkey on fossology_license(id);
alter table fossology_license add primary key using index fossology_license_pkey;

create unique index on fossology_license(name);

-- content_fossology_license
create unique index concurrently content_fossology_license_pkey on content_fossology_license(id, license_id, indexer_configuration_id);
alter table content_fossology_license add primary key using index content_fossology_license_pkey;

alter table content_fossology_license add constraint content_fossology_license_id_fkey foreign key (id) references content(sha1) not valid;
alter table content_fossology_license validate constraint content_fossology_license_id_fkey;

alter table content_fossology_license add constraint content_fossology_license_license_id_fkey foreign key (license_id) references fossology_license(id) not valid;
alter table content_fossology_license validate constraint content_fossology_license_license_id_fkey;

alter table content_fossology_license add constraint content_fossology_license_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table content_fossology_license validate constraint content_fossology_license_indexer_configuration_id_fkey;
