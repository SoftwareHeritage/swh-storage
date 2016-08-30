-- SWH DB schema upgrade
-- from_version: 76
-- to_version: 77
-- description: Add content provenance information cache tables

insert into dbversion(version, release, description)
      values(77, now(), 'Work In Progress');

CREATE TABLE cache_content_revision (
	content sha1_git NOT NULL,
	revision sha1_git NOT NULL,
	"path" unix_path NOT NULL
);

CREATE TABLE cache_revision_origin (
	revision sha1_git NOT NULL,
	origin bigint NOT NULL,
	visit bigint NOT NULL
);

ALTER TABLE cache_content_revision
	ADD CONSTRAINT cache_content_revision_pkey PRIMARY KEY (content, revision, path);

ALTER TABLE cache_revision_origin
	ADD CONSTRAINT cache_revision_origin_pkey PRIMARY KEY (revision, origin, visit);

ALTER TABLE cache_content_revision
	ADD CONSTRAINT cache_content_revision_content_fkey FOREIGN KEY (content) REFERENCES content(sha1_git);

ALTER TABLE cache_content_revision
	ADD CONSTRAINT cache_content_revision_revision_fkey FOREIGN KEY (revision) REFERENCES revision(id);

ALTER TABLE cache_revision_origin
	ADD CONSTRAINT cache_revision_origin_origin_fkey FOREIGN KEY (origin, visit) REFERENCES origin_visit(origin, visit);

ALTER TABLE cache_revision_origin
	ADD CONSTRAINT cache_revision_origin_revision_fkey FOREIGN KEY (revision) REFERENCES revision(id);

CREATE INDEX cache_content_revision_content_idx ON cache_content_revision USING btree (content);

CREATE INDEX cache_content_revision_revision_idx ON cache_content_revision USING btree (revision);

CREATE INDEX cache_revision_origin_revision_idx ON cache_revision_origin USING btree (revision);
