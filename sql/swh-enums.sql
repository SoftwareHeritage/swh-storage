---
--- Software Heritage Data Types
---

create type content_status as enum ('absent', 'visible', 'hidden');
comment on type content_status is 'Content visibility';

-- Types of entities.
--
-- - organization: a root entity, usually backed by a non-profit, a
-- company, or another kind of "association". (examples: Software
-- Heritage, Debian, GNU, GitHub)
--
-- - group_of_entities: used for hierarchies, doesn't need to have a
-- concrete existence. (examples: GNU hosting facilities, Debian
-- hosting facilities, GitHub users, ...)
--
-- - hosting: a hosting facility, can usually be listed to generate
-- other data. (examples: GitHub git hosting, alioth.debian.org,
-- snapshot.debian.org)
--
-- - group_of_persons: an entity representing a group of
-- persons. (examples: a GitHub organization, a Debian team)
--
-- - person: an entity representing a person. (examples:
-- a GitHub user, a Debian developer)
--
-- - project: an entity representing a software project. (examples: a
-- GitHub project, Apache httpd, a Debian source package, ...)
create type entity_type as enum (
  'organization',
  'group_of_entities',
  'hosting',
  'group_of_persons',
  'person',
  'project'
);
comment on type entity_type is 'Entity types';

create type revision_type as enum ('git', 'tar', 'dsc', 'svn');
comment on type revision_type is 'Possible revision types';

create type object_type as enum ('content', 'directory', 'revision', 'release');
comment on type object_type is 'Data object types stored in data model';

create type origin_visit_status as enum (
  'ongoing',
  'full',
  'partial'
);
comment on type origin_visit_status IS 'Possible visit status';
