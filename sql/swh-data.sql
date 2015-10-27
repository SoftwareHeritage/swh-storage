insert into entity_history
  (uuid, parent, name, type, description, homepage, active, generated, validity)
values
  ('5f4d4c51-498a-4e28-88b3-b3e4e8396cba', NULL, 'softwareheritage',
   'organization', 'Software Heritage',
   'http://www.softwareheritage.org/', true, false, ARRAY[now()]),
  ('6577984d-64c8-4fab-b3ea-3cf63ebb8589', NULL, 'gnu', 'organization',
   'GNU is not UNIX', 'https://gnu.org/', true, false, ARRAY[now()]),
  ('7c33636b-8f11-4bda-89d9-ba8b76a42cec', '6577984d-64c8-4fab-b3ea-3cf63ebb8589',
   'GNU Hosting', 'group_of_entities',
   'GNU Hosting facilities', NULL, true, false, ARRAY[now()]),
  ('4706c92a-8173-45d9-93d7-06523f249398', '6577984d-64c8-4fab-b3ea-3cf63ebb8589',
   'GNU rsync mirror', 'hosting',
   'GNU rsync mirror', 'rsync://mirror.gnu.org/', true, false, ARRAY[now()]),
  ('5cb20137-c052-4097-b7e9-e1020172c48e', '6577984d-64c8-4fab-b3ea-3cf63ebb8589',
   'GNU Projects', 'group_of_entities',
   'GNU Projects', 'https://gnu.org/software/', true, false, ARRAY[now()]),
  ('4bfb38f6-f8cd-4bc2-b256-5db689bb8da4', NULL, 'GitHub', 'organization',
   'GitHub', 'https://github.org/', true, false, ARRAY[now()]),
  ('aee991a0-f8d7-4295-a201-d1ce2efc9fb2', '4bfb38f6-f8cd-4bc2-b256-5db689bb8da4',
   'GitHub Hosting', 'group_of_entities',
   'GitHub Hosting facilities', 'https://github.org/', true, false, ARRAY[now()]),
  ('34bd6b1b-463f-43e5-a697-785107f598e4', 'aee991a0-f8d7-4295-a201-d1ce2efc9fb2',
   'GitHub git hosting', 'hosting',
   'GitHub git hosting', 'https://github.org/', true, false, ARRAY[now()]),
  ('e8c3fc2e-a932-4fd7-8f8e-c40645eb35a7', 'aee991a0-f8d7-4295-a201-d1ce2efc9fb2',
   'GitHub asset hosting', 'hosting',
   'GitHub asset hosting', 'https://github.org/', true, false, ARRAY[now()]),
  ('9f7b34d9-aa98-44d4-8907-b332c1036bc3', '4bfb38f6-f8cd-4bc2-b256-5db689bb8da4',
   'GitHub Organizations', 'group_of_entities',
   'GitHub Organizations', 'https://github.org/', true, false, ARRAY[now()]),
  ('ad6df473-c1d2-4f40-bc58-2b091d4a750e', '4bfb38f6-f8cd-4bc2-b256-5db689bb8da4',
   'GitHub Users', 'group_of_entities',
   'GitHub Users', 'https://github.org/', true, false, ARRAY[now()]);

insert into listable_entity
  (uuid, list_engine)
values
  ('34bd6b1b-463f-43e5-a697-785107f598e4', 'swh.lister.github');
