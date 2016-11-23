-- SWH DB schema upgrade
-- from_version: 89
-- to_version: 90
-- description: indexer: Add content_ctags

insert into dbversion(version, release, description)
      values(91, now(), 'Work In Progress');

insert into fossology_license (name)
values
  ('Affero-possibility'),
  ('Apache-possibility'),
  ('Apache_v2-possibility'),
  ('Artistic-possibility'),
  ('BSD-possibility'),
  ('CMU-possibility'),
  ('CPL-possibility'),
  ('Freeware'),
  ('FSF-possibility'),
  ('GPL-2.0+:3.0'),
  ('GPL-2.0+&GPL-3.0+'),
  ('GPL-2.1[sic]'),
  ('GPL-2.1+[sic]'),
  ('GPL-possibility'),
  ('HP-possibility'),
  ('IBM-possibility'),
  ('ISC-possibility'),
  ('LGPL-possibility'),
  ('LGPL_v3-possibility'),
  ('Microsoft-possibility'),
  ('MIT-possibility'),
  ('NOT-public-domain'),
  ('Perl-possibility'),
  ('PHP-possibility'),
  ('RSA-possibility'),
  ('Sun-possibility'),
  ('Trademark-ref'),
  ('UnclassifiedLicense'),
  ('W3C-possibility'),
  ('X11-possibility');
