-- SWH DB schema upgrade
-- from_version: 3
-- to_version: 4
-- description: Add azure instance

INSERT INTO dbversion(version, release, description)
VALUES(4, now(), 'Work In Progress');

ALTER TABLE archive DROP COLUMN url;
ALTER TABLE archive ALTER COLUMN id SET DATA TYPE TEXT;

INSERT INTO archive(id) VALUES ('azure');
