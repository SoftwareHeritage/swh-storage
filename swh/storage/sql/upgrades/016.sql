-- SWH DB schema upgrade
-- from_version: 15
-- to_version: 16
-- description: change unix_path to bytea

insert into dbversion(version, release, description)
      values(16, now(), 'Work In Progress');

alter domain unix_path rename to unix_path_text;
create domain unix_path as bytea;

-- pygit2 assumes utf-8 encoding on paths anyway
alter table directory_entry_dir alter column name type unix_path using convert_to(name, 'UTF-8');
alter table directory_entry_file alter column name type unix_path using convert_to(name, 'UTF-8');
alter table directory_entry_rev alter column name type unix_path using convert_to(name, 'UTF-8');

alter type directory_entry alter attribute name type unix_path;
alter type content_dir alter attribute path type unix_path;
alter type content_occurrence alter attribute path type unix_path;

drop domain unix_path_text;
