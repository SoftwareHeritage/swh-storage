-- SWH DB schema upgrade
-- from_version: 138
-- to_version: 139
-- description: Make fields target/name/perms of directory_entry_{dir,file,rev} not-null

insert into dbversion(version, release, description)
      values(139, now(), 'Work In Progress');

alter table directory_entry_dir alter column target set not null;
alter table directory_entry_dir alter column name set not null;
alter table directory_entry_dir alter column perms set not null;

alter table directory_entry_file alter column target set not null;
alter table directory_entry_file alter column name set not null;
alter table directory_entry_file alter column perms set not null;

alter table directory_entry_rev alter column target set not null;
alter table directory_entry_rev alter column name set not null;
alter table directory_entry_rev alter column perms set not null;
