-- SWH DB schema upgrade
-- from_version: 14
-- to_version: 15
-- description: merge directory_list_* tables into directory

alter table directory
    add column dir_entries  bigint[],
    add column file_entries bigint[],
    add column rev_entries  bigint[];

with ls as (
    -- we need an explicit sub-query here, because left joins aren't allowed in
    -- update from_list
    select id,
           ls_d.entry_ids as dir_entries,
	   ls_f.entry_ids as file_entries,
	   ls_r.entry_ids as rev_entries
    from directory as d
    left join directory_list_dir  as ls_d on ls_d.dir_id = d.id
    left join directory_list_file as ls_f on ls_f.dir_id = d.id
    left join directory_list_rev  as ls_r on ls_r.dir_id = d.id
)
update directory
    set dir_entries =  ls.dir_entries,
        file_entries = ls.file_entries,
	rev_entries =  ls.rev_entries
    from ls
    where ls.id = directory.id;

create index on directory using gin (dir_entries);
create index on directory using gin (file_entries);
create index on directory using gin (rev_entries);

drop table directory_list_dir;
drop table directory_list_file;
drop table directory_list_rev;
