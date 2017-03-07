-- SWH DB schema upgrade
-- from_version: 100
-- to_version: 101
-- description: Open swh_content_update function

insert into dbversion(version, release, description)
      values(101, now(), 'Work In Progress');

-- Update content entries from temporary table.
-- (columns are potential new columns added to the schema, this cannot be empty)
--
create or replace function swh_content_update(columns_update text[])
    returns void
    language plpgsql
as $$
declare
   query text;
   tmp_array text[];
begin
    if array_length(columns_update, 1) = 0 then
        raise exception 'Please, provide the list of column names to update.';
    end if;

    tmp_array := array(select format('%1$s=t.%1$s', unnest) from unnest(columns_update));

    query = format('update content set %s
                    from tmp_content t where t.sha1 = content.sha1',
                    array_to_string(tmp_array, ', '));

    execute query;

    return;
end
$$;

comment on function swh_content_update(text[]) IS 'Update existing content''s columns';
