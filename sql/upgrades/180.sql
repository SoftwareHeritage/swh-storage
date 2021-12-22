-- SWH DB schema upgrade
-- from_version: 177
-- to_version: 180
-- description: add {,committer_}date_offset_bytes to rev/rel + raw_manifest to dir/rev/rel, part 2

insert into dbversion(version, release, description)
    values(180, now(), 'Work In Progress');

-- copied from 60-indexes.sql
select swh_get_dbflavor() != 'read_replica' as dbflavor_does_deduplication \gset

-- step 3: fill the offsets

create or replace function _format_offset(offset_ smallint, neg_utc_offset bool)
    returns bytea
    language plpgsql
as $$
    begin
    return convert_to(
        -- sign
        case when offset_ < 0 or neg_utc_offset then '-' else '+' end
        -- hours (unfortunately we can't use lpad because it truncates)
        || case when abs(offset_) >= 600 then
            cast(abs(offset_) / 60 as text)
           else
            '0' || cast(abs(offset_) / 60 as text)
           end
        -- minutes
        || lpad(cast(mod(abs(offset_), 60) as text), 2, '0'),
        'utf8'
    );
    end
$$;

-- make sure it's correct
do $$ begin
    assert (select _format_offset(NULL::smallint, NULL::bool)) is not distinct from NULL;
    assert (select _format_offset(0::smallint, false)) = '+0000'::bytea;
    assert (select _format_offset(0::smallint, true)) = '-0000'::bytea;
    assert (select _format_offset(1::smallint, false)) = '+0001'::bytea;
    assert (select _format_offset(-1::smallint, false)) = '-0001'::bytea;
    assert (select _format_offset(120::smallint, false)) = '+0200'::bytea;
    assert (select _format_offset(-120::smallint, false)) = '-0200'::bytea;
    assert (select _format_offset(6000::smallint, false)) = '+10000'::bytea;
    assert (select _format_offset(-6000::smallint, false)) = '-10000'::bytea;
end$$;

update release
    set date_offset_bytes=_format_offset(date_offset, date_neg_utc_offset)
    where date is not null and date_offset_bytes is null;

update revision
    set date_offset_bytes=_format_offset(date_offset, date_neg_utc_offset),
        committer_date_offset_bytes=_format_offset(committer_date_offset, committer_date_neg_utc_offset)
    where (date is not null and date_offset_bytes is null)
       or (committer_date is not null and committer_date_offset_bytes is null);


-- step 4: add integrity constraints

\if :dbflavor_does_deduplication

  -- add new constraint on release dates
  alter table release
    add constraint release_date_offset_not_null
    check (date is null or date_offset_bytes is not null)
    not valid;
  alter table release
    validate constraint release_date_offset_not_null;

  alter table revision
    add constraint revision_date_offset_not_null
    check (date is null or date_offset_bytes is not null)
    not valid;

  alter table revision
    add constraint revision_committer_date_offset_not_null
    check (committer_date is null or committer_date_offset_bytes is not null)
    not valid;

  alter table revision
    validate constraint revision_date_offset_not_null;
  alter table revision
    validate constraint revision_committer_date_offset_not_null;
\endif

-- step 5: remove the old columns (date_offset and date_neg_utc_offset): in a future migration...

