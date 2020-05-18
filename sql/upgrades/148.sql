-- SWH DB schema upgrade
-- from_version: 147
-- to_version: 148
-- description: Fix origin_metadata values and migration schema to use correct primary key
-- 1. Fix existing duplicated entries in origin_metadata and metadata_provider
-- 2. Fix primary key on metadata_provider

-- latest schema version
insert into dbversion(version, release, description)
      values(148, now(), 'Work In Progress');

-- Clean up duplicated provider references in origin_metadata

-- Lookup provider with the most minimal id already matching the future
-- composite key (provider_type, provider_url)
update origin_metadata om set provider_id=(
  select min(mp1.id) as min_provider_id
  from metadata_provider mp1 inner join metadata_provider mp2
    on (mp1.provider_type = mp2.provider_type and mp1.provider_url = mp2.provider_url)
  where mp2.id=om.provider_id
);

--                                                              QUERY PLAN
-- -------------------------------------------------------------------------------------------------------------------------------------
--  Update on origin_metadata om  (cost=0.00..7506.94 rows=554 width=1157)
--    ->  Seq Scan on origin_metadata om  (cost=0.00..7506.94 rows=554 width=1157)
--          SubPlan 1
--            ->  Aggregate  (cost=13.37..13.38 rows=1 width=4)
--                  ->  Hash Join  (cost=2.18..12.90 rows=190 width=4)
--                        Hash Cond: ((mp1.provider_type = mp2.provider_type) AND (mp1.provider_url = mp2.provider_url))
--                        ->  Seq Scan on metadata_provider mp1  (cost=0.00..6.88 rows=288 width=51)
--                        ->  Hash  (cost=2.17..2.17 rows=1 width=47)
--                              ->  Index Scan using metadata_provider_pkey on metadata_provider mp2  (cost=0.15..2.17 rows=1 width=47)
--                                    Index Cond: (id = om.provider_id)
-- (10 rows)

-- Cleanup duplicated provider entries

delete from metadata_provider
where id not in (select distinct provider_id from origin_metadata);

-- Create unique index on table

create unique index metadata_provider_type_url
    on metadata_provider(provider_type, provider_url);
