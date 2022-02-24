-- SWH DB schema upgrade
-- from_version: 152
-- to_version: 153
-- description: Make (origin, authority, discover_date, fetcher) a unique index

-- latest schema version
insert into dbversion(version, release, description)
      values(153, now(), 'Work In Progress');


create unique index origin_metadata_origin_authority_date_fetcher on origin_metadata(origin_id, authority_id, discovery_date, fetcher_id);

drop index origin_metadata_origin_authority_date;
