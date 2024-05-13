--
-- SWH Masking Proxy DB schema upgrade
-- from_version: 192
-- to_version: 193
-- description: Actually creates a dedicated DB for the masking proxy
--              This does nothing but dropping the flavor table and type

drop function if exists swh_get_dbflavor;
drop table if exists dbflavor;
drop type if exists database_flavor;
