-- SWH DB schema upgrade
-- from_version: 191
-- to_version: 192
-- description: Add only_masking db flavor

alter type database_flavor add value 'only_masking';
