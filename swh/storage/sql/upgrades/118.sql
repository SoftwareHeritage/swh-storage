-- SWH DB schema upgrade
-- from_version: 117
-- to_version: 118
-- description: implement bucketed object counts

insert into dbversion(version, release, description)
      values(118, now(), 'Work In Progress');

CREATE SEQUENCE object_counts_bucketed_line_seq
	AS integer
	START WITH 1
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 1;

CREATE TABLE object_counts_bucketed (
	line integer DEFAULT nextval('object_counts_bucketed_line_seq'::regclass) NOT NULL,
	object_type text NOT NULL,
	identifier text NOT NULL,
	bucket_start bytea,
	bucket_end bytea,
	"value" bigint,
	last_update timestamp with time zone
);

ALTER TABLE object_counts
	ADD COLUMN single_update boolean;

ALTER SEQUENCE object_counts_bucketed_line_seq
	OWNED BY object_counts_bucketed.line;

CREATE OR REPLACE FUNCTION swh_update_counter_bucketed() RETURNS void
    LANGUAGE plpgsql
    AS $$
declare
  query text;
  line_to_update int;
  new_value bigint;
begin
  select
    object_counts_bucketed.line,
    format(
      'select count(%I) from %I where %s',
      coalesce(identifier, '*'),
      object_type,
      coalesce(
        concat_ws(
          ' and ',
          case when bucket_start is not null then
            format('%I >= %L', identifier, bucket_start) -- lower bound condition, inclusive
          end,
          case when bucket_end is not null then
            format('%I < %L', identifier, bucket_end) -- upper bound condition, exclusive
          end
        ),
        'true'
      )
    )
    from object_counts_bucketed
    order by coalesce(last_update, now() - '1 month'::interval) asc
    limit 1
    into line_to_update, query;

  execute query into new_value;

  update object_counts_bucketed
    set value = new_value,
        last_update = now()
    where object_counts_bucketed.line = line_to_update;

END
$$;

CREATE OR REPLACE FUNCTION swh_update_counters_from_buckets() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
with to_update as (
  select object_type, sum(value) as value, max(last_update) as last_update
  from object_counts_bucketed ob1
  where not exists (
    select 1 from object_counts_bucketed ob2
    where ob1.object_type = ob2.object_type
    and value is null
    )
  group by object_type
) update object_counts
  set
    value = to_update.value,
    last_update = to_update.last_update
  from to_update
  where
    object_counts.object_type = to_update.object_type
    and object_counts.value != to_update.value;
return null;
end
$$;

ALTER TABLE object_counts_bucketed
	ADD CONSTRAINT object_counts_bucketed_pkey PRIMARY KEY (line);

CREATE TRIGGER update_counts_from_bucketed
	AFTER INSERT OR UPDATE ON object_counts_bucketed
	FOR EACH ROW
	WHEN (((new.line % 256) = 0))
	EXECUTE PROCEDURE swh_update_counters_from_buckets();
