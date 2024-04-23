
create unique index if not exists masking_request_slug_idx on masking_request using btree(slug);

create index if not exists masked_object_request_idx on masked_object using btree(request, object_type, object_id);
comment on index masked_object_request_idx is 'Allow listing all the objects associated by request, ordered by SWHID';
