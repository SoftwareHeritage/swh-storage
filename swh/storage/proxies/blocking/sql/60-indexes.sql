
create unique index if not exists blocking_request_slug_idx on blocking_request using btree(slug);

create index if not exists blocked_origin_request_idx on blocked_origin using btree(request, url_match);
comment on index blocked_origin_request_idx is 'Allow listing all the objects associated by request, ordered by url';
