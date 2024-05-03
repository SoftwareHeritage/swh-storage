
create type blocked_state as enum ('non_blocked', 'decision_pending', 'blocked');
comment on type blocked_state is 'The degree to which an origin url is blocked';

create table if not exists blocking_request (
  id uuid primary key default uuid_generate_v4(),
  slug text not null,
  date timestamptz not null default now(),
  reason text not null
);

comment on table  blocking_request is 'A recorded request for blocking certain objects';
comment on column blocking_request.id is 'Opaque id of the request';
comment on column blocking_request.slug is 'Human-readable id of the request';
comment on column blocking_request.date is 'Date when the request was recorded';
comment on column blocking_request.reason is 'Free-form description of the request';

create table if not exists blocking_request_history (
  request uuid references blocking_request(id),
  date timestamptz not null default now(),
  message text not null,
  primary key (request, date)
);

comment on table blocking_request_history is 'History of a blocking request';
comment on column blocking_request_history.request is 'Opaque id of the request';
comment on column blocking_request_history.date is 'Date at which the message was recorded';
comment on column blocking_request_history.message is 'History message';

create table if not exists blocked_origin (
 url_match text not null,
 request uuid references blocking_request(id) not null,
 state blocked_state not null,
 primary key (url_match, request)
);

comment on table blocked_origin is 'All the origin known to be affected by a specific request';
comment on column blocked_origin.url_match is 'The url matching scheme to be blocked from being ingested';
comment on column blocked_origin.request is 'Reference to the affecting request';
comment on column blocked_origin.state is 'The degree to which the origin is blocked as a result of the request';

create table if not exists blocked_origin_log (
 url text not null,
 url_match text not null,
 request uuid references blocking_request(id) not null,
 state blocked_state not null,
 date  timestamptz not null default now(),
 primary key (url, date)
);
comment on table blocked_origin_log is 'Log origins that got blocked by the blocking proxy';
comment on column blocked_origin_log.url is 'The url of the origin';
comment on column blocked_origin_log.url_match is 'The url pattern matching the origin';
comment on column blocked_origin_log.request is 'Reference to the request which caused the blocking';
comment on column blocked_origin_log.state is 'The degree to which the origin has been blocked';
comment on column blocked_origin_log.date is 'The date the origin got blocked';
