
create table if not exists masking_request (
  id uuid primary key default uuid_generate_v4(),
  slug text not null,
  date timestamptz not null default now(),
  reason text not null
);

comment on table masking_request is 'A recorded request for masking certain objects';
comment on column masking_request.id is 'Opaque id of the request';
comment on column masking_request.slug is 'Human-readable id of the request';
comment on column masking_request.date is 'Date when the request was recorded';
comment on column masking_request.reason is 'Free-form description of the request';

create table if not exists masking_request_history (
  request uuid references masking_request(id),
  date timestamptz not null default now(),
  message text not null,
  primary key (request, date)
);

comment on table masking_request_history is 'History of a masking request';
comment on column masking_request_history.request is 'Opaque id of the request';
comment on column masking_request_history.date is 'Date at which the message was recorded';
comment on column masking_request_history.message is 'History message';


create table if not exists masked_object (
 object_id bytea not null,
 object_type extended_object_type not null,
 request uuid references masking_request(id) not null,
 state masked_state not null,
 primary key (object_id, object_type, request)
);

comment on table masked_object is 'All the objects known to be affected by a specific request';
comment on column masked_object.object_id is 'The object_id part of the object''s SWHID';
comment on column masked_object.object_type is 'The object_type part of the object''s SWHID';
comment on column masked_object.request is 'Reference to the affecting request';
comment on column masked_object.state is 'The degree to which the object is masked as a result of the request';



-- Used only by the patching proxy, not the masking proxy

create table if not exists display_name (
  original_email bytea not null primary key,
  display_name bytea not null
);

comment on table display_name is 'Map from revision/release email to current full name';
comment on column display_name.original_email is 'Email on revision/release objects to match before applying the display name';
comment on column display_name.display_name is 'Full name, usually of the form `Name <email>, used for display queries';
