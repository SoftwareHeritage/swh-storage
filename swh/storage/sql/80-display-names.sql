create table if not exists display_name (
  original_email bytea not null primary key,
  display_name bytea not null
);

comment on table display_name is 'Map from revision/release email to current full name';
comment on column display_name.original_email is 'Email on revision/release objects to match before applying the display name';
comment on column display_name.display_name is 'Full name, usually of the form `Name <email>, used for display queries';
