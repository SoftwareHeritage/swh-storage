#!/usr/bin/env python3

import os
import pickle

import psycopg2.extras

from swh.storage import converters, db
from swh.model import identifiers


QUERY = '''
select
  r.id,
  r.date, r.date_offset, r.date_neg_utc_offset,
  r.committer_date, r.committer_date_offset, r.committer_date_neg_utc_offset,
  r.type, r.directory, r.message,
  a.id as author_id, a.name as author_name, a.email as author_email,
  c.id as committer_id, c.name as committer_name, c.email as committer_email,
  r.metadata, r.synthetic,
  array(select rh.parent_id::bytea from revision_history rh where rh.id = r.id
        order by rh.parent_rank)
          as parents
from revision r
left join person a on a.id = r.author
left join person c on c.id = r.committer
where r.id > %s
order by r.id
limit %s
'''


def dump_revision(revision):
    rev_id = identifiers.identifier_to_str(revision['id'])
    dirs = 'revs/%s/%s' % (rev_id[0:2], rev_id[2:4])
    os.makedirs(dirs, exist_ok=True)
    with open(os.path.join(dirs, rev_id), 'wb') as f:
        pickle.dump(revision, f)


def check_revision(revision):
    id = identifiers.identifier_to_str(revision['id'])

    computed_id = identifiers.revision_identifier(revision)
    if id != computed_id:
        dump_revision(revision)


if __name__ == '__main__':
    swh_db = db.Db.connect('service=swh',
                           cursor_factory=psycopg2.extras.RealDictCursor)

    last_id = bytes.fromhex('51606a8181f7c6d0aff852106c3ec23ebc186439')

    while True:
        with swh_db.transaction() as cur:
            cur.execute(QUERY, (last_id, 10000))
            if not cur.rowcount > 0:
                break
            for db_rev in db.cursor_to_bytes(cur):
                revision = converters.db_to_revision(db_rev)
                check_revision(revision)

            last_id = revision['id']
