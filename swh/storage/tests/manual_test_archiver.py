import string
import random

from swh.core import hashutil
from swh.storage import Storage
from swh.storage.db import cursor_to_bytes

from swh.storage.archiver import ArchiverDirector


def rs(size=6, chars=string.ascii_uppercase + string.ascii_lowercase):
    return ''.join(random.choice(chars) for _ in range(size))


def mc(data):
    data = bytes(data, 'utf8')
    content = hashutil.hashdata(data)
    content.update({'data': data})
    return content


def initialize_content_archive(db, sample_size, names=['Local']):
    """ Initialize the content_archive table with a sample.

    From the content table, get a sample of id, and fill the
    content_archive table with those id in order to create a test sample
    for the archiver.

    Args:
        db: The database of the storage.
        sample_size (int): The size of the sample to create.
        names: A list of archive names. Those archives must already exists.
            Archival status of the archives content will be erased on db.

    Returns:
        Tha amount of entry created.
    """
    with db.transaction() as cur:
        cur.execute('DELETE FROM content_archive')

    with db.transaction() as cur:
        cur.execute('SELECT sha1 from content limit %d' % sample_size)
        ids = list(cursor_to_bytes(cur))

    for id, in ids:
        tid = r'\x' + hashutil.hash_to_hex(id)

        with db.transaction() as cur:
            for name in names:
                s = """INSERT INTO content_archive
                    VALUES('%s'::sha1, '%s', 'missing', now())
                    """ % (tid, name)
                cur.execute(s)

    print('Initialized database with', sample_size * len(names), 'items')
    return sample_size * len(names)


def clean():
    # Clean all
    with loc.db.transaction() as cur:
        cur.execute('delete from content_archive')
        cur.execute('delete from content')
    import os
    os.system("rm -r /tmp/swh/storage-dev/2/*")


CONTENT_SIZE = 10

if __name__ == '__main__':
    random.seed(0)
    # Local database
    dbname = 'softwareheritage-dev'
    user = 'qcampos'
    cstring = 'dbname=%s user=%s' % (dbname, user)
    # Archiver config
    config = {
        'objstorage_path': '/tmp/swh/storage-dev/2',
        'archival_max_age': 3600,
        'batch_max_size': 10,
        'retention_policy': 1,
        'asynchronous': False
    }

    # Grand-palais's storage
    loc = Storage(cstring, config['objstorage_path'])

    # Add the content
    l = [mc(rs(100)) for _ in range(CONTENT_SIZE)]
    loc.content_add(l)
    initialize_content_archive(loc.db, CONTENT_SIZE, ['petit-palais'])

    # Launch the archiver
    archiver = ArchiverDirector(cstring, config)
    archiver.run()
