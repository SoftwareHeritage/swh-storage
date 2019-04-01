# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
from multiprocessing import Manager


class InMemoryJournalWriter:
    def __init__(self):
        # Share the list of objects across processes, for RemoteAPI tests.
        self.manager = Manager()
        self.objects = self.manager.list()

    def write_addition(self, object_type, object_):
        self.objects.append((object_type, copy.deepcopy(object_)))

    write_update = write_addition

    def write_additions(self, object_type, objects):
        for object_ in objects:
            self.write_addition(object_type, object_)


def get_journal_writer(cls, args={}):
    if cls == 'inmemory':
        JournalWriter = InMemoryJournalWriter
    elif cls == 'kafka':
        from swh.journal.direct_writer import DirectKafkaWriter \
            as JournalWriter
    else:
        raise ValueError('Unknown storage class `%s`' % cls)

    return JournalWriter(**args)
