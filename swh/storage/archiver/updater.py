# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh.journal.client import SWHJournalClient

from .storage import get_archiver_storage


class SWHArchiverContentUpdater(SWHJournalClient):
    """Client in charge of updating new contents in the content_archiver
       db.

       This is a swh.journal client only dealing with contents.

    """
    CONFIG_BASE_FILENAME = 'archiver/content_updater'

    ADDITIONAL_CONFIG = {
        'archiver_storage': (
            'dict', {
                'cls': 'db',
                'args': {
                    'dbconn': 'dbname=softwareheritage-archiver-dev '
                              'user=guest',
                }
            }),
        'sources_present': ('list[str]', ['uffizi'])
    }

    def __init__(self):
        # Only interested in content here so override the configuration
        super().__init__(extra_configuration={'object_types': ['content']})

        self.sources_present = self.config['sources_present']

        self.archiver_storage = get_archiver_storage(
            **self.config['archiver_storage'])

    def process_objects(self, messages):
        self.archiver_storage.content_archive_add(
            (c[b'sha1'] for c in messages['content']),
            self.sources_present)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(process)d %(levelname)s %(message)s'
    )

    content_updater = SWHArchiverContentUpdater()
    content_updater.process()
