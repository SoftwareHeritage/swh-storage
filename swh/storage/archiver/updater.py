# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.journal.client import SWHJournalClient

from .storage import ArchiverStorage


class SWHArchiverContentUpdater(SWHJournalClient):
    """Client in charge of updating new contents in the content_archiver
       db.

       This is a swh.journal client only dealing with contents.

    """
    ADDITIONAL_CONFIG = {
        'archiver_storage_conn': (
            'str', 'dbname=softwareheritage-archiver-dev user=guest'),
        'sources_missing': ('list[str]', ['banco', 'azure']),
        'sources_present': ('list[str]', ['uffizi'])

    }

    def __init__(self):
        # Only interested in content here so override the configuration
        super().__init__(extra_configuration={'object_types': ['content']})

        self.sources_present = self.config['sources_present']
        self.sources_missing = self.config['sources_missing']

        self.archiver_storage = ArchiverStorage(
            self.config['archiver_storage_conn'])

    def process_objects(self, messages):
        key_id = b'sha1'

        content_ids = [{'content_id': c[key_id]} for c in messages['content']]
        unknowns = self.archiver_storage.content_archive_get_unknown(
            content_ids)
        self.archiver_storage.content_archive_content_add(
            unknowns, self.sources_present, self.sources_missing)


if __name__ == '__main__':
    content_updater = SWHArchiverContentUpdater()
    content_updater.process()
