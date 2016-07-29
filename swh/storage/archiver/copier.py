# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


class ArchiverCopier():
    """ This archiver copy some files into a remote objstorage
    in order to get a backup.
    """
    def __init__(self, source, destination, content_ids):
        """ Create a Copier for the archiver

        Args:
            source (ObjStorage): source storage to get the contents.
            destination (ObjStorage): Storage where the contents will
                be copied.
            content_ids: list of content's id to archive.
        """
        self.source = source
        self.destination = destination
        self.content_ids = content_ids

    def run(self):
        """ Do the copy on the backup storage.

        Run the archiver copier in order to copy the required content
        into the current destination.
        The content which corresponds to the sha1 in self.content_ids
        will be fetched from the master_storage and then copied into
        the backup object storage.

        Returns:
            A boolean that indicates if the whole content have been copied.
        """
        try:
            for content_id in self.content_ids:
                content = self.source.get(content_id)
                self.destination.add(content, content_id)
        except:
            return False
        return True
