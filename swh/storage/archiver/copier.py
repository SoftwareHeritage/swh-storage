# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core import hashutil
from swh.objstorage.api.client import RemoteObjStorage


class ArchiverCopier():
    """ This archiver copy some files into a remote objstorage
    in order to get a backup.

    Attributes:
        content_ids: A list of sha1's that represents the content this copier
            has to archive.
        server (RemoteArchive): The remote object storage that is used to
            backup content.
        master_objstorage (ObjStorage): The master storage that contains the
            data the copier needs to archive.
    """
    def __init__(self, destination, content, master_objstorage):
        """ Create a Copier for the archiver

        Args:
            destination: A tuple (archive_name, archive_url) that represents a
                remote object storage as in the 'archive' table.
            content: A list of sha1 that represents the content this copier
                have to archive.
            master_storage (Storage): The master storage of the system that
                contains the data to archive.
        """
        _name, self.url = destination
        self.content_ids = content
        self.server = RemoteObjStorage(self.url)
        self.master_objstorage = master_objstorage

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
        self.content_ids = map(lambda x: hashutil.hex_to_hash(x[2:]),
                               self.content_ids)
        try:
            for content_id in self.content_ids:
                content = self.master_objstorage.get(content_id)
                self.server.content_add(content, content_id)
        except:
            return False
        return True
