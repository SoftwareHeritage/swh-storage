# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import io
import os
import tarfile
import tempfile
import itertools
from swh.core import hashutil

SKIPPED_MESSAGE = (b'This content have not been retrieved in '
                   b'Software Heritage archive due to its size')


HIDDEN_MESSAGE = (b'This content is hidden')


class BaseVaultCooker(metaclass=abc.ABCMeta):
    """Abstract base class for the vault's bundle creators

    This class describes a common API for the cookers.

    """
    @abc.abstractmethod
    def cook(self, obj_id):
        """Cook the requested object into a bundle

        The type of the object represented by the id depends on the
        concrete class. Very likely, each type of bundle will have its
        own cooker class.

        Args:
            obj_id: id of the object to be cooked into a bundle.

        """
        raise NotImplementedError(
            'Vault cookers must implement a `cook` method')


class DirectoryVaultCooker(BaseVaultCooker):
    """Cooker to create a directory bundle """

    def __init__(self, storage, cache):
        """Initialize a cooker that create directory bundles

        Args:
            storage: source storage where content are retrieved.
            cache: destination storage where the cooked bundle are stored.

        """
        self.storage = storage
        self.cache = cache

    def cook(self, dir_id):
        """Cook the requested directory into a Bundle

        Args:
            dir_id (bytes): the id of the directory to be cooked.

        Returns:
            bytes that correspond to the bundle

        """
        root = bytes(tempfile.mkdtemp(prefix='directory.', suffix='.cook'),
                     'utf8')
        # Retrieve data from the database
        data = list(self.storage.directory_ls(dir_id, recursive=True))
        data1, data2 = itertools.tee(data, 2)
        dir_data = (entry['name'] for entry in data1 if entry['type'] == 'dir')
        file_data = (entry for entry in data2 if entry['type'] == 'file')

        # Recreate the directory
        self._create_tree(root, dir_data)
        self._create_files(root, file_data)

        # Use the created directory to get the bundle datas
        bundle_content = self._create_bundle_content(
            root,
            hashutil.hash_to_hex(dir_id)
        )
        self._cache_bundle(dir_id, bundle_content)

        # Make a notification that the bundle have been cooked
        self._notify_bundle_ready(dir_id)

    def _create_tree(self, root, directory_paths):
        """Create a directory tree from the given paths

        The tree is created from `root` and each given path in
        `directory_paths` will be created.

        """
        # Directories are sorted by depth so they are created in the
        # right order
        bsep = bytes(os.path.sep, 'utf8')
        dir_names = sorted(
            directory_paths,
            key=lambda x: len(x.split(bsep))
        )
        for dir_name in dir_names:
            os.makedirs(os.path.join(root, dir_name))

    def _create_files(self, root, file_datas):
        """Iterates over the file datas and delegate to the right method.

        """
        # Then create the files
        for file_data in file_datas:
            path = os.path.join(root, file_data['name'])
            status = file_data['status']
            if status == 'absent':
                self._create_file_absent(path)
            elif status == 'hidden':
                self._create_file_hidden(path)
            else:
                content = self._get_file_content(file_data['sha1'])
                self._create_file(path, content)

    def _get_file_content(self, obj_id):
        content = list(self.storage.content_get([obj_id]))[0]['data']
        return content

    def _create_file(self, path, content):
        """Create the given file and fill it with content."""
        with open(path, 'wb') as f:
            f.write(content)

    def _create_file_absent(self, path):
        """Create a file that indicates a skipped content

        Create the given file but fill it with a specific content to
        indicates that the content have not been retrieved by the
        software heritage archive due to its size.

        """
        self._create_file(self, SKIPPED_MESSAGE)

    def _create_file_hidden(self, path):
        """Create a file that indicates an hidden content

        Create the given file but fill it with a specific content to
        indicates that the content could not be retrieved due to
        privacy policy.

        """
        self._create_file(self, HIDDEN_MESSAGE)

    def _create_bundle_content(self, path, hex_dir_id):
        """Create a bundle from the given directory

        Args:
            path: location of the directory to package.
            hex_dir_id: hex representation of the directory id

        Returns:
            a path to the newly created archive file.

        """
        tar_buffer = io.BytesIO()
        tar = tarfile.open(fileobj=tar_buffer, mode='w')
        tar.add(path.decode(), arcname=hex_dir_id)
        return tar_buffer.getbuffer()

    def _cache_bundle(self, dir_id, bundle_content):
        self.cache.add('directory', dir_id, bundle_content)

    def _notify_bundle_ready(self, bundle_id):
        # TODO plug this method with the notification method once
        # done.
        pass
