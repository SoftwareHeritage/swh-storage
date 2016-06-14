# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import gzip
import tempfile
import random

from contextlib import contextmanager

from swh.core import hashutil

from .objstorage import ObjStorage, ID_HASH_ALGO, ID_HASH_LENGTH
from ..exc import ObjNotFoundError, Error


GZIP_BUFSIZ = 1048576

DIR_MODE = 0o755
FILE_MODE = 0o644


@contextmanager
def _write_obj_file(hex_obj_id, objstorage):
    """ Context manager for writing object files to the object storage.

    During writing, data are written to a temporary file, which is atomically
    renamed to the right file name after closing. This context manager also
    takes care of (gzip) compressing the data on the fly.

    Usage sample:
        with _write_obj_file(hex_obj_id, objstorage):
            f.write(obj_data)

    Yields:
        a file-like object open for writing bytes.
    """
    # Get the final paths and create the directory if absent.
    dir = objstorage._obj_dir(hex_obj_id)
    if not os.path.isdir(dir):
        os.makedirs(dir, DIR_MODE, exist_ok=True)
    path = os.path.join(dir, hex_obj_id)

    # Create a temporary file.
    (tmp, tmp_path) = tempfile.mkstemp(suffix='.tmp', prefix='hex_obj_id.',
                                       dir=dir)

    # Open the file and yield it for writing.
    tmp_f = os.fdopen(tmp, 'wb')
    with gzip.GzipFile(filename=tmp_path, fileobj=tmp_f) as f:
        yield f

    # Then close the temporary file and move it to the right directory.
    tmp_f.close()
    os.chmod(tmp_path, FILE_MODE)
    os.rename(tmp_path, path)


@contextmanager
def _read_obj_file(hex_obj_id, objstorage):
    """ Context manager for reading object file in the object storage.

    Usage sample:
        with _read_obj_file(hex_obj_id, objstorage) as f:
            b = f.read()

    Yields:
        a file-like object open for reading bytes.
    """
    path = objstorage._obj_path(hex_obj_id)
    with gzip.GzipFile(path, 'rb') as f:
        yield f


class PathSlicingObjStorage(ObjStorage):
    """ Implementation of the ObjStorage API based on the hash of the content.

    On disk, an object storage is a directory tree containing files named after
    their object IDs. An object ID is a checksum of its content, depending on
    the value of the ID_HASH_ALGO constant (see hashutil for its meaning).

    To avoid directories that contain too many files, the object storage has a
    given slicing. Each slicing correspond to a directory that is named
    according to the hash of its content.

    So for instance a file with SHA1 34973274ccef6ab4dfaaf86599792fa9c3fe4689
    will be stored in the given object storages :

    - 0:2/2:4/4:6 : 34/97/32/34973274ccef6ab4dfaaf86599792fa9c3fe4689
    - 0:1/0:5/    : 3/34973/34973274ccef6ab4dfaaf86599792fa9c3fe4689

    The files in the storage are stored in gzipped compressed format.

    Attributes:
        root (string): path to the root directory of the storage on the disk.
        bounds: list of tuples that indicates the beginning and the end of
            each subdirectory for a content.
    """

    def __init__(self, root, slicing):
        """ Create an object to access a hash-slicing based object storage.

        Args:
            root (string): path to the root directory of the storage on
                the disk.
            slicing (string): string that indicates the slicing to perform
                on the hash of the content to know the path where it should
                be stored.
        """
        if not os.path.isdir(root):
            raise ValueError(
                'PathSlicingObjStorage root "%s" is not a directory' % root
            )

        self.root = root
        # Make a list of tuples where each tuple contains the beginning
        # and the end of each slicing.
        self.bounds = [
            slice(*map(int, sbounds.split(':')))
            for sbounds in slicing.split('/')
            if sbounds
        ]

        max_endchar = max(map(lambda bound: bound.stop, self.bounds))
        if ID_HASH_LENGTH < max_endchar:
            raise ValueError(
                'Algorithm %s has too short hash for slicing to char %d'
                % (ID_HASH_ALGO, max_endchar)
            )

    def __contains__(self, obj_id):
        """ Check whether the given object is present in the storage or not.

        Returns:
            True iff the object is present in the storage.
        """
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        return os.path.exists(self._obj_path(hex_obj_id))

    def __iter__(self):
        """iterate over the object identifiers currently available in the storage

        Warning: with the current implementation of the object storage, this
        method will walk the filesystem to list objects, meaning that listing
        all objects will be very slow for large storages. You almost certainly
        don't want to use this method in production.

        Return:
            iterator over object IDs
        """
        def obj_iterator():
            # XXX hackish: it does not verify that the depth of found files
            # matches the slicing depth of the storage
            for root, _dirs, files in os.walk(self.root):
                for f in files:
                    yield bytes.fromhex(f)

        return obj_iterator()

    def __len__(self):
        """compute the number of objects available in the storage

        Warning: this currently uses `__iter__`, its warning about bad
        performances applies

        Return:
            number of objects contained in the storage

        """
        return sum(1 for i in self)

    def _obj_dir(self, hex_obj_id):
        """ Compute the storage directory of an object.

        See also: PathSlicingObjStorage::_obj_path

        Args:
            hex_obj_id: object id as hexlified string.

        Returns:
            Path to the directory that contains the required object.
        """
        slices = [hex_obj_id[bound] for bound in self.bounds]
        return os.path.join(self.root, *slices)

    def _obj_path(self, hex_obj_id):
        """ Compute the full path to an object into the current storage.

        See also: PathSlicingObjStorage::_obj_dir

        Args:
            hex_obj_id: object id as hexlified string.

        Returns:
            Path to the actual object corresponding to the given id.
        """
        return os.path.join(self._obj_dir(hex_obj_id), hex_obj_id)

    def add(self, bytes, obj_id=None, check_presence=True):
        """ Add a new object to the object storage.

        Args:
            bytes: content of the object to be added to the storage.
            obj_id: checksum of [bytes] using [ID_HASH_ALGO] algorithm. When
                given, obj_id will be trusted to match the bytes. If missing,
                obj_id will be computed on the fly.
            check_presence: indicate if the presence of the content should be
                verified before adding the file.

        Returns:
            the id of the object into the storage.
        """
        if obj_id is None:
            # Checksum is missing, compute it on the fly.
            h = hashutil._new_hash(ID_HASH_ALGO, len(bytes))
            h.update(bytes)
            obj_id = h.digest()

        if check_presence and obj_id in self:
            # If the object is already present, return immediatly.
            return obj_id

        hex_obj_id = hashutil.hash_to_hex(obj_id)
        with _write_obj_file(hex_obj_id, self) as f:
            f.write(bytes)

        return obj_id

    def restore(self, bytes, obj_id=None):
        """ Restore a content that have been corrupted.

        This function is identical to add_bytes but does not check if
        the object id is already in the file system.

        Args:
            bytes: content of the object to be added to the storage
            obj_id: checksums of `bytes` as computed by ID_HASH_ALGO. When
                given, obj_id will be trusted to match bytes. If missing,
                obj_id will be computed on the fly.
        """
        return self.add(bytes, obj_id, check_presence=False)

    def get(self, obj_id):
        """ Retrieve the content of a given object.

        Args:
            obj_id: object id.

        Returns:
            the content of the requested object as bytes.

        Raises:
            ObjNotFoundError: if the requested object is missing.
        """
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        # Open the file and return its content as bytes
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        with _read_obj_file(hex_obj_id, self) as f:
            return f.read()

    def check(self, obj_id):
        """ Perform an integrity check for a given object.

        Verify that the file object is in place and that the gziped content
        matches the object id.

        Args:
            obj_id: object id.

        Raises:
            ObjNotFoundError: if the requested object is missing.
            Error: if the request object is corrupted.
        """
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        hex_obj_id = hashutil.hash_to_hex(obj_id)

        try:
            with gzip.open(self._obj_path(hex_obj_id)) as f:
                length = None
                if ID_HASH_ALGO.endswith('_git'):
                    # if the hashing algorithm is git-like, we need to know the
                    # content size to hash on the fly. Do a first pass here to
                    # compute the size
                    length = 0
                    while True:
                        chunk = f.read(GZIP_BUFSIZ)
                        length += len(chunk)
                        if not chunk:
                            break
                    f.rewind()

                checksums = hashutil._hash_file_obj(f, length,
                                                    algorithms=[ID_HASH_ALGO])
                actual_obj_id = checksums[ID_HASH_ALGO]
                if obj_id != actual_obj_id:
                    raise Error(
                        'Corrupt object %s should have id %s'
                        % (hashutil.hash_to_hex(obj_id),
                           hashutil.hash_to_hex(actual_obj_id))
                    )
        except (OSError, IOError):
            # IOError is for compatibility with older python versions
            raise Error('Corrupt object %s is not a gzip file' % obj_id)

    def get_random(self, batch_size):
        """ Get random ids of existing contents

        This method is used in order to get random ids to perform
        content integrity verifications on random contents.

        Attributes:
            batch_size (int): Number of ids that will be given

        Yields:
            An iterable of ids of contents that are in the current object
            storage.
        """
        def get_random_content(self, batch_size):
            """ Get a batch of content inside a single directory.

            Returns:
                a tuple (batch size, batch).
            """
            dirs = []
            for level in range(len(self.bounds)):
                path = os.path.join(self.root, *dirs)
                dir_list = next(os.walk(path))[1]
                if 'tmp' in dir_list:
                    dir_list.remove('tmp')
                dirs.append(random.choice(dir_list))

            path = os.path.join(self.root, *dirs)
            content_list = next(os.walk(path))[2]
            length = min(batch_size, len(content_list))
            return length, map(hashutil.hex_to_hash,
                               random.sample(content_list, length))

        while batch_size:
            length, it = get_random_content(self, batch_size)
            batch_size = batch_size - length
            yield from it
