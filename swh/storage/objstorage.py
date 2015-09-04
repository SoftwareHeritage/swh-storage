# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import gzip
import os
import shutil
import tempfile

from contextlib import contextmanager

from swh.core import hashutil


ID_HASH_ALGO = 'sha1'
# ID_HASH_ALGO = 'sha1_git'

GZIP_BUFSIZ = 1048576


class Error(Exception):

    def __str__(self):
        return 'storage error on object: %s' % self.args


class DuplicateObjError(Error):

    def __str__(self):
        return 'duplicate object: %s' % self.args


class ObjNotFoundError(Error):

    def __str__(self):
        return 'object not found: %s' % self.args


def _obj_dir(obj_id, root_dir, depth):
    """compute the storage directory of an object

    Args:
        obj_id: object id
        root_dir: object storage root directory
        depth: slicing depth of object IDs in the storage

    see also: `_obj_path`

    """
    if len(obj_id) < depth * 2:
        raise ValueError('object id "%s" is too short for slicing at depth %d'
                         % (obj_id, depth))

    # compute [depth] substrings of [obj_id], each of length 2, starting from
    # the beginning
    id_steps = [obj_id[i*2:i*2+2] for i in range(0, depth)]
    steps = [root_dir] + id_steps

    return os.path.join(*steps)


def _obj_path(obj_id, root_dir, depth):
    """similar to `obj_dir`, but also include the actual object file name in the
    returned path

    """
    return os.path.join(_obj_dir(obj_id, root_dir, depth), obj_id)


@contextmanager
def _write_obj_file(obj_id, root_dir, depth):
    """context manager for writing object files to the object storage

    During writing data are written to a temporary file, which is atomically
    renamed to the right file name after closing. This context manager also
    takes care of (gzip) compressing the data on the fly.

    Yields:
        a file-like object open for writing bytes

    Sample usage:

    with _write_obj_file(obj_id, root_dir, depth) as f:
        f.write(obj_data)

    """
    dir = _obj_dir(obj_id, root_dir, depth)
    if not os.path.isdir(dir):
        os.makedirs(dir)

    path = os.path.join(dir, obj_id)
    tmp_path = path + '.tmp'
    with gzip.GzipFile(tmp_path, 'wb') as f:
        yield f
    os.rename(tmp_path, path)


class ObjStorage:
    """high-level API to manipulate the Software Heritage object storage

    Conceptually, the object storage offers 4 methods:

    - add()           add a new object, returning an object id
    - __contains__()  check if an object is present, by object id
    - get()           retrieve the content of an object, by object id
    - check()         check the integrity of an object, by object id

    Variants of the above methods are implemented by this class, depending on
    how the content of an object is specified (bytes, file-like object, etc.).

    On disk, an object storage is a directory tree containing files named after
    their object IDs. An object ID is a checksum of its content, depending on
    the value of the ID_HASH_ALGO constant (see hashutil for its meaning).

    To avoid directories that contain too many files, the object storage has a
    given depth (default: 3). Each depth level consumes two characters of the
    object id. So for instance a file with (git) SHA1 of
    34973274ccef6ab4dfaaf86599792fa9c3fe4689 will be stored in an object
    storage configured at depth 3 at
    34/97/32/34973274ccef6ab4dfaaf86599792fa9c3fe4689.

    The actual files in the storage are stored in gzipped compressed format.

    Each file can hence be self-verified (on the shell) with something like:

    actual_id=34973274ccef6ab4dfaaf86599792fa9c3fe4689
    expected_id=$(zcat $filename | sha1sum | cut -f 1 -d' ')
    if [ $actual_id != $expected_id ] ; then
       echo "AYEEE, invalid object $actual_id /o\"
    fi

    """

    def __init__(self, root, depth=3):
        """create a proxy object to the object storage

        Args:
            root: object storage root directory
            depth: slicing depth of object IDs in the storage

        """
        if not os.path.isdir(root):
            raise ValueError('obj storage root "%s" is not a directory'
                             % root)

        self._root_dir = root
        self._depth = depth

        self._temp_dir = os.path.join(root, 'tmp')
        if not os.path.isdir(self._temp_dir):
            os.makedirs(self._temp_dir)

    def __obj_dir(self, obj_id):
        """_obj_dir wrapper using this storage configuration"""
        return _obj_dir(obj_id, self._root_dir, self._depth)

    def __obj_path(self, obj_id):
        """_obj_path wrapper using this storage configuration"""
        return _obj_path(obj_id, self._root_dir, self._depth)

    def __contains__(self, obj_id):
        """check whether a given object id is present in the storage or not

        Return:
            True iff the object id is present in the storage

        """
        return os.path.exists(_obj_path(obj_id, self._root_dir, self._depth))

    def add_bytes(self, bytes, obj_id=None, clobber=False):
        """add a new object to the object storage

        Args:
            bytes: content of the object to be added to the storage
            obj_id: checksums of `bytes` as computed by ID_HASH_ALGO. When
                given, obj_id will be trusted to match bytes. If missing,
                obj_id will be computed on the fly.
            clobber (boolean): whether overwriting objects in the storage is
                allowed or not

        Raises:
            DuplicateObjError: if obj_id is already present in the storage,
            unless clobber is True

        """
        if obj_id is None:
            # missing checksum, compute it in memory and write to file
            h = hashutil._new_hash(ID_HASH_ALGO, len(bytes))
            h.update(bytes)
            obj_id = h.hexdigest()

        if not clobber and obj_id in self:
            raise DuplicateObjError(obj_id)

        with _write_obj_file(obj_id,
                             root_dir=self._root_dir,
                             depth=self._depth) as f:
            f.write(bytes)

        return obj_id

    def add_file(self, f, length, obj_id=None, clobber=False):
        """similar to `add_bytes`, but add the content of file-like object f to the
        object storage

        add_file will read the file content only once, and avoid storing all of
        it in memory

        """
        if obj_id is None:
            # unknkown object id: work on temp file, compute checksum as we go,
            # mv temp file into place
            (tmp, tmp_path) = tempfile.mkstemp(dir=self._temp_dir)
            try:
                t = os.fdopen(tmp, 'wb')
                tz = gzip.GzipFile(fileobj=t)
                sums = hashutil._hash_file_obj(f, length,
                                               algorithms=[ID_HASH_ALGO],
                                               chunk_cb=lambda b: tz.write(b))
                tz.close()
                t.close()

                obj_id = sums[ID_HASH_ALGO]
                if not clobber and obj_id in self:
                    raise DuplicateObjError(obj_id)

                dir = self.__obj_dir(obj_id)
                if not os.path.isdir(dir):
                    os.makedirs(dir)
                path = os.path.join(dir, obj_id)
                os.rename(tmp_path, path)
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
        else:
            # known object id: write to .new file, rename
            if not clobber and obj_id in self:
                raise DuplicateObjError(obj_id)

            with _write_obj_file(obj_id,
                                 root_dir=self._root_dir,
                                 depth=self._depth) as obj:
                shutil.copyfileobj(f, obj)

        return obj_id

    @contextmanager
    def get_file_obj(self, obj_id):
        """context manager to read the content of an object

        Args:
            obj_id: object id

        Yields:
            a file-like object open for reading (bytes)

        Raises:
            ObjNotFoundError: if the requested object is missing

        Sample usage:

           with objstorage.get_file_obj(obj_id) as f:
               do_something(f.read())

        """
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        path = self.__obj_path(obj_id)
        with gzip.GzipFile(path, 'rb') as f:
            yield f

    def get_bytes(self, obj_id):
        """retrieve the content of a given object

        Args:
            obj_id: object id

        Returns:
            the content of the requested objects as bytes

        Raises:
            ObjNotFoundError: if the requested object is missing

        """
        with self.get_file_obj(obj_id) as f:
            return f.read()

    def _get_file_path(self, obj_id):
        """retrieve the path of a given object in the objects storage

        Note that the path point to a gzip-compressed file, so you need
        gzip.open() or equivalent to get the actual object content.

        Args:
            obj_id: object id

        Returns:
            a file path pointing into the object storage

        Raises:
            ObjNotFoundError: if the requested object is missing

        """
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        return self.__obj_path(obj_id)

    def check(self, obj_id):
        """integrity check for a given object

        verify that the file object is in place, and that the gzipped content
        matches the object id

        Args:
            obj_id: object id

        Raises:
            ObjNotFoundError: if the requested object is missing
            Error: if the requested object is corrupt

        """
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        try:
            with gzip.open(self.__obj_path(obj_id)) as f:
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
                    raise Error('corrupt object %s should have id %s' %
                                (obj_id, actual_obj_id))
        except OSError:
            raise Error('corrupt object %s is not a gzip file' % obj_id)
