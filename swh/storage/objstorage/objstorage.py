# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import gzip
import os
import shutil
import tempfile

from contextlib import contextmanager

from ..exc import ObjNotFoundError, Error
from swh.core import hashutil


ID_HASH_ALGO = 'sha1'
# ID_HASH_ALGO = 'sha1_git'

GZIP_BUFSIZ = 1048576

DIR_MODE = 0o755
FILE_MODE = 0o644


def _obj_dir(hex_obj_id, root_dir, depth):
    """compute the storage directory of an object

    Args:
        hex_obj_id: object id as hexlified string
        root_dir: object storage root directory
        depth: slicing depth of object IDs in the storage

    see also: `_obj_path`

    """
    if len(hex_obj_id) < depth * 2:
        raise ValueError('object id "%s" is too short for slicing at depth %d'
                         % (hex_obj_id, depth))

    # compute [depth] substrings of [obj_id], each of length 2, starting from
    # the beginning
    id_steps = [hex_obj_id[i*2:i*2+2] for i in range(0, depth)]
    steps = [root_dir] + id_steps

    return os.path.join(*steps)


def _obj_path(hex_obj_id, root_dir, depth):
    """similar to `obj_dir`, but also include the actual object file name in the
    returned path

    """
    return os.path.join(_obj_dir(hex_obj_id, root_dir, depth), hex_obj_id)


@contextmanager
def _write_obj_file(hex_obj_id, root_dir, depth):
    """context manager for writing object files to the object storage

    During writing data are written to a temporary file, which is atomically
    renamed to the right file name after closing. This context manager also
    takes care of (gzip) compressing the data on the fly.

    Yields:
        a file-like object open for writing bytes

    Sample usage:

    with _write_obj_file(hex_obj_id, root_dir, depth) as f:
        f.write(obj_data)

    """
    dir = _obj_dir(hex_obj_id, root_dir, depth)
    if not os.path.isdir(dir):
        os.makedirs(dir, DIR_MODE, exist_ok=True)

    path = os.path.join(dir, hex_obj_id)
    (tmp, tmp_path) = tempfile.mkstemp(suffix='.tmp', prefix='hex_obj_id.',
                                       dir=dir)
    tmp_f = os.fdopen(tmp, 'wb')
    with gzip.GzipFile(filename=tmp_path, fileobj=tmp_f) as f:
        yield f
    tmp_f.close()
    os.chmod(tmp_path, FILE_MODE)
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
            os.makedirs(self._temp_dir, DIR_MODE, exist_ok=True)

    def __obj_dir(self, hex_obj_id):
        """_obj_dir wrapper using this storage configuration"""
        return _obj_dir(hex_obj_id, self._root_dir, self._depth)

    def __obj_path(self, hex_obj_id):
        """_obj_path wrapper using this storage configuration"""
        return _obj_path(hex_obj_id, self._root_dir, self._depth)

    def __contains__(self, obj_id):
        """check whether a given object id is present in the storage or not

        Return:
            True iff the object id is present in the storage

        """
        hex_obj_id = hashutil.hash_to_hex(obj_id)

        return os.path.exists(_obj_path(hex_obj_id, self._root_dir,
                                        self._depth))

    def add_bytes(self, bytes, obj_id=None):
        """add a new object to the object storage

        Args:
            bytes: content of the object to be added to the storage
            obj_id: checksums of `bytes` as computed by ID_HASH_ALGO. When
                given, obj_id will be trusted to match bytes. If missing,
                obj_id will be computed on the fly.

        """
        if obj_id is None:
            # missing checksum, compute it in memory and write to file
            h = hashutil._new_hash(ID_HASH_ALGO, len(bytes))
            h.update(bytes)
            obj_id = h.digest()

        if obj_id in self:
            return obj_id

        hex_obj_id = hashutil.hash_to_hex(obj_id)

        # object is either absent, or present but overwrite is requested
        with _write_obj_file(hex_obj_id,
                             root_dir=self._root_dir,
                             depth=self._depth) as f:
            f.write(bytes)

        return obj_id

    def add_file(self, f, length, obj_id=None):
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
                if obj_id in self:
                    return obj_id

                hex_obj_id = hashutil.hash_to_hex(obj_id)

                dir = self.__obj_dir(hex_obj_id)
                if not os.path.isdir(dir):
                    os.makedirs(dir, DIR_MODE, exist_ok=True)
                path = os.path.join(dir, hex_obj_id)

                os.chmod(tmp_path, FILE_MODE)
                os.rename(tmp_path, path)
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
        else:
            # known object id: write to .new file, rename
            if obj_id in self:
                return obj_id

            hex_obj_id = hashutil.hash_to_hex(obj_id)

            with _write_obj_file(hex_obj_id,
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

        hex_obj_id = hashutil.hash_to_hex(obj_id)

        path = self.__obj_path(hex_obj_id)
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

        hex_obj_id = hashutil.hash_to_hex(obj_id)

        return self.__obj_path(hex_obj_id)

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

        hex_obj_id = hashutil.hash_to_hex(obj_id)

        try:
            with gzip.open(self.__obj_path(hex_obj_id)) as f:
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
        except (OSError, IOError):
            # IOError is for compatibility with older python versions
            raise Error('corrupt object %s is not a gzip file' % obj_id)

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
            for root, _dirs, files in os.walk(self._root_dir):
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
