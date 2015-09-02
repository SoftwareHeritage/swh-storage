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


# ID_HASH_ALGO = 'sha1'
ID_HASH_ALGO = 'sha1_git'

GZIP_BUFSIZ = 1048576


class ObjStorageError(Exception):
    pass


class DuplicateObjError(ObjStorageError):
    pass


class ObjNotFoundError(ObjStorageError):
    pass


class ObjIntegrityError(ObjStorageError):
    pass


def _obj_dir(obj_id, root_dir, depth):
    """compute the directory (up to, but excluding the actual object file name) of
    an object carrying a given object id, to be sliced at a given depth, and
    rooted at root_dir

    see also: _obj_path

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
    """similar to obj_dir, but also include the actual object file name in the
    returned path

    """
    return os.path.join(_obj_dir(obj_id, root_dir, depth), obj_id)


@contextmanager
def _write_obj_file(obj_id, root_dir, depth):
    """context manager for writing object files to the object storage. It yiels a
    file-like object open for writing (bytes). During writing data are written
    to a temporary file, which is atomically renamed to the right file name
    after closing. This context manager also takes care of (gzip) compressing
    the data on the fly.

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

    On disk, an object storage is a directory tree containing files named after
    their object IDs. An object ID is a salted sha1 checksums, salted in the
    same way of Git blob objects, i.e., prefixing the string "blob
    CONTENT_LENGTH\0" to the actual content, and computing the sha1 of the
    obtained data.

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

        - root is the root directory of the object storage
        - depth is the directory slicing depth applied to object IDs

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

    def has(self, obj_id):
        """check whether a given object id is present in the storage or not

        return a boolean

        """
        return os.path.exists(_obj_path(obj_id, self._root_dir, self._depth))

    def add_bytes(self, bytes, obj_id=None, clobber=False):
        """add a new object to the object storage, return its identifier

        obj_id, if given, should be the checksums of bytes as computed by
        ID_HASH_ALGO. When given, obj_id will be trusted to match bytes. If
        missing, obj_id will be computed on the fly.

        if obj_id is already present, the DuplicateObjError exception will be
        raised unless clobber=True is passed

        """
        if obj_id is None:
            # missing checksum, compute it in memory and write to file
            h = hashutil._new_hash(ID_HASH_ALGO, len(bytes))
            h.update(bytes)
            obj_id = h.hexdigest()

        if not clobber and self.has(obj_id):
            raise DuplicateObjError(obj_id)

        with _write_obj_file(obj_id,
                             root_dir=self._root_dir,
                             depth=self._depth) as f:
            f.write(bytes)

    def add_file(self, f, length, obj_id=None, clobber=False):
        """similar to add_bytes, but add the content of file-like object f to the
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
                if not clobber and self.has(obj_id):
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
            if not clobber and self.has(obj_id):
                raise DuplicateObjError(obj_id)

            with _write_obj_file(obj_id,
                                 root_dir=self._root_dir,
                                 depth=self._depth) as obj:
                shutil.copyfileobj(f, obj)

    def check(self, obj_id):
        """integrity check for a given object

        verify that the file object is in place (i.e., has(obj_id)==True), and
        that the gzipped content matches the object id

        raise exceptions: ObjNotFoundError, ObjIntegrityError

        """
        if not self.has(obj_id):
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
                    raise ObjIntegrityError(obj_id, actual_obj_id)
        except OSError:
            raise ObjIntegrityError(obj_id)
