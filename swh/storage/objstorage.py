# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import os
import shutil
import tempfile

from contextlib import contextmanager

from swh.core import hashutil


ID_HASH_ALGO = 'sha1_git'


def _obj_dir(obj_id, root_dir, depth):
    """compute the directory (up to, but excluding the actual object file name) of
    an object carrying a given object id, to be sliced at a given depth, and
    rooted at root_dir

    see also: _obj_path

    """
    if len(obj_id) < depth * 2:
        raise ValueError('object id %s is too short for slicing at depth %d'
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
def new_obj_file(obj_id, root_dir, depth):
    """context manager for adding new object files to the object storage. It yiels
    a file-like object open for writing (bytes). During writing data are
    written to a temporary file, which is atomically renamed to the right file
    name after closing.

    Sample usage:

    with new_obj_file(obj_id, root_dir, depth) as f:
        f.write(obj_data)

    """
    dir = _obj_dir(obj_id, root_dir, depth)
    if not os.path.isdir(dir):
        os.makedirs(dir)

    path = os.path.join(dir, obj_id)
    tmp_path = path + '.tmp'
    with open(tmp_path, 'wb') as f:
        yield f

    os.rename(tmp_path, path)


class ObjStorage:

    """high-level API to manipulate the Software Heritage object storage

    """

    def __init__(self, root, depth=3):
        """create a proxy object to the object storage

        - root is the root directory of the object storage
        - depth is the directory slicing depth applied to object IDs

        """
        if not os.path.isdir(root):
            raise ValueError('obj storage root dir %s is not a directory'
                             % root)

        self._root_dir = root
        self._depth = depth

        self._temp_dir = os.path.join(root, 'tmp')
        if not os.path.isdir(self._temp_dir):
            os.makedirs(self._temp_dir)

    def add_bytes(self, bytes, obj_id=None):
        """add a new object to the object storage, return its identifier

        obj_id, if given, should be the checksums of bytes as computed by
        ID_HASH_ALGO. When given, obj_id will be trusted to match bytes. If
        missing, obj_id will be computed on the fly.

        """
        if obj_id is None:
            # missing checksum, compute it in memory and write to file
            h = hashutil._new_hash(ID_HASH_ALGO, len(bytes))
            h.update(bytes)
            obj_id = h.hexdigest()

        with new_obj_file(obj_id,
                          root_dir=self._root_dir,
                          depth=self._depth) as f:
            f.write(bytes)

    def add_file(self, f, length, obj_id=None):
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
                tmp = os.fdopen(tmp, 'wb')
                sums = hashutil._hash_file_obj(f, length,
                                               algorithms=[ID_HASH_ALGO],
                                               chunk_cb=lambda b: tmp.write(b))
                tmp.close()

                obj_id = sums[ID_HASH_ALGO]
                dir = _obj_dir(obj_id, self._root_dir, self._depth)
                if not os.path.isdir(dir):
                    os.makedirs(dir)
                path = os.path.join(dir, obj_id)
                os.rename(tmp_path, path)
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
        else:
            # known object id: write to .new file, rename
            with new_obj_file(obj_id,
                              root_dir=self._root_dir,
                              depth=self._depth) as obj:
                shutil.copyfileobj(f, obj)
