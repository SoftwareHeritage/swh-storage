# Copyright (C) 2020-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, Iterable, List, Optional, Tuple, Union, cast
import warnings

from swh.model.hashutil import DEFAULT_ALGORITHMS
from swh.model.model import Content, MissingData, Sha1
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.factory import get_objstorage
from swh.objstorage.interface import CompositeObjId
from swh.storage.interface import HashDict, StorageInterface

from .exc import StorageArgumentException


class ObjStorage:
    """Objstorage collaborator in charge of adding objects to
    the objstorage.

    """

    def __init__(self, storage: StorageInterface, objstorage_config: Optional[Dict]):
        self.storage = storage
        self.warn_usage = False
        if objstorage_config is None:
            objstorage_config = {"cls": "noop"}
            self.warn_usage = True
        self.objstorage = get_objstorage(**objstorage_config)

    def __getattr__(self, key):
        if key in ("objstorage", "warn_usage", "storage"):
            raise AttributeError(key)
        if self.warn_usage:
            warnings.warn(
                "Actually using a NoopObjstorage; this is most probably a configuration error.",
            )
        return getattr(self.objstorage, key)

    def content_get(self, obj_id: Union[Sha1, HashDict]) -> Optional[bytes]:
        """Retrieve data associated to the content from the objstorage

        Args:
            content: content identitier

        Returns:
            associated content's data if any, None otherwise.

        """
        if self.warn_usage:
            warnings.warn(
                "Actually using a NoopObjstorage; this is most probably a configuration error.",
            )
        hashes: HashDict
        if isinstance(obj_id, bytes):
            warnings.warn(
                'Identifying contents by sha1 instead of hash dicts `{"sha1": b"..."}` '
                "is deprecated.",
                DeprecationWarning,
                stacklevel=3,  # Report to the caller of swh/storage/*/storage.py
            )
            hashes = {"sha1": obj_id}
        else:
            hashes = obj_id
        if set(hashes) < DEFAULT_ALGORITHMS:
            # If some hashes are missing, query the database to fill blanks
            candidates = self.storage.content_find(hashes)
            if candidates:
                # There may be more than one in case of collision; but we cannot
                # do anything about it here
                hashes = cast(HashDict, candidates[0].hashes())
            else:
                # we will pass the partial hash dict to the objstorage, which
                # will do the best it can with it. Usually, this will return None,
                # as objects missing from the storage DB are unlikely to be present in the
                # objstorage
                pass
        try:
            data = self.objstorage.get(hashes)
        except ObjNotFoundError:
            data = None

        return data

    def content_add(self, contents: Iterable[Content]) -> Dict:
        """Add contents to the objstorage.

        Args:
            contents: List of contents to add1

        Returns:
            The summary dict of content and content bytes added to the
            objstorage.

        """
        if self.warn_usage:
            warnings.warn(
                "Actually using a NoopObjstorage; this is most probably a configuration error.",
            )
        content_pairs: List[Tuple[CompositeObjId, bytes]] = []
        for content in contents:
            try:
                content = content.with_data()
            except MissingData:
                raise StorageArgumentException("Missing data") from None
            assert content.data is not None
            content_pairs.append((cast(CompositeObjId, content.hashes()), content.data))
        summary = self.objstorage.add_batch(content_pairs)
        return {
            "content:add": summary["object:add"],
            "content:add:bytes": summary["object:add:bytes"],
        }
