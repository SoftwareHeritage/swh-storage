# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, Iterable, Optional

from swh.model.model import Content, MissingData
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.factory import get_objstorage
from swh.storage.interface import Sha1

from .exc import StorageArgumentException


class ObjStorage:
    """Objstorage collaborator in charge of adding objects to
    the objstorage.

    """

    def __init__(self, objstorage_config: Dict):
        self.objstorage = get_objstorage(**objstorage_config)

    def __getattr__(self, key):
        if key == "objstorage":
            raise AttributeError(key)
        return getattr(self.objstorage, key)

    def content_get(self, obj_id: Sha1) -> Optional[bytes]:
        """Retrieve data associated to the content from the objstorage

        Args:
            content: content identitier

        Returns:
            associated content's data if any, None otherwise.

        """
        try:
            data = self.objstorage.get(obj_id)
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
        try:
            contents = [c.with_data() for c in contents]
        except MissingData:
            raise StorageArgumentException("Missing data") from None
        summary = self.objstorage.add_batch({cont.sha1: cont.data for cont in contents})
        return {
            "content:add": summary["object:add"],
            "content:add:bytes": summary["object:add:bytes"],
        }
