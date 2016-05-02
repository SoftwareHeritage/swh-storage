# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import pickle

import requests

from requests.exceptions import ConnectionError
from ...exc import StorageAPIError
from ...api.common import (decode_response,
                           encode_data_client as encode_data)


class RemoteObjStorage():
    """ Proxy to a remote object storage.

    This class allows to connect to an object storage server via
    http protocol.

    Attributes:
        base_url (string): The url of the server to connect. Must end
            with a '/'
        session: The session to send requests.
    """
    def __init__(self, base_url):
        self.base_url = base_url
        self.session = requests.Session()

    def url(self, endpoint):
        return '%s%s' % (self.base_url, endpoint)

    def post(self, endpoint, data):
        try:
            response = self.session.post(
                self.url(endpoint),
                data=encode_data(data),
                headers={'content-type': 'application/x-msgpack'},
            )
        except ConnectionError as e:
            print(str(e))
            raise StorageAPIError(e)

        # XXX: this breaks language-independence and should be
        # replaced by proper unserialization
        if response.status_code == 400:
            raise pickle.loads(decode_response(response))

        return decode_response(response)

    def content_add(self, bytes, obj_id=None):
        """ Add a new object to the object storage.

        Args:
            bytes: content of the object to be added to the storage.
            obj_id: checksums of `bytes` as computed by ID_HASH_ALGO. When
                given, obj_id will be trusted to match bytes. If missing,
                obj_id will be computed on the fly.

        """
        return self.post('content/add', {'bytes': bytes, 'obj_id': obj_id})

    def content_get(self, obj_id):
        """ Retrieve the content of a given object.

        Args:
            obj_id: The id of the object.

        Returns:
            The content of the requested objects as bytes.

        Raises:
            ObjNotFoundError: if the requested object is missing
        """
        return self.post('content/get', {'obj_id': obj_id})

    def content_check(self, obj_id):
        """ Integrity check for a given object

        verify that the file object is in place, and that the gzipped content
        matches the object id

        Args:
            obj_id: The id of the object.

        Raises:
            ObjNotFoundError: if the requested object is missing
            Error: if the requested object is corrupt
        """
        self.post('content/check', {'obj_id': obj_id})
