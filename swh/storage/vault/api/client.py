# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pickle
import requests

from swh.core import hashutil
from swh.storage.exc import StorageAPIError
from swh.objstorage.api.common import (decode_response,
                                       encode_data_client as encode_data)


class RemoteVaultCache():
    """Client to the Software Heritage vault cache."""

    def __init__(self, base_url):
        self.base_url = base_url if base_url.endswith('/') else base_url + '/'
        self.session = requests.Session()

    def url(self, endpoint):
        return'%s%s' % (self.base_url, endpoint)

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

    def get(self, endpoint, data=None):
        try:
            response = self.session.get(
                self.url(endpoint),
                params=data,
            )
        except ConnectionError as e:
            print(str(e))
            raise StorageAPIError(e)

        if response.status_code == 404:
            return None

        # XXX: this breaks language-independence and should be
        # replaced by proper unserialization
        if response.status_code == 400:
            raise pickle.loads(decode_response(response))
        else:
            return decode_response(response)

    def directory_ls(self):
        return self.get('vault/directory/')

    def directory_get(self, obj_id):
        return self.get('vault/directory/%s/' % (hashutil.hash_to_hex(obj_id)))

    def directory_cook(self, obj_id):
        return self.post('vault/directory/%s/' % hashutil.hash_to_hex(obj_id),
                         data={})
