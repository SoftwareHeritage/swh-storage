# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pickle

import requests

from swh.core.serializers import msgpack_dumps, msgpack_loads, SWHJSONDecoder


def encode_data(data):
    return msgpack_dumps(data)


def decode_response(response):
    content_type = response.headers['content-type']

    if content_type.startswith('application/x-msgpack'):
        r = msgpack_loads(response.content)
    elif content_type.startswith('application/json'):
        r = response.json(cls=SWHJSONDecoder)
    else:
        raise ValueError('Wrong content type `%s` for API response'
                         % content_type)

    return r


class RemoteStorage():
    """Proxy to a remote storage API"""
    def __init__(self, base_url):
        self.base_url = base_url
        self.session = requests.Session()

    def url(self, endpoint):
        return '%s%s' % (self.base_url, endpoint)

    def post(self, endpoint, data):
        response = self.session.post(
            self.url(endpoint),
            data=encode_data(data),
            headers={'content-type': 'application/x-msgpack'},
        )

        # XXX: this breaks language-independence and should be
        # replaced by proper unserialization
        if response.status_code == 400:
            raise pickle.loads(decode_response(response))

        return decode_response(response)

    def get(self, endpoint, data=None):
        response = self.session.get(
            self.url(endpoint),
            params=data,
        )

        if response.status_code == 404:
            return None

        # XXX: this breaks language-independence and should be
        # replaced by proper unserialization
        if response.status_code == 400:
            raise pickle.loads(decode_response(response))

        else:
            return decode_response(response)

    def content_add(self, content):
        return self.post('content/add', {'content': content})

    def content_missing(self, content, key_hash='sha1'):
        return self.post('content/missing', {'content': content,
                                             'key_hash': key_hash})

    def content_exist(self, content):
        return self.post('content/present', {'content': content})

    def content_find_occurrence(self, content):
        return self.post('content/occurrence', {'content': content})

    def directory_add(self, directories):
        return self.post('directory/add', {'directories': directories})

    def directory_missing(self, directories):
        return self.post('directory/missing', {'directories': directories})

    def directory_get(self, directory, recursive=False):
        return self.get('directory', {'directory': directory,
                                      'recursive': recursive})

    def revision_get(self, revisions):
        return self.post('revision', {'revisions': revisions})

    def revision_add(self, revisions):
        return self.post('revision/add', {'revisions': revisions})

    def revision_missing(self, revisions):
        return self.post('revision/missing', {'revisions': revisions})

    def release_add(self, releases):
        return self.post('release/add', {'releases': releases})

    def release_missing(self, releases):
        return self.post('release/missing', {'releases': releases})

    def occurrence_add(self, occurrences):
        return self.post('occurrence/add', {'occurrences': occurrences})

    def origin_get(self, origin):
        origin = self.get('origin', origin)

        if not origin:
            return None
        else:
            return origin['id']

    def origin_add_one(self, origin):
        return self.post('origin', {'origin': origin})

    def fetch_history_start(self, origin_id):
        return self.post('fetch_history/start', {'origin_id': origin_id})

    def fetch_history_end(self, fetch_history_id, data):
        return self.post('fetch_history/end',
                         {'fetch_history_id': fetch_history_id,
                          'data': data})

    def fetch_history_get(self, fetch_history_id):
        return self.get('fetch_history', {'id': fetch_history_id})

    def entity_add(self, entities):
        return self.post('entity/add', {'entities': entities})

    def entity_get_from_lister_metadata(self, entities):
        return self.post('entity/from_lister_metadata', {'entities': entities})

    def stat_counters(self):
        return self.get('stat/counters')
