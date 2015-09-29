# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json

import requests

from swh.core.hashutil import hash_to_hex
from swh.core.json import SWHJSONDecoder, SWHJSONEncoder


class RemoteStorage():
    """Proxy to a remote storage API"""
    def __init__(self, base_url):
        self.base_url = base_url

    def url(self, endpoint):
        return '%s%s' % (self.base_url, endpoint)

    def post(self, endpoint, data):
        raw_data = json.dumps(data, cls=SWHJSONEncoder)
        response = requests.post(
            self.url(endpoint),
            data=raw_data,
            headers={'content-type': 'application/json; charset=utf8'},
        )

        return response.json(cls=SWHJSONDecoder)

    def post_files(self, endpoint, data, files):
        raw_data = json.dumps(data, cls=SWHJSONEncoder)
        files['metadata'] = raw_data
        response = requests.post(
            self.url(endpoint),
            files=files,
        )

        return response.json(cls=SWHJSONDecoder)

    def get(self, endpoint, data):
        response = requests.get(
            self.url(endpoint),
            params=data,
        )

        if response.status_code == 404:
            return None
        else:
            return response.json(cls=SWHJSONDecoder)

    def content_add(self, content):
        files = {}
        for file in content:
            if file.get('status', 'visible') != 'visible':
                continue
            file_id = hash_to_hex(file['sha1'])
            files[file_id] = file.pop('data')
        return self.post_files('content/add', {'content': content}, files)

    def content_missing(self, content, key_hash='sha1'):
        return self.post('content/missing', {'content': content,
                                             'key_hash': key_hash})

    def content_present(self, content):
        return self.post('content/present', {'content': content})

    def directory_add(self, directories):
        return self.post('directory/add', {'directories': directories})

    def directory_missing(self, directories):
        return self.post('directory/missing', {'directories': directories})

    def directory_get(self, directory):
        return [tuple(entry)
                for entry in self.get('directory', {'directory': directory})]

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
