# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pickle
import requests

from requests.exceptions import ConnectionError

from swh.objstorage.api.common import (decode_response,
                                       encode_data_client as encode_data)

from ..exc import StorageAPIError


class RemoteStorage():
    """Proxy to a remote storage API"""
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

    def content_add(self, content):
        return self.post('content/add', {'content': content})

    def content_missing(self, content, key_hash='sha1'):
        return self.post('content/missing', {'content': content,
                                             'key_hash': key_hash})

    def content_missing_per_sha1(self, contents):
        return self.post('content/missing/sha1', {'contents': contents})

    def content_get(self, content):
        return self.post('content/data', {'content': content})

    def content_get_metadata(self, content):
        return self.post('content/metadata', {'content': content})

    def content_find(self, content):
        return self.post('content/present', {'content': content})

    def content_find_provenance(self, content):
        return self.post('content/provenance', {'content': content})

    def directory_add(self, directories):
        return self.post('directory/add', {'directories': directories})

    def directory_missing(self, directories):
        return self.post('directory/missing', {'directories': directories})

    def directory_get(self, directories):
        return self.post('directory', dict(directories=directories))

    def directory_ls(self, directory, recursive=False):
        return self.get('directory/ls', {'directory': directory,
                                         'recursive': recursive})

    def revision_get(self, revisions):
        return self.post('revision', {'revisions': revisions})

    def revision_get_by(self, origin_id, branch_name, timestamp, limit=None):
        return self.post('revision/by', dict(origin_id=origin_id,
                                             branch_name=branch_name,
                                             timestamp=timestamp,
                                             limit=limit))

    def revision_log(self, revisions, limit=None):
        return self.post('revision/log', {'revisions': revisions,
                                          'limit': limit})

    def revision_log_by(self, origin_id, branch_name, timestamp, limit=None):
        return self.post('revision/logby', {'origin_id': origin_id,
                                            'branch_name': branch_name,
                                            'timestamp': timestamp,
                                            'limit': limit})

    def revision_shortlog(self, revisions, limit=None):
        return self.post('revision/shortlog', {'revisions': revisions,
                                               'limit': limit})

    def cache_content_revision_add(self, revisions):
        return self.post('cache/content_revision', {'revisions': revisions})

    def cache_content_get_all(self):
        return self.get('cache/contents')

    def cache_content_get(self, content):
        return self.post('cache/content', {'content': content})

    def cache_revision_origin_add(self, origin, visit):
        return self.post('cache/revision_origin', {'origin': origin,
                                                   'visit': visit})

    def revision_add(self, revisions):
        return self.post('revision/add', {'revisions': revisions})

    def revision_missing(self, revisions):
        return self.post('revision/missing', {'revisions': revisions})

    def release_add(self, releases):
        return self.post('release/add', {'releases': releases})

    def release_get(self, releases):
        return self.post('release', {'releases': releases})

    def release_get_by(self, origin_id, limit=None):
        return self.post('release/by', dict(origin_id=origin_id,
                                            limit=limit))

    def release_missing(self, releases):
        return self.post('release/missing', {'releases': releases})

    def object_find_by_sha1_git(self, ids):
        return self.post('object/find_by_sha1_git', {'ids': ids})

    def occurrence_get(self, origin_id):
        return self.post('occurrence', {'origin_id': origin_id})

    def occurrence_add(self, occurrences):
        return self.post('occurrence/add', {'occurrences': occurrences})

    def origin_get(self, origin):
        return self.post('origin/get', {'origin': origin})

    def origin_add_one(self, origin):
        return self.post('origin/add', {'origin': origin})

    def origin_visit_add(self, origin, ts):
        return self.post('origin/visit/add', {'origin': origin, 'ts': ts})

    def origin_visit_update(self, origin, visit_id, status, metadata=None):
        return self.post('origin/visit/update', {'origin': origin,
                                                 'visit_id': visit_id,
                                                 'status': status,
                                                 'metadata': metadata})

    def origin_visit_get(self, origin):
        return self.post('origin/visit/get', {'origin': origin})

    def origin_visit_get_by(self, origin, visit):
        return self.post('origin/visit/getby', {'origin': origin,
                                                'visit': visit})

    def person_get(self, person):
        return self.post('person', {'person': person})

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

    def entity_get(self, uuid):
        return self.post('entity/get', {'uuid': uuid})

    def entity_get_one(self, uuid):
        return self.get('entity', {'uuid': uuid})

    def entity_get_from_lister_metadata(self, entities):
        return self.post('entity/from_lister_metadata', {'entities': entities})

    def stat_counters(self):
        return self.get('stat/counters')

    def directory_entry_get_by_path(self, directory, paths):
        return self.post('directory/path', dict(directory=directory,
                                                paths=paths))

    def content_mimetype_add(self, mimetypes, conflict_update=False):
        return self.post('content_mimetype/add', {
            'mimetypes': mimetypes,
            'conflict_update': conflict_update,
        })

    def content_mimetype_missing(self, mimetypes):
        return self.post('content_mimetype/missing', {'mimetypes': mimetypes})

    def content_mimetype_get(self, ids):
        return self.post('content_mimetype', {'ids': ids})

    def content_language_add(self, languages, conflict_update=False):
        return self.post('content_language/add', {
            'languages': languages,
            'conflict_update': conflict_update,
        })

    def content_language_missing(self, languages):
        return self.post('content_language/missing', {'languages': languages})

    def content_language_get(self, ids):
        return self.post('content_language', {'ids': ids})
