# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import warnings

from swh.core.api import SWHRemoteAPI

from ..exc import StorageAPIError


class RemoteStorage(SWHRemoteAPI):
    """Proxy to a remote storage API"""
    api_exception = StorageAPIError

    def check_config(self, *, check_write):
        return self.post('check_config', {'check_write': check_write})

    def content_add(self, content):
        return self.post('content/add', {'content': content})

    def content_add_metadata(self, content):
        return self.post('content/add_metadata', {'content': content})

    def content_update(self, content, keys=[]):
        return self.post('content/update', {'content': content,
                                            'keys': keys})

    def content_missing(self, content, key_hash='sha1'):
        return self.post('content/missing', {'content': content,
                                             'key_hash': key_hash})

    def content_missing_per_sha1(self, contents):
        return self.post('content/missing/sha1', {'contents': contents})

    def skipped_content_missing(self, contents):
        return self.post('content/skipped/missing', {'contents': contents})

    def content_get(self, content):
        return self.post('content/data', {'content': content})

    def content_get_metadata(self, content):
        return self.post('content/metadata', {'content': content})

    def content_get_range(self, start, end, limit=1000):
        return self.post('content/range', {'start': start,
                                           'end': end,
                                           'limit': limit})

    def content_find(self, content):
        return self.post('content/present', {'content': content})

    def directory_add(self, directories):
        return self.post('directory/add', {'directories': directories})

    def directory_missing(self, directories):
        return self.post('directory/missing', {'directories': directories})

    def directory_ls(self, directory, recursive=False):
        return self.post('directory/ls', {'directory': directory,
                                          'recursive': recursive})

    def revision_get(self, revisions):
        return self.post('revision', {'revisions': revisions})

    def revision_log(self, revisions, limit=None):
        return self.post('revision/log', {'revisions': revisions,
                                          'limit': limit})

    def revision_shortlog(self, revisions, limit=None):
        return self.post('revision/shortlog', {'revisions': revisions,
                                               'limit': limit})

    def revision_add(self, revisions):
        return self.post('revision/add', {'revisions': revisions})

    def revision_missing(self, revisions):
        return self.post('revision/missing', {'revisions': revisions})

    def release_add(self, releases):
        return self.post('release/add', {'releases': releases})

    def release_get(self, releases):
        return self.post('release', {'releases': releases})

    def release_missing(self, releases):
        return self.post('release/missing', {'releases': releases})

    def object_find_by_sha1_git(self, ids):
        return self.post('object/find_by_sha1_git', {'ids': ids})

    def snapshot_add(self, snapshots, origin=None, visit=None):
        if origin:
            assert visit
            (origin, visit, snapshots) = (snapshots, origin, visit)
            warnings.warn("arguments 'origin' and 'visit' of snapshot_add "
                          "are deprecated since v0.0.131, please use "
                          "snapshot_add([snapshot]) + "
                          "origin_visit_update(origin, visit, "
                          "snapshot=snapshot['id']) instead.",
                          DeprecationWarning)
            return self.post('snapshot/add', {
                'origin': origin, 'visit': visit, 'snapshots': snapshots,
            })
        else:
            assert not visit
            return self.post('snapshot/add', {
                'snapshots': snapshots,
            })

    def snapshot_get(self, snapshot_id):
        return self.post('snapshot', {
            'snapshot_id': snapshot_id
        })

    def snapshot_get_by_origin_visit(self, origin, visit):
        return self.post('snapshot/by_origin_visit', {
            'origin': origin,
            'visit': visit
        })

    def snapshot_get_latest(self, origin, allowed_statuses=None):
        return self.post('snapshot/latest', {
            'origin': origin,
            'allowed_statuses': allowed_statuses
        })

    def snapshot_count_branches(self, snapshot_id):
        return self.post('snapshot/count_branches', {
            'snapshot_id': snapshot_id
        })

    def snapshot_get_branches(self, snapshot_id, branches_from=b'',
                              branches_count=1000, target_types=None):
        return self.post('snapshot/get_branches', {
            'snapshot_id': snapshot_id,
            'branches_from': branches_from,
            'branches_count': branches_count,
            'target_types': target_types
        })

    def origin_get(self, origins=None, *, origin=None):
        if origin is None:
            if origins is None:
                raise TypeError('origin_get expected 1 argument')
        else:
            assert origins is None
            origins = origin
            warnings.warn("argument 'origin' of origin_get was renamed "
                          "to 'origins' in v0.0.123.",
                          DeprecationWarning)
        return self.post('origin/get', {'origins': origins})

    def origin_search(self, url_pattern, offset=0, limit=50, regexp=False,
                      with_visit=False):
        return self.post('origin/search', {'url_pattern': url_pattern,
                                           'offset': offset,
                                           'limit': limit,
                                           'regexp': regexp,
                                           'with_visit': with_visit})

    def origin_count(self, url_pattern, regexp=False, with_visit=False):
        return self.post('origin/count', {'url_pattern': url_pattern,
                                          'regexp': regexp,
                                          'with_visit': with_visit})

    def origin_get_range(self, origin_from=1, origin_count=100):
        return self.post('origin/get_range', {'origin_from': origin_from,
                                              'origin_count': origin_count})

    def origin_add(self, origins):
        return self.post('origin/add_multi', {'origins': origins})

    def origin_add_one(self, origin):
        return self.post('origin/add', {'origin': origin})

    def origin_visit_add(self, origin, date, *, ts=None):
        if ts is None:
            if date is None:
                raise TypeError('origin_visit_add expected 2 arguments.')
        else:
            assert date is None
            warnings.warn("argument 'ts' of origin_visit_add was renamed "
                          "to 'date' in v0.0.109.",
                          DeprecationWarning)
            date = ts
        return self.post('origin/visit/add', {'origin': origin, 'date': date})

    def origin_visit_update(self, origin, visit_id, status=None,
                            metadata=None, snapshot=None):
        return self.post('origin/visit/update', {'origin': origin,
                                                 'visit_id': visit_id,
                                                 'status': status,
                                                 'metadata': metadata,
                                                 'snapshot': snapshot})

    def origin_visit_upsert(self, visits):
        return self.post('origin/visit/upsert', {'visits': visits})

    def origin_visit_get(self, origin, last_visit=None, limit=None):
        return self.post('origin/visit/get', {
            'origin': origin, 'last_visit': last_visit, 'limit': limit})

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

    def stat_counters(self):
        return self.get('stat/counters')

    def directory_entry_get_by_path(self, directory, paths):
        return self.post('directory/path', dict(directory=directory,
                                                paths=paths))

    def tool_add(self, tools):
        return self.post('tool/add', {'tools': tools})

    def tool_get(self, tool):
        return self.post('tool/data', {'tool': tool})

    def origin_metadata_add(self, origin_id, ts, provider, tool, metadata):
        return self.post('origin/metadata/add', {'origin_id': origin_id,
                                                 'ts': ts,
                                                 'provider': provider,
                                                 'tool': tool,
                                                 'metadata': metadata})

    def origin_metadata_get_by(self, origin_id, provider_type=None):
        return self.post('origin/metadata/get', {
            'origin_id': origin_id,
            'provider_type': provider_type
        })

    def metadata_provider_add(self, provider_name, provider_type, provider_url,
                              metadata):
        return self.post('provider/add', {'provider_name': provider_name,
                                          'provider_type': provider_type,
                                          'provider_url': provider_url,
                                          'metadata': metadata})

    def metadata_provider_get(self, provider_id):
        return self.post('provider/get', {'provider_id': provider_id})

    def metadata_provider_get_by(self, provider):
        return self.post('provider/getby', {'provider': provider})

    def diff_directories(self, from_dir, to_dir, track_renaming=False):
        return self.post('algos/diff_directories',
                         {'from_dir': from_dir,
                          'to_dir': to_dir,
                          'track_renaming': track_renaming})

    def diff_revisions(self, from_rev, to_rev, track_renaming=False):
        return self.post('algos/diff_revisions',
                         {'from_rev': from_rev,
                          'to_rev': to_rev,
                          'track_renaming': track_renaming})

    def diff_revision(self, revision, track_renaming=False):
        return self.post('algos/diff_revision',
                         {'revision': revision,
                          'track_renaming': track_renaming})
