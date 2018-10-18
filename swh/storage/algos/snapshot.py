# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def snapshot_get_all_branches(storage, snapshot_id):
    """Get all the branches for a given snapshot

    Args:
        storage (swh.storage.storage.Storage): the storage instance
        snapshot_id (bytes): the snapshot's identifier
    Returns:
        dict: a dict with two keys:
            * **id**: identifier of the snapshot
            * **branches**: a dict of branches contained in the snapshot
              whose keys are the branches' names.
    """
    ret = storage.snapshot_get(snapshot_id)

    if not ret:
        return

    next_branch = ret.pop('next_branch', None)
    while next_branch:
        data = storage.snapshot_get_branches(snapshot_id,
                                             branches_from=next_branch)
        ret['branches'].update(data['branches'])
        next_branch = data.get('next_branch')

    return ret
