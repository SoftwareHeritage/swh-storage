# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import random

from swh.model.discovery import (
    SAMPLE_SIZE,
    ArchiveDiscoveryInterface,
    BaseDiscoveryGraph,
    Sample,
)
from swh.model.from_disk import model

logger = logging.getLogger(__name__)


class RandomDirSamplingDiscoveryGraph(BaseDiscoveryGraph):
    """Use a random sampling using only directories.

    This allows us to find a statistically good spread of entries in the graph
    with a smaller population than using all types of entries. When there are
    no more directories, only contents or skipped contents are undecided if any
    are left: we send them directly to the storage since they should be few and
    their structure flat."""

    async def get_sample(self) -> Sample:
        if self._undecided_directories:
            if len(self._undecided_directories) <= SAMPLE_SIZE:
                return Sample(
                    contents=set(),
                    skipped_contents=set(),
                    directories=set(self._undecided_directories),
                )
            sample = random.sample(self._undecided_directories, SAMPLE_SIZE)
            directories = {o for o in sample}
            return Sample(
                contents=set(), skipped_contents=set(), directories=directories
            )

        contents = set()
        skipped_contents = set()

        for sha1 in self.undecided:
            obj = self._all_contents[sha1]
            obj_type = obj.object_type
            if obj_type == model.Content.object_type:
                contents.add(sha1)
            elif obj_type == model.SkippedContent.object_type:
                skipped_contents.add(sha1)
            else:
                raise TypeError(f"Unexpected object type {obj_type}")

        return Sample(
            contents=contents, skipped_contents=skipped_contents, directories=set()
        )


async def filter_known_objects(archive: ArchiveDiscoveryInterface):
    """Filter ``archive``'s ``contents``, ``skipped_contents`` and ``directories``
    to only return those that are unknown to the SWH archive using a discovery
    algorithm."""
    contents = archive.contents
    skipped_contents = archive.skipped_contents
    directories = archive.directories

    contents_count = len(contents)
    skipped_contents_count = len(skipped_contents)
    directories_count = len(directories)

    graph = RandomDirSamplingDiscoveryGraph(contents, skipped_contents, directories)

    while graph.undecided:
        sample = await graph.get_sample()
        await graph.do_query(archive, sample)

    contents = [c for c in contents if c.sha1_git in graph.unknown]
    skipped_contents = [c for c in skipped_contents if c.sha1_git in graph.unknown]
    directories = [c for c in directories if c.id in graph.unknown]

    logger.debug(
        "Filtered out %d contents, %d skipped contents and %d directories",
        contents_count - len(contents),
        skipped_contents_count - len(skipped_contents),
        directories_count - len(directories),
    )

    return (contents, skipped_contents, directories)
