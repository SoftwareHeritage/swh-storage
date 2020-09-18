# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import Any, Callable, Dict, Iterable, List

try:
    from systemd.daemon import notify
except ImportError:
    notify = None

from swh.core.statsd import statsd
from swh.model.model import (
    BaseContent,
    BaseModel,
    Content,
    Directory,
    MetadataAuthority,
    MetadataFetcher,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    RawExtrinsicMetadata,
    Release,
    Revision,
    SkippedContent,
    Snapshot,
)
from swh.storage.exc import HashCollision
from swh.storage.fixer import fix_objects

logger = logging.getLogger(__name__)

GRAPH_OPERATIONS_METRIC = "swh_graph_replayer_operations_total"
GRAPH_DURATION_METRIC = "swh_graph_replayer_duration_seconds"


object_converter_fn: Dict[str, Callable[[Dict], BaseModel]] = {
    "origin": Origin.from_dict,
    "origin_visit": OriginVisit.from_dict,
    "origin_visit_status": OriginVisitStatus.from_dict,
    "snapshot": Snapshot.from_dict,
    "revision": Revision.from_dict,
    "release": Release.from_dict,
    "directory": Directory.from_dict,
    "content": Content.from_dict,
    "skipped_content": SkippedContent.from_dict,
    "metadata_authority": MetadataAuthority.from_dict,
    "metadata_fetcher": MetadataFetcher.from_dict,
    "raw_extrinsic_metadata": RawExtrinsicMetadata.from_dict,
}


def process_replay_objects(all_objects, *, storage):
    for (object_type, objects) in all_objects.items():
        logger.debug("Inserting %s %s objects", len(objects), object_type)
        with statsd.timed(GRAPH_DURATION_METRIC, tags={"object_type": object_type}):
            _insert_objects(object_type, objects, storage)
        statsd.increment(
            GRAPH_OPERATIONS_METRIC, len(objects), tags={"object_type": object_type}
        )
    if notify:
        notify("WATCHDOG=1")


def collision_aware_content_add(
    content_add_fn: Callable[[Iterable[Any]], None], contents: List[BaseContent]
) -> None:
    """Add contents to storage. If a hash collision is detected, an error is
       logged. Then this adds the other non colliding contents to the storage.

    Args:
        content_add_fn: Storage content callable
        contents: List of contents or skipped contents to add to storage

    """
    if not contents:
        return
    colliding_content_hashes: List[Dict[str, Any]] = []
    while True:
        try:
            content_add_fn(contents)
        except HashCollision as e:
            colliding_content_hashes.append(
                {
                    "algo": e.algo,
                    "hash": e.hash_id,  # hex hash id
                    "objects": e.colliding_contents,  # hex hashes
                }
            )
            colliding_hashes = e.colliding_content_hashes()
            # Drop the colliding contents from the transaction
            contents = [c for c in contents if c.hashes() not in colliding_hashes]
        else:
            # Successfully added contents, we are done
            break
    if colliding_content_hashes:
        for collision in colliding_content_hashes:
            logger.error("Collision detected: %(collision)s", {"collision": collision})


def _insert_objects(object_type: str, objects: List[Dict], storage) -> None:
    """Insert objects of type object_type in the storage.

    """
    objects = fix_objects(object_type, objects)

    if object_type == "content":
        # for bw compat, skipped content should now be delivered in the skipped_content
        # topic
        contents: List[BaseContent] = []
        skipped_contents: List[BaseContent] = []
        for content in objects:
            c = BaseContent.from_dict(content)
            if isinstance(c, SkippedContent):
                logger.warning(
                    "Received a series of skipped_content in the "
                    "content topic, this should not happen anymore"
                )
                skipped_contents.append(c)
            else:
                contents.append(c)
        collision_aware_content_add(storage.skipped_content_add, skipped_contents)
        collision_aware_content_add(storage.content_add_metadata, contents)
    elif object_type == "skipped_content":
        skipped_contents = [SkippedContent.from_dict(obj) for obj in objects]
        collision_aware_content_add(storage.skipped_content_add, skipped_contents)
    elif object_type in ("origin_visit", "origin_visit_status"):
        origins: List[Origin] = []
        converter_fn = object_converter_fn[object_type]
        model_objs = []
        for obj in objects:
            origins.append(Origin(url=obj["origin"]))
            model_objs.append(converter_fn(obj))
        storage.origin_add(origins)
        method = getattr(storage, f"{object_type}_add")
        method(model_objs)
    elif object_type in ("directory", "revision", "release", "snapshot", "origin",):
        method = getattr(storage, object_type + "_add")
        method([object_converter_fn[object_type](o) for o in objects])
    else:
        logger.warning("Received a series of %s, this should not happen", object_type)
