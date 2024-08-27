# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
from functools import partial
import logging
from typing import Any, Callable
from typing import Counter as CounterT
from typing import Dict, List, Optional, Tuple, TypeVar, Union, cast
from uuid import uuid4

try:
    from systemd.daemon import notify
except ImportError:
    notify = None

from swh.core.statsd import statsd
from swh.journal.serializers import kafka_to_value
from swh.model.hashutil import hash_to_hex
from swh.model.model import (
    BaseContent,
    BaseModel,
    Content,
    Directory,
    ExtID,
    HashableObject,
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
from swh.storage.exc import HashCollision, StorageArgumentException
from swh.storage.interface import StorageInterface
from swh.storage.utils import remove_keys

logger = logging.getLogger(__name__)

GRAPH_OPERATIONS_METRIC = "swh_graph_replayer_operations_total"
GRAPH_DURATION_METRIC = "swh_graph_replayer_duration_seconds"


OBJECT_CONVERTERS: Dict[str, Callable[[Dict], BaseModel]] = {
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
    "extid": ExtID.from_dict,
}
# Deprecated, for BW compat only.
object_converter_fn = OBJECT_CONVERTERS


OBJECT_FIXERS = {
    # drop the metadata field from the revision (if any); this field is
    # about to be dropped from the data model (in favor of
    # raw_extrinsic_metadata) and there can be bogus values in the existing
    # journal (metadata with \0000 in it)
    "revision": partial(remove_keys, keys=("metadata",)),
}


class ModelObjectDeserializer:
    """A swh.journal object deserializer that checks object validity and reports
    invalid objects

    The deserializer will directly produce BaseModel objects from journal
    objects representations.

    If validation is activated and the object is hashable, it will check if the
    computed hash matches the identifier of the object.

    If the object is invalid and a 'reporter' function is given, it will be
    called with 2 arguments::

      reporter(object_id, journal_msg)

    Where 'object_id' is a string representation of the object identifier (from
    the journal message), and 'journal_msg' is the row message (bytes)
    retrieved from the journal.

    If 'raise_on_error' is True, a 'StorageArgumentException' exception is
    raised.

    If 'known_mismatched_hashes' is given, it must be a tuple of triplets
    (object_type, object_id, expected_id) listing objects that store invalid
    hash (object_id) instead of the computed expected_id, but should not be
    discarded (i.e. they should be replicated by the replayer despite being
    invalid).

    Typical usage::

      deserializer = ModelObjectDeserializer(validate=True, reporter=reporter_cb)
      client = get_journal_client(
          cls="kafka", value_deserializer=deserializer, **cfg)

    """

    def __init__(
        self,
        validate: bool = True,
        raise_on_error: bool = False,
        reporter: Optional[Callable[[str, bytes], None]] = None,
        known_mismatched_hashes: Optional[Tuple[Tuple[str, bytes, bytes]]] = None,
    ):
        self.validate = validate
        self.reporter = reporter
        self.raise_on_error = raise_on_error
        self.known_mismatched_hashes = known_mismatched_hashes

    def convert(self, object_type: str, msg: bytes) -> Optional[BaseModel]:
        dict_repr = kafka_to_value(msg)
        if object_type in OBJECT_FIXERS:
            dict_repr = OBJECT_FIXERS[object_type](dict_repr)
        try:
            obj = OBJECT_CONVERTERS[object_type](dict_repr)
        except ValueError as exc:
            # we do not catch AttributeTypeError here since these are (most
            # likely) a clue of something very wrong is occurring, so better crash
            error_msg = f"Unable to create model object {object_type}: {repr(exc)}"
            self.report_failure(msg, (object_type, dict_repr))
            if self.raise_on_error:
                raise StorageArgumentException(error_msg)
            return None

        if self.validate:
            if isinstance(obj, HashableObject):
                cid = obj.compute_hash()
                if obj.id != cid:
                    error_msg = (
                        f"Object has id {hash_to_hex(obj.id)}, "
                        f"but it should be {hash_to_hex(cid)}: {obj}."
                    )
                    if (
                        self.known_mismatched_hashes is None
                        or (object_type, obj.id, cid)
                        not in self.known_mismatched_hashes
                    ):
                        logger.error(error_msg)
                        self.report_failure(msg, obj)
                        if self.raise_on_error:
                            raise StorageArgumentException(error_msg)
                        return None
                    else:
                        logger.warning(
                            error_msg + " Known exception, replicating it anyway."
                        )
        return obj

    def report_failure(
        self, msg: bytes, obj: Union[BaseModel, Tuple[str, Dict[str, Any]]]
    ):
        if self.reporter:
            oid: str
            if isinstance(obj, tuple):
                object_type, dict_repr = obj
                if "id" in dict_repr:
                    uid = dict_repr["id"]
                    assert isinstance(uid, bytes)
                    oid = f"{object_type}:{uid.hex()}"
                else:
                    oid = f"{object_type}:uuid:{uuid4()}"
            elif hasattr(obj, "swhid"):
                swhid = obj.swhid()
                oid = str(swhid)
            elif isinstance(obj, HashableObject):
                uid = obj.compute_hash()
                oid = f"{obj.object_type}:{uid.hex()}"
            else:
                oid = f"{obj.object_type}:uuid:{uuid4()}"

            self.reporter(oid, msg)


def process_replay_objects(
    all_objects: Dict[str, List[BaseModel]], *, storage: StorageInterface
) -> None:
    for object_type, objects in all_objects.items():
        logger.debug("Inserting %s %s objects", len(objects), object_type)
        with statsd.timed(GRAPH_DURATION_METRIC, tags={"object_type": object_type}):
            _insert_objects(object_type, objects, storage)
            storage.flush()
        statsd.increment(
            GRAPH_OPERATIONS_METRIC, len(objects), tags={"object_type": object_type}
        )
    if notify:
        notify("WATCHDOG=1")


ContentType = TypeVar("ContentType", bound=BaseContent)


def collision_aware_content_add(
    contents: List[ContentType],
    content_add_fn: Callable[[List[ContentType]], Dict[str, int]],
) -> Dict[str, int]:
    """Add contents to storage. If a hash collision is detected, an error is
       logged. Then this adds the other non colliding contents to the storage.

    Args:
        content_add_fn: Storage content callable
        contents: List of contents or skipped contents to add to storage

    """
    if not contents:
        return {}
    colliding_content_hashes: List[Dict[str, Any]] = []
    results: CounterT[str] = Counter()
    while True:
        try:
            results.update(content_add_fn(contents))
        except HashCollision as e:
            logger.debug("Hash collision detected: %s", e)
            colliding_content_hashes.append(
                {
                    "algo": e.algo,
                    "hash": e.hash_id,  # hex hash id
                    "objects": e.colliding_contents,  # hex hashes
                }
            )
            colliding_hashes = e.colliding_content_hashes()
            logger.debug("Dropping colliding hashes: %s", colliding_hashes)

            # Drop the colliding contents from the transaction
            contents = [c for c in contents if c.hashes() not in colliding_hashes]
        else:
            # Successfully added contents, we are done
            break
    if colliding_content_hashes:
        for collision in colliding_content_hashes:
            logger.error("Collision detected: %(collision)s", {"collision": collision})
    return dict(results)


def _insert_objects(
    object_type: str, objects: List[BaseModel], storage: StorageInterface
) -> None:
    """Insert objects of type object_type in the storage."""
    if object_type not in OBJECT_CONVERTERS:
        logger.warning("Received a series of %s, this should not happen", object_type)
        return

    method = getattr(storage, f"{object_type}_add")
    if object_type == "skipped_content":
        method = partial(collision_aware_content_add, content_add_fn=method)
    elif object_type == "content":
        method = partial(
            collision_aware_content_add, content_add_fn=storage.content_add_metadata
        )
    elif object_type in ("origin_visit", "origin_visit_status"):
        origins: List[Origin] = []
        for obj in cast(List[Union[OriginVisit, OriginVisitStatus]], objects):
            origins.append(Origin(url=obj.origin))
        storage.origin_add(origins)
    elif object_type == "raw_extrinsic_metadata":
        emds = cast(List[RawExtrinsicMetadata], objects)
        authorities = {emd.authority for emd in emds}
        fetchers = {emd.fetcher for emd in emds}
        storage.metadata_authority_add(list(authorities))
        storage.metadata_fetcher_add(list(fetchers))
    method(objects)
