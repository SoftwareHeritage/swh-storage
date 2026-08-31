# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random
from typing import Callable, Dict, Generic, Iterable, List, Tuple, TypeVar

import attr

from swh.model.model import (
    Content,
    OriginVisit,
    OriginVisitStatus,
    RawExtrinsicMetadata,
    SkippedContent,
)

from .in_memory import BaseRow, InMemoryCqlRunner, InMemoryStorage, Table
from .objstorage import ObjStorage

TRow = TypeVar("TRow", bound=BaseRow)


class NoopTable(Table[TRow], Generic[TRow]):
    def insert(self, row: TRow) -> None:
        pass

    def delete(self, predicate: Callable[[TRow], bool]) -> None:
        pass


class NoopCqlRunner(InMemoryCqlRunner):
    _Table = NoopTable

    def content_index_add_concurrent(
        self, algo: str, contents: List[Tuple[int, Content]]
    ) -> None:
        pass

    def content_get_tokens_from_single_algo(
        self, algo: str, hashes: List[bytes]
    ) -> Iterable[int]:
        # parent implementation accesses a defaultdict, which inserts a key
        # with empty list as value
        yield from ()

    def skipped_content_index_add_one(
        self, algo: str, content: SkippedContent, token: int
    ) -> None:
        pass

    def skipped_content_get_tokens_from_single_hash(
        self, algo: str, hash_: bytes
    ) -> Iterable[int]:
        # parent implementation accesses a defaultdict, which inserts a key
        # with empty list as value
        yield from ()


class NoopStorage(InMemoryStorage):
    def reset(self):
        self._cql_runner = NoopCqlRunner()
        self.objstorage = ObjStorage(self, self.objstorage_config)

    def origin_visit_add(self, visits: List[OriginVisit]) -> Iterable[OriginVisit]:
        # parent implementation checks visits[].origin is known
        # loaders read the unique visit id from the result
        yield from (
            attr.evolve(visit, visit=random.randrange(2**128)) for visit in visits
        )

    def origin_visit_status_add(
        self, visit_statuses: List[OriginVisitStatus]
    ) -> Dict[str, int]:
        # parent implementation checks visit_statuses[].origin
        return {}

    def raw_extrinsic_metadata_add(
        self, metadata: List[RawExtrinsicMetadata]
    ) -> Dict[str, int]:
        # parent implementation checks metadata[].authority and metadata[].fetcher
        # are known
        return {}
