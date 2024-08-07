# Copyright (C) 2024 The Software Heritage developers

# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import enum
import itertools
from typing import Dict, Iterable, Iterator, List, Optional, Tuple
from uuid import UUID

import attr
import psycopg2.errors
from psycopg2.extras import execute_values

from swh.core.db import BaseDb
from swh.core.statsd import statsd
from swh.model.swhids import ExtendedObjectType, ExtendedSWHID
from swh.storage.exc import StorageArgumentException

METRIC_QUERY_TOTAL = "swh_storage_masking_queried_total"
METRIC_LIST_REQUESTS_TOTAL = "swh_storage_masking_list_requests_total"
METRIC_LISTED_TOTAL = "swh_storage_masking_listed_total"
METRIC_MASKED_TOTAL = "swh_storage_masking_masked_total"


class DuplicateRequest(StorageArgumentException):
    pass


class RequestNotFound(StorageArgumentException):
    pass


class MaskedState(enum.Enum):
    """Value recording "how much" an object associated to a masking request is masked"""

    # TODO: is this needed? Can be used to record objects that have been masked
    # in the past but are not anymore?
    VISIBLE = enum.auto()
    """The object is visible"""
    DECISION_PENDING = enum.auto()
    """The object is temporarily masked until the request is reviewed"""
    RESTRICTED = enum.auto()
    """The access to the object is restricted permanently"""


@attr.s
class MaskedStatus:
    """Return value when requesting if an object is masked"""

    state = attr.ib(type=MaskedState)
    request = attr.ib(type=UUID)


@attr.s
class MaskingRequest:
    """A request for masking a set of objects"""

    id = attr.ib(type=UUID)
    """Unique id for the request (will be returned to requesting clients)"""

    slug = attr.ib(type=str)
    """Unique, human-readable id for the request (for administrative interactions)"""

    date = attr.ib(type=datetime.datetime)
    """Date the request was received"""

    reason = attr.ib(type=str)
    """Why the request was made"""  # TODO: should this be stored here?


@attr.s
class MaskingRequestHistory:
    request = attr.ib(type=UUID)
    """id of the masking request"""
    date = attr.ib(type=datetime.datetime)
    """Date the history entry has been added"""
    message = attr.ib(type=str)
    """Free-form history information (e.g. "policy decision made")"""


@attr.s
class MaskedObject:
    request_slug = attr.ib(type=str)
    swhid = attr.ib(type=ExtendedSWHID)
    state = attr.ib(type=MaskedState)


@attr.s
class DisplayName:
    """A request for masking a set of objects"""

    original_email = attr.ib(type=bytes)
    """Email on revision/release objects to match before applying the display name"""

    display_name = attr.ib(type=bytes)
    """Full name, usually of the form ``Name <email>``, used for display queries"""


class MaskingDb(BaseDb):
    # we started with 192, because this used to be part of the main storage db
    current_version = 194

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.conn.autocommit = True


class MaskingAdmin(MaskingDb):
    def create_request(self, slug: str, reason: str) -> MaskingRequest:
        """Record a new masking request

        Arguments:
          slug: human-readable unique identifier for the request
          reason: free-form text recording why the request was made

        Raises:
          DuplicateRequest when the slug already exists
        """
        cur = self.cursor()

        try:
            cur.execute(
                """
                INSERT INTO masking_request (slug, reason)
                VALUES (%s, %s) RETURNING id, date
                """,
                (slug, reason),
            )
        except psycopg2.errors.UniqueViolation:
            raise DuplicateRequest(slug)

        res = cur.fetchone()
        assert res is not None, "PostgreSQL returned an inconsistent result"
        id, date = res
        return MaskingRequest(id=id, date=date, slug=slug, reason=reason)

    def find_request(self, slug: str) -> Optional[MaskingRequest]:
        """Find a masking request using its slug

        Returns: :const:`None` if a request with the given slug doesn't exist
        """
        cur = self.cursor()
        cur.execute(
            """
            SELECT id, slug, date, reason
            FROM masking_request
            WHERE slug = %s
            """,
            (slug,),
        )

        res = cur.fetchone()
        if not res:
            return None
        id, slug, date, reason = res
        return MaskingRequest(id=id, date=date, slug=slug, reason=reason)

    def find_request_by_id(self, id: UUID) -> Optional[MaskingRequest]:
        """Find a masking request using its id

        Returns: :const:`None` if a request with the given request doesn't exist
        """
        cur = self.cursor()
        cur.execute(
            """
            SELECT id, slug, date, reason
            FROM masking_request
            WHERE id = %s
            """,
            (id,),
        )

        res = cur.fetchone()
        if not res:
            return None
        id, slug, date, reason = res
        return MaskingRequest(id=id, date=date, slug=slug, reason=reason)

    def get_requests(
        self, include_cleared_requests: bool = False
    ) -> List[Tuple[MaskingRequest, int]]:
        """Get known requests

        Args:
            include_cleared_requests: also include requests with no associated
            masking states
        """
        cur = self.cursor()

        query = """SELECT id, slug, date, reason, COUNT(object_id) AS mask_count
                     FROM masking_request
                     LEFT JOIN masked_object ON (request = id)
                    GROUP BY id"""
        if not include_cleared_requests:
            query += " HAVING COUNT(object_id) > 0"
        query += " ORDER BY date DESC"
        cur.execute(query)
        result = []
        for id, slug, date, reason, mask_count in cur:
            result.append(
                (MaskingRequest(id=id, slug=slug, date=date, reason=reason), mask_count)
            )
        return result

    def set_object_state(
        self, request_id: UUID, new_state: MaskedState, swhids: List[ExtendedSWHID]
    ):
        """Within the request with the given id, record the state of the given
        objects as ``new_state``.

        This creates entries or updates them as appropriate.

        Raises: :exc:`RequestNotFound` if the request is not found.
        """
        cur = self.cursor()

        cur.execute("SELECT 1 FROM masking_request WHERE id = %s", (request_id,))
        if cur.fetchone() is None:
            raise RequestNotFound(request_id)

        execute_values(
            cur,
            """
            INSERT INTO masked_object (object_id, object_type, request, state)
            VALUES %s
            ON CONFLICT (object_id, object_type, request)
            DO UPDATE SET state = EXCLUDED.state
            """,
            (
                (
                    swhid.object_id,
                    swhid.object_type.name.lower(),
                    request_id,
                    new_state.name.lower(),
                )
                for swhid in swhids
            ),
        )

    def get_states_for_request(
        self, request_id: UUID
    ) -> Dict[ExtendedSWHID, MaskedState]:
        """Get the state of objects associated with the given request.

        Raises :exc:`RequestNotFound` if the request is not found.
        """

        cur = self.cursor()

        cur.execute("SELECT 1 FROM masking_request WHERE id = %s", (request_id,))
        if cur.fetchone() is None:
            raise RequestNotFound(request_id)

        result = {}
        cur.execute(
            """SELECT object_id, object_type, state
                 FROM masked_object
                WHERE request = %s""",
            (request_id,),
        )
        for object_id, object_type, state in cur:
            swhid = ExtendedSWHID(
                object_id=object_id,
                object_type=ExtendedObjectType[object_type.upper()],
            )
            result[swhid] = MaskedState[state.upper()]
        return result

    def find_masks(self, swhids: List[ExtendedSWHID]) -> List[MaskedObject]:
        """Lookup the masking state and associated requests for the given SWHIDs."""

        cur = self.cursor()
        result = []
        for (
            object_id,
            object_type,
            request_slug,
            state,
        ) in psycopg2.extras.execute_values(
            cur,
            """SELECT object_id, object_type, slug, state
                 FROM masked_object
                INNER JOIN (VALUES %s) v(object_id, object_type)
                USING (object_id, object_type)
                 LEFT JOIN masking_request ON (masking_request.id = request)
                ORDER BY object_type, object_id, masking_request.date DESC
            """,
            ((swhid.object_id, swhid.object_type.name.lower()) for swhid in swhids),
            template="(%s, %s::extended_object_type)",
            fetch=True,
        ):
            swhid = ExtendedSWHID(
                object_id=object_id, object_type=ExtendedObjectType[object_type.upper()]
            )
            result.append(
                MaskedObject(
                    request_slug=request_slug,
                    swhid=swhid,
                    state=MaskedState[state.upper()],
                )
            )
        return result

    def delete_masks(self, request_id: UUID) -> None:
        """Remove all masking states for the given request.

        Raises: :exc:`RequestNotFound` if the request is not found.
        """

        cur = self.cursor()
        cur.execute("SELECT 1 FROM masking_request WHERE id = %s", (request_id,))
        if cur.fetchone() is None:
            raise RequestNotFound(request_id)

        cur.execute("DELETE FROM masked_object WHERE request = %s", (request_id,))

    def record_history(self, request_id: UUID, message: str) -> MaskingRequestHistory:
        """Add an entry to the history of the given request.

        Raises: :exc:`RequestNotFound` if the request is not found.
        """
        cur = self.cursor()
        try:
            cur.execute(
                """
                INSERT INTO masking_request_history (request, message)
                VALUES (%s, %s) RETURNING date
                """,
                (request_id, message),
            )
        except psycopg2.errors.ForeignKeyViolation:
            raise RequestNotFound(request_id)

        res = cur.fetchone()
        assert res is not None, "PostgreSQL returned an inconsistent result"

        return MaskingRequestHistory(request=request_id, date=res[0], message=message)

    def get_history(self, request_id: UUID) -> List[MaskingRequestHistory]:
        """Get the history of a given request.

        Raises: :exc:`RequestNotFound` if the request if not found.
        """
        cur = self.cursor()

        cur.execute("SELECT 1 FROM masking_request WHERE id = %s", (request_id,))
        if cur.fetchone() is None:
            raise RequestNotFound(request_id)

        cur.execute(
            """SELECT date, message
                    FROM masking_request_history
                WHERE request = %s
                ORDER BY date DESC""",
            (request_id,),
        )
        records = []
        for date, message in cur:
            records.append(
                MaskingRequestHistory(request=request_id, date=date, message=message)
            )
        return records

    def set_display_name(self, original_email: bytes, display_name: bytes) -> None:
        """Updates the display name of the person identified by the email address."""
        cur = self.cursor()

        cur.execute(
            """
            INSERT INTO display_name (original_email, display_name)
            VALUES (%s, %s)
            ON CONFLICT (original_email) DO UPDATE
            SET display_name=EXCLUDED.display_name
            """,
            (original_email, display_name),
        )

    def set_display_names(
        self, display_names: Iterable[Tuple[bytes, bytes]], clear=False
    ) -> None:
        """Insert a list of display names of persons identified by their email address.

        If 'clear' is True, empty the existing display names before inserting
        the new entries.

        """
        with self.transaction() as cur:
            if clear:
                cur.execute(
                    """
                    DELETE FROM display_name
                    """
                )
            execute_values(
                cur,
                """
                INSERT INTO display_name (original_email, display_name)
                VALUES %s
                ON CONFLICT (original_email)
                DO UPDATE SET display_name=EXCLUDED.display_name
                """,
                (
                    (
                        original_email,
                        display_name,
                    )
                    for (original_email, display_name) in display_names
                ),
            )


class MaskingQuery(MaskingDb):
    def swhids_are_masked(
        self, swhids: List[ExtendedSWHID]
    ) -> Dict[ExtendedSWHID, List[MaskedStatus]]:
        """Checks which objects in the list are masked.

        Returns:
            For each masked object, a list of :class:`MaskedStatus` objects
            where the State is not :const:`MaskedState.VISIBLE`.
        """

        cur = self.cursor()

        statsd.increment(METRIC_QUERY_TOTAL, len(swhids))

        ret: Dict[ExtendedSWHID, List[MaskedStatus]] = {}

        for object_id, object_type, request_id, state in psycopg2.extras.execute_values(
            cur,
            """
            SELECT object_id, object_type, request, state
            FROM masked_object
            INNER JOIN (VALUES %s) v(object_id, object_type)
            USING (object_id, object_type)
            WHERE state != 'visible'
            """,
            (
                (
                    swhid.object_id,
                    swhid.object_type.name.lower(),
                )
                for swhid in swhids
            ),
            template="(%s, %s::extended_object_type)",
            fetch=True,
        ):
            swhid = ExtendedSWHID(
                object_id=object_id,
                object_type=ExtendedObjectType[object_type.upper()],
            )
            if swhid not in ret:
                ret[swhid] = []

            ret[swhid].append(
                MaskedStatus(request=request_id, state=MaskedState[state.upper()])
            )

        if ret:
            statsd.increment(METRIC_MASKED_TOTAL, len(ret))
        return ret

    def display_name(self, original_emails: List[bytes]) -> Dict[bytes, bytes]:
        """Returns the display name of the person identified by each ``original_email``,
        if any.
        """
        cur = self.cursor()

        ret: Dict[bytes, bytes] = {}

        for original_email, display_name in psycopg2.extras.execute_values(
            cur,
            """
            SELECT original_email, display_name
            FROM display_name
            INNER JOIN (VALUES %s) v(original_email)
            USING (original_email)
            """,
            [(email,) for email in original_emails],
            fetch=True,
        ):
            ret[original_email] = display_name

        return ret

    def iter_masked_swhids(self) -> Iterator[Tuple[ExtendedSWHID, List[MaskedStatus]]]:
        """Returns the complete list of masked SWHIDs.

        SWHIDs are guaranteed to be unique in the iterator.

        Yields:
            For each masked object, its SWHID and a list of :class:`MaskedStatus`
            objects where the State is not :const:`MaskedState.VISIBLE`.
        """

        cur = self.cursor()

        statsd.increment(METRIC_LIST_REQUESTS_TOTAL, 1)

        cur.execute(
            """
            SELECT object_id, object_type, request, state
            FROM masked_object
            WHERE state != 'visible'
            ORDER BY object_id, object_type
            """
        )

        count = 0

        for (object_id, object_type), statuses in itertools.groupby(
            cur, key=lambda t: (t[0], t[1])
        ):
            count += 1
            swhid = ExtendedSWHID(
                object_id=object_id,
                object_type=ExtendedObjectType[object_type.upper()],
            )
            yield (
                swhid,
                [
                    MaskedStatus(request=request_id, state=MaskedState[state.upper()])
                    for (_, _, request_id, state) in statuses
                ],
            )

        statsd.increment(METRIC_LISTED_TOTAL, count)
