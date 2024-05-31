# Copyright (C) 2024 The Software Heritage developers

# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import enum
import logging
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from uuid import UUID

import attr
import psycopg2.errors
from psycopg2.extras import execute_values

from swh.core.db import BaseDb
from swh.core.statsd import statsd
from swh.storage.proxies.masking.db import DuplicateRequest, RequestNotFound

METRIC_QUERY_TOTAL = "swh_storage_blocking_queried_total"
METRIC_BLOCKED_TOTAL = "swh_storage_blocking_blocked_total"
KNOWN_SUFFIXES = ("git", "svn", "hg", "cvs", "CVS")

logger = logging.getLogger(__name__)


class BlockingState(enum.Enum):
    """Value recording "how much" an url associated to a blocking request is blocked"""

    NON_BLOCKED = enum.auto()
    """The origin url can be ingested/updated"""
    DECISION_PENDING = enum.auto()
    """Ingestion from origin url is temporarily blocked until the request is reviewed"""
    BLOCKED = enum.auto()
    """Ingestion from origin url is permanently blocked"""


@attr.s
class BlockingStatus:
    """Return value when requesting if an origin url ingestion is blocked"""

    state = attr.ib(type=BlockingState)
    request = attr.ib(type=UUID)


@attr.s
class BlockingRequest:
    """A request for blocking a set of origins from being ingested"""

    id = attr.ib(type=UUID)
    """Unique id for the request (will be returned to requesting clients)"""
    slug = attr.ib(type=str)
    """Unique, human-readable id for the request (for administrative interactions)"""
    date = attr.ib(type=datetime.datetime)
    """Date the request was received"""
    reason = attr.ib(type=str)
    """Why the request was made"""  # TODO: should this be stored here?


@attr.s
class RequestHistory:
    request = attr.ib(type=UUID)
    """id of the blocking request"""
    date = attr.ib(type=datetime.datetime)
    """Date the history entry has been added"""
    message = attr.ib(type=str)
    """Free-form history information (e.g. "policy decision made")"""


@attr.s
class BlockingLogEntry:
    url = attr.ib(type=str)
    """origin url that have been blocked"""
    url_match = attr.ib(type=str)
    """url matching pattern that caused the blocking of the origin url"""
    request = attr.ib(type=UUID)
    """id of the blocking request"""
    date = attr.ib(type=datetime.datetime)
    """Date the blocking event occurred"""
    state = attr.ib(type=BlockingState)
    """Blocking state responsible for the blocking event"""


@attr.s
class BlockedOrigin:
    request_slug = attr.ib(type=str)
    url_pattern = attr.ib(type=str)
    state = attr.ib(type=BlockingState)


class BlockingDb(BaseDb):
    current_version = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.conn.autocommit = True


def get_urls_to_check(url: str) -> Tuple[List[str], List[str]]:
    """Get the entries to check in the database for the given `url`, in order.

    Exact matching is done on the following strings, in order:
     - the url with any trailing slashes removed (the so-called "trimmed url");
     - the url passed exactly;
     - if the trimmed url ends with a dot and one of the
       :const:`KNOWN_SUFFIXES`, the url with this suffix stripped.

    The prefix matching is done by splitting the path part of the URL on
    slashes, and successively removing the last elements.

    Returns:
      A tuple with a list of exact matches, and a list of prefix matches

    """

    # Generate exact strings to match against
    exact_matches = [url]

    if url.endswith("/"):
        trimmed_url = url.rstrip("/")
        exact_matches.insert(0, trimmed_url)
    else:
        trimmed_url = url

    if "." in trimmed_url:
        stripped_url, suffix = trimmed_url.rsplit(".", 1)
        if suffix in KNOWN_SUFFIXES:
            exact_matches.append(stripped_url)

    # Generate prefixes to match against
    parsed_url = urlparse(trimmed_url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    prefix_matches = []

    split_path = parsed_url.path.lstrip("/").split("/")
    for i in range(1, len(split_path) + 1):
        prefix_matches.append("/".join([base_url] + split_path[: len(split_path) - i]))

    return exact_matches, prefix_matches


class BlockingAdmin(BlockingDb):
    def create_request(self, slug: str, reason: str) -> BlockingRequest:
        """Record a new blocking request

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
                INSERT INTO blocking_request (slug, reason)
                VALUES (%s, %s) RETURNING id, date
                """,
                (slug, reason),
            )
        except psycopg2.errors.UniqueViolation:
            raise DuplicateRequest(slug)

        res = cur.fetchone()
        assert res is not None, "PostgreSQL returned an inconsistent result"
        id, date = res
        return BlockingRequest(id=id, date=date, slug=slug, reason=reason)

    def find_request(self, slug: str) -> Optional[BlockingRequest]:
        """Find a blocking request using its slug

        Returns: :const:`None` if a request with the given slug doesn't exist
        """
        cur = self.cursor()
        cur.execute(
            """
            SELECT id, slug, date, reason
            FROM blocking_request
            WHERE slug = %s
            """,
            (slug,),
        )

        res = cur.fetchone()
        if not res:
            return None
        id, slug, date, reason = res
        return BlockingRequest(id=id, date=date, slug=slug, reason=reason)

    def find_request_by_id(self, id: UUID) -> Optional[BlockingRequest]:
        """Find a blocking request using its id

        Returns: :const:`None` if a request with the given request doesn't exist
        """
        cur = self.cursor()
        cur.execute(
            """
            SELECT id, slug, date, reason
            FROM blocking_request
            WHERE id = %s
            """,
            (id,),
        )

        res = cur.fetchone()
        if not res:
            return None
        id, slug, date, reason = res
        return BlockingRequest(id=id, date=date, slug=slug, reason=reason)

    def get_requests(
        self, include_cleared_requests: bool = False
    ) -> List[Tuple[BlockingRequest, int]]:
        """Get known requests

        Args:
            include_cleared_requests: also include requests with no associated
            blocking states
        """
        cur = self.cursor()

        query = """SELECT id, slug, date, reason, COUNT(url_match) AS blocking_count
                     FROM blocking_request
                     LEFT JOIN blocked_origin ON (request = id)
                     GROUP BY id"""
        if not include_cleared_requests:
            query += "  HAVING COUNT(url_match) > 0"
        query += "  ORDER BY date DESC"
        cur.execute(query)
        result = []
        for id, slug, date, reason, block_count in cur:
            result.append(
                (
                    BlockingRequest(id=id, slug=slug, date=date, reason=reason),
                    block_count,
                )
            )
        return result

    def set_origins_state(
        self, request_id: UUID, new_state: BlockingState, urls: List[str]
    ):
        """Within the request with the given id, record the state of the given
        objects as ``new_state``.

        This creates entries or updates them as appropriate.

        Raises: :exc:`RequestNotFound` if the request is not found.
        """
        cur = self.cursor()

        cur.execute("SELECT 1 FROM blocking_request WHERE id = %s", (request_id,))
        if cur.fetchone() is None:
            raise RequestNotFound(request_id)

        execute_values(
            cur,
            """
            INSERT INTO blocked_origin (url_match, request, state)
            VALUES %s
            ON CONFLICT (url_match, request)
            DO UPDATE SET state = EXCLUDED.state
            """,
            (
                (
                    url,
                    request_id,
                    new_state.name.lower(),
                )
                for url in urls
            ),
        )

    def get_states_for_request(self, request_id: UUID) -> Dict[str, BlockingState]:
        """Get the state of urls associated with the given request.

        Raises :exc:`RequestNotFound` if the request is not found.
        """

        cur = self.cursor()

        cur.execute("SELECT 1 FROM blocking_request WHERE id = %s", (request_id,))
        if cur.fetchone() is None:
            raise RequestNotFound(request_id)

        result = {}
        cur.execute(
            """SELECT url_match, state
                 FROM blocked_origin
                WHERE request = %s""",
            (request_id,),
        )
        for url, state in cur:
            result[url] = BlockingState[state.upper()]
        return result

    def find_blocking_states(self, urls: List[str]) -> List[BlockedOrigin]:
        """Lookup the blocking state and associated requests for the given urls
        (exact match)."""

        cur = self.cursor()
        result = []
        for (
            url,
            request_slug,
            state,
        ) in psycopg2.extras.execute_values(
            cur,
            """SELECT url_match, slug, state
                 FROM blocked_origin
                INNER JOIN (VALUES %s) v(url_match)
                USING (url_match)
                 LEFT JOIN blocking_request ON (blocking_request.id = request)
                ORDER BY url_match, blocking_request.date DESC
            """,
            ((url,) for url in urls),
            fetch=True,
        ):
            result.append(
                BlockedOrigin(
                    request_slug=request_slug,
                    url_pattern=url,
                    state=BlockingState[state.upper()],
                )
            )
        return result

    def delete_blocking_states(self, request_id: UUID) -> None:
        """Remove all blocking states for the given request.

        Raises: :exc:`RequestNotFound` if the request is not found.
        """

        cur = self.cursor()
        cur.execute("SELECT 1 FROM blocking_request WHERE id = %s", (request_id,))
        if cur.fetchone() is None:
            raise RequestNotFound(request_id)

        cur.execute("DELETE FROM blocked_origin WHERE request = %s", (request_id,))

    def record_history(self, request_id: UUID, message: str) -> RequestHistory:
        """Add an entry to the history of the given request.

        Raises: :exc:`RequestNotFound` if the request is not found.
        """
        cur = self.cursor()
        try:
            cur.execute(
                """
                INSERT INTO blocking_request_history (request, message)
                VALUES (%s, %s) RETURNING date
                """,
                (request_id, message),
            )
        except psycopg2.errors.ForeignKeyViolation:
            raise RequestNotFound(request_id)

        res = cur.fetchone()
        assert res is not None, "PostgreSQL returned an inconsistent result"

        return RequestHistory(request=request_id, date=res[0], message=message)

    def get_history(self, request_id: UUID) -> List[RequestHistory]:
        """Get the history of a given request.

        Raises: :exc:`RequestNotFound` if the request if not found.
        """
        cur = self.cursor()

        cur.execute("SELECT 1 FROM blocking_request WHERE id = %s", (request_id,))
        if cur.fetchone() is None:
            raise RequestNotFound(request_id)

        cur.execute(
            """SELECT date, message
                    FROM blocking_request_history
                WHERE request = %s
                ORDER BY date DESC""",
            (request_id,),
        )
        records = []
        for date, message in cur:
            records.append(
                RequestHistory(request=request_id, date=date, message=message)
            )
        return records

    def get_log(
        self, request_id: Optional[UUID] = None, url: Optional[str] = None
    ) -> List[BlockingLogEntry]:
        cur = self.cursor()
        where = []
        args = []
        if request_id:
            where.append("request = %s")
            args.append(str(request_id))
        if url:
            where.append("url = %s")
            args.append(url)
        if where:
            condition = "WHERE " + " AND ".join(where)
        else:
            condition = ""
        cur.execute(
            f"""SELECT date, url, url_match, request, state
                    FROM blocked_origin_log
                    {condition}
                    ORDER BY date DESC""",
            args,
        )
        records = []
        for db_date, db_url, db_url_match, db_request_id, db_state in cur:
            records.append(
                BlockingLogEntry(
                    url=db_url,
                    url_match=db_url_match,
                    request=db_request_id,
                    date=db_date,
                    state=BlockingState[db_state.upper()],
                )
            )
        return records


class BlockingQuery(BlockingDb):
    def origins_are_blocked(
        self, urls: List[str], all_statuses=False
    ) -> Dict[str, BlockingStatus]:
        """Return the blocking status for eeach origin url given in urls

        If all_statuses is False, do not return urls whose blocking status is
        defined as NON_BLOCKING (so only return actually blocked urls).
        Otherwise, return all matching blocking status.

        """
        ret = {}
        for url in urls:
            status = self.origin_is_blocked(url)
            if status and (all_statuses or status.state != BlockingState.NON_BLOCKED):
                ret[url] = status
        return ret

    def origin_is_blocked(self, url: str) -> Optional[BlockingStatus]:
        """Checks if the origin URL should be blocked.

        If the given url matches a set of registered blocking rules, return the
        most appropriate one. Otherwise, return None.

        Log the blocking event in the database (log only a matching events).
        """
        logging.debug("url: %s", url)
        statsd.increment(METRIC_QUERY_TOTAL, 1)

        exact_matches, prefix_matches = get_urls_to_check(url)

        cur = self.cursor()
        psycopg2.extras.execute_values(
            cur,
            """
            SELECT url_match, request, state
            FROM blocked_origin
            INNER JOIN (VALUES %s) v(url_match)
            USING (url_match)
            ORDER BY char_length(url_match) DESC LIMIT 1
            """,
            ((_url,) for _url in exact_matches + prefix_matches),
        )
        row = cur.fetchone()

        if row:
            url_match, request_id, state = row
            status = BlockingStatus(
                state=BlockingState[state.upper()], request=request_id
            )
            logger.debug("Matching status for %s: %s", url_match, status)
            statsd.increment(METRIC_BLOCKED_TOTAL, 1)
            # log the event; even a NON_BLOCKED decision is logged
            cur.execute(
                """
                INSERT INTO blocked_origin_log (url, url_match, request, state)
                VALUES (%s, %s, %s, %s)
                """,
                (url, url_match, status.request, status.state.name.lower()),
            )
            return status
        return None
