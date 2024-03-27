# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, List

import attr
import psycopg2.errors

from swh.core.db import BaseDb


@attr.s
class DisplayName:
    """A request for masking a set of objects"""

    original_email = attr.ib(type=bytes)
    """Email on revision/release objects to match before applying the display name"""

    display_name = attr.ib(type=bytes)
    """Full name, usually of the form `Name <email>, used for display queries"""


class PatchingDb(BaseDb):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.conn.autocommit = True


class PatchingAdmin(PatchingDb):
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


class PatchingQuery(PatchingDb):
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
