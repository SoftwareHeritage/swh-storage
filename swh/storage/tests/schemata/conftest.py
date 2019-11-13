# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import create_engine


@pytest.fixture
def sqlalchemy_engine(postgresql_proc):
    pg_host = postgresql_proc.host
    pg_port = postgresql_proc.port
    pg_user = postgresql_proc.user

    pg_db = 'sqlalchemy-tests'

    url = f'postgresql://{pg_user}@{pg_host}:{pg_port}/{pg_db}'
    with DatabaseJanitor(
            pg_user, pg_host, pg_port, pg_db, postgresql_proc.version
    ):
        engine = create_engine(url)
        yield engine
        engine.dispose()
