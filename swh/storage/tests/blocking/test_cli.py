# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import closing
import datetime as dt
from io import StringIO
import socket
import textwrap

from click.testing import CliRunner
import pytest

from swh.core.cli.db import db as swhdb
from swh.core.db.db_utils import get_database_info
from swh.core.tests.test_cli import assert_result
from swh.storage.tests.storage_data import StorageData as data

from ...proxies.blocking.cli import (
    EditAborted,
    blocking_cli_group,
    clear_request,
    edit_message,
    history_cmd,
    list_requests,
    new_request,
    object_state,
    read_origins,
    status_cmd,
    update_objects,
)
from ...proxies.blocking.db import (
    BlockedOrigin,
    BlockingAdmin,
    BlockingRequest,
    BlockingState,
    RequestHistory,
    RequestNotFound,
)


@pytest.mark.parametrize(
    "module_name", ["storage:blocking", "storage.proxies.blocking"]
)
def test_cli_db_create(postgresql, module_name):
    """Create a db then initializing it should be ok"""
    db_params = postgresql.info
    dbname = f"{module_name}-db"
    conninfo = (
        f"postgresql://{db_params.user}@{db_params.host}:{db_params.port}/{dbname}"
    )

    # This creates the db and installs the necessary admin extensions
    result = CliRunner().invoke(swhdb, ["create", module_name, "--dbname", conninfo])
    assert_result(result)

    # This initializes the schema and data
    result = CliRunner().invoke(swhdb, ["init", module_name, "--dbname", conninfo])
    assert_result(result)

    dbmodule, dbversion, dbflavor = get_database_info(conninfo)
    assert dbmodule == "storage:blocking"
    assert dbversion == BlockingAdmin.current_version
    assert dbflavor is None


@pytest.fixture
def blocking_admin_config(blocking_db_postgresql):
    return {
        "blocking_admin": {"cls": "postgresql", "db": blocking_db_postgresql.info.dsn}
    }


def test_blocking_admin_not_defined():
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        blocking_cli_group,
        ["list-requests"],
        obj={"config": {}},
    )
    assert result.exit_code == 2
    assert "blocking_admin" in result.stderr


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("localhost", 0))
        # We explicitly not set SO_REUSEADDR because we want the port
        # to stay free so we can get our connection refused.
        return s.getsockname()[1]


def test_blocking_admin_unreachable():
    erroneous_postgresql_port = find_free_port()
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        blocking_cli_group,
        ["list-requests"],
        obj={
            "config": {
                "blocking_admin": {
                    "blocking_db": (
                        "postgresql://localhost/postgres?"
                        f"port={erroneous_postgresql_port}"
                    )
                }
            }
        },
    )
    assert result.exit_code == 1
    assert "failed: Connection refused" in result.stderr


def test_edit_message_success(mocker):
    mocked_click_edit = mocker.patch(
        "swh.storage.proxies.blocking.cli.click.edit",
        return_value="Sometimes I'm round",
    )
    assert (
        edit_message("Hello!", extra_lines=["Sometimes I'm alone", "Sometimes I'm not"])
        == "Sometimes I'm round"
    )
    mocked_click_edit.assert_called_once_with(
        textwrap.dedent(
            """

        # Hello!
        # Lines starting with “#” will be ignored. An empty message will abort the operation.
        #
        # Sometimes I'm alone
        # Sometimes I'm not"""
        )
    )


def test_edit_message_removes_comment(mocker):
    mocker.patch(
        "swh.storage.proxies.blocking.cli.click.edit",
        return_value=textwrap.dedent(
            """\
        Hello!

        # This is a comment"""
        ),
    )
    assert edit_message("RANDOM PROMPT") == "Hello!"


def test_edit_message_empty_message_aborts(mocker):
    mocker.patch("swh.storage.proxies.blocking.cli.click.edit", return_value="")
    with pytest.raises(EditAborted):
        edit_message("RANDOM PROMPT")


@pytest.fixture
def request01():
    return BlockingRequest(
        id="da785a27-7e59-4a35-b82a-a5ae3714407c",
        slug="request-01",
        date=dt.datetime.now(dt.timezone.utc),
        reason="one",
    )


@pytest.fixture
def request02():
    return BlockingRequest(
        id="4fd42e35-2b6c-4536-8447-bc213cd0118b",
        slug="request-02",
        date=dt.datetime.now(dt.timezone.utc),
        reason="two",
    )


@pytest.fixture
def blocked_origin(request01):
    return BlockedOrigin(
        request_slug=request01.slug,
        url_pattern=data.origins[0].url,
        state=BlockingState.BLOCKED,
    )


@pytest.fixture
def blocked_origin2(request01):
    return BlockedOrigin(
        request_slug=request01.slug,
        url_pattern=data.origins[1].url,
        state=BlockingState.DECISION_PENDING,
    )


def blocked_origin3(request02):
    return BlockedOrigin(
        request_slug=request02.slug,
        url_pattern=data.origins[2].url,
        state=BlockingState.NOT_BLOCKED,
    )


@pytest.fixture
def history01(request01):
    return RequestHistory(
        request=request01.id,
        date=dt.datetime.now(dt.timezone.utc),
        message="record one",
    )


@pytest.fixture
def history02(request01):
    return RequestHistory(
        request=request01.id,
        date=dt.datetime.now(dt.timezone.utc),
        message="record two",
    )


@pytest.fixture
def mocked_blocking_admin(
    mocker, request01, request02, blocked_origin, blocked_origin2, history01, history02
):
    def mock_find_request(slug):
        if slug == "request-01":
            return request01
        elif slug == "request-02":
            return request02
        else:
            return None

    def mock_find_request_by_id(id):
        if str(id) == "da785a27-7e59-4a35-b82a-a5ae3714407c":
            return request01
        return None

    def mock_get_states_for_request(request_id):
        if request_id == request01.id:
            return {
                blocked_origin.url_pattern: blocked_origin.state,
                blocked_origin2.url_pattern: blocked_origin2.state,
            }
        return RequestNotFound(request_id)

    def mock_get_history(request_id):
        if request_id == request01.id:
            return [history02, history01]
        return RequestNotFound(request_id)

    mock = mocker.Mock(spec=BlockingAdmin)
    mock.find_request.side_effect = mock_find_request
    mock.find_request_by_id.side_effect = mock_find_request_by_id
    mock.get_states_for_request.side_effect = mock_get_states_for_request
    mock.get_history.side_effect = mock_get_history
    mock.get_requests.return_value = [(request02, 2), (request01, 1)]
    return mock


def test_new_request(mocker, mocked_blocking_admin, request01):
    mocked_blocking_admin.create_request.return_value = request01
    mocker.patch("swh.storage.proxies.blocking.cli.click.edit", return_value="one")
    runner = CliRunner()
    result = runner.invoke(
        new_request, ["request-01"], obj={"blocking_admin": mocked_blocking_admin}
    )
    assert_result(result)
    mocked_blocking_admin.create_request.assert_called_once_with("request-01", "one")
    assert "da785a27-7e59-4a35-b82a-a5ae3714407c" in result.output
    assert "request-01" in result.output


def test_new_request_with_message(mocker, mocked_blocking_admin, request01):
    mocked_blocking_admin.create_request.return_value = request01
    mocked_click_edit = mocker.patch("swh.storage.proxies.blocking.cli.click.edit")
    runner = CliRunner()
    result = runner.invoke(
        new_request,
        ["--message=one", "request-01"],
        obj={"blocking_admin": mocked_blocking_admin},
    )
    assert_result(result)
    mocked_click_edit.assert_not_called()
    mocked_blocking_admin.create_request.assert_called_once_with("request-01", "one")
    assert "da785a27-7e59-4a35-b82a-a5ae3714407c" in result.output
    assert "request-01" in result.output


def test_new_request_empty_message_aborts(mocker, mocked_blocking_admin):
    mocked_blocking_admin.create_request.return_value = request01
    mocker.patch("swh.storage.proxies.blocking.cli.click.edit", side_effect=EditAborted)
    runner = CliRunner()
    result = runner.invoke(
        new_request,
        ["request-01"],
        obj={"blocking_admin": mocked_blocking_admin},
    )
    assert result.exit_code == 1
    assert "Aborting" in result.output
    mocked_blocking_admin.create_request.assert_not_called()


def test_list_requests_default(mocked_blocking_admin):
    runner = CliRunner()
    result = runner.invoke(
        list_requests, [], obj={"blocking_admin": mocked_blocking_admin}
    )
    assert_result(result)
    mocked_blocking_admin.get_requests.assert_called_once_with(
        include_cleared_requests=False
    )
    assert "request-01" in result.output
    assert "da785a27-7e59-4a35-b82a-a5ae3714407c" in result.output
    assert "request-02" in result.output
    assert "4fd42e35-2b6c-4536-8447-bc213cd0118b" in result.output


def test_list_requests_include_cleared_requests(mocked_blocking_admin):
    runner = CliRunner()
    result = runner.invoke(
        list_requests,
        ["--include-cleared-requests"],
        obj={"blocking_admin": mocked_blocking_admin},
    )
    assert_result(result)
    mocked_blocking_admin.get_requests.assert_called_once_with(
        include_cleared_requests=True
    )


ORIGINS_INPUT = f"""\
# origin1
{data.origin.url}

# origin2
{data.origin2.url}
"""

ORIGINS_INPUT_PARSED = [
    data.origin.url,
    data.origin2.url,
]


def test_read_origins():
    file = StringIO(ORIGINS_INPUT)
    assert read_origins(file) == ORIGINS_INPUT_PARSED


def test_update_objects(mocker, mocked_blocking_admin, request01):
    mocked_click_edit = mocker.patch(
        "swh.storage.proxies.blocking.cli.click.edit", return_value="action made"
    )
    runner = CliRunner()
    result = runner.invoke(
        update_objects,
        ["request-01", "decision-pending"],
        input=ORIGINS_INPUT,
        obj={"blocking_admin": mocked_blocking_admin},
    )
    assert_result(result)
    assert "2 origins" in result.output
    assert "request-01" in result.output
    mocked_click_edit.assert_called_once()
    mocked_blocking_admin.set_origins_state.assert_called_once_with(
        request01.id, BlockingState.DECISION_PENDING, ORIGINS_INPUT_PARSED
    )
    mocked_blocking_admin.record_history.assert_called_once_with(
        request01.id, "action made"
    )


def test_update_objects_with_message(mocker, mocked_blocking_admin, request01):
    mocked_click_edit = mocker.patch(
        "swh.storage.proxies.blocking.cli.click.edit", return_value="action made"
    )
    runner = CliRunner()
    result = runner.invoke(
        update_objects,
        ["request-01", "decision-pending", "--message=action made"],
        input=ORIGINS_INPUT,
        obj={"blocking_admin": mocked_blocking_admin},
    )
    assert_result(result)
    mocked_click_edit.assert_not_called()
    mocked_blocking_admin.set_origins_state.assert_called_once()
    mocked_blocking_admin.record_history.assert_called_once_with(
        request01.id, "action made"
    )


def test_update_objects_empty_message_aborts(mocker, mocked_blocking_admin):
    mocker.patch("swh.storage.proxies.blocking.cli.click.edit", side_effect=EditAborted)
    runner = CliRunner()
    result = runner.invoke(
        update_objects,
        ["request-01", "decision-pending"],
        input=ORIGINS_INPUT,
        obj={"blocking_admin": mocked_blocking_admin},
    )
    assert result.exit_code == 1
    mocked_blocking_admin.record_history.assert_not_called()


def test_update_objects_with_file(tmp_path, mocker, mocked_blocking_admin, request01):
    mocker.patch(
        "swh.storage.proxies.blocking.cli.click.edit", return_value="action made"
    )
    origins_file = tmp_path / "origins"
    origins_file.write_text(ORIGINS_INPUT)
    runner = CliRunner()
    result = runner.invoke(
        update_objects,
        ["request-01", "decision-pending", f"--file={str(origins_file)}"],
        obj={"blocking_admin": mocked_blocking_admin},
    )
    assert_result(result)
    assert "2 origins" in result.output
    mocked_blocking_admin.set_origins_state.assert_called_once_with(
        request01.id, BlockingState.DECISION_PENDING, ORIGINS_INPUT_PARSED
    )


def test_update_objects_no_origins(mocked_blocking_admin):
    runner = CliRunner()
    result = runner.invoke(
        update_objects,
        ["request-01", "decision-pending", "--message=message"],
        input="",
        obj={"blocking_admin": mocked_blocking_admin},
    )
    assert result.exit_code == 1
    assert "No origin given!" in result.output
    mocked_blocking_admin.set_origins_state.assert_not_called()
    mocked_blocking_admin.record_history.assert_not_called()


def test_status(mocked_blocking_admin, request01):
    runner = CliRunner()
    result = runner.invoke(
        status_cmd, ["request-01"], obj={"blocking_admin": mocked_blocking_admin}
    )
    assert_result(result)
    mocked_blocking_admin.get_states_for_request.assert_called_once_with(request01.id)
    assert result.output == textwrap.dedent(
        """\
        https://github.com/user1/repo1 blocked
        https://github.com/user2/repo1 decision-pending
        """
    )


def test_status_request_not_found(mocked_blocking_admin):
    runner = CliRunner()
    result = runner.invoke(
        status_cmd, ["garbage"], obj={"blocking_admin": mocked_blocking_admin}
    )
    assert result.exit_code == 1
    assert "Error: Request “garbage” not found" in result.output


def test_status_request_via_uuid(mocked_blocking_admin, request01):
    runner = CliRunner()
    result = runner.invoke(
        status_cmd,
        ["da785a27-7e59-4a35-b82a-a5ae3714407c"],
        obj={"blocking_admin": mocked_blocking_admin},
    )
    assert_result(result)
    mocked_blocking_admin.get_states_for_request.assert_called_once_with(request01.id)


def test_history(mocked_blocking_admin, request01):
    runner = CliRunner()
    result = runner.invoke(
        history_cmd, ["request-01"], obj={"blocking_admin": mocked_blocking_admin}
    )
    assert_result(result)
    mocked_blocking_admin.get_history.assert_called_once_with(request01.id)
    assert (
        "History for request “request-01” (da785a27-7e59-4a35-b82a-a5ae3714407c)"
        in result.output
    )
    assert "record two" in result.output
    assert "record one" in result.output


def test_history_not_found(mocked_blocking_admin):
    runner = CliRunner()
    result = runner.invoke(
        history_cmd, ["garbage"], obj={"blocking_admin": mocked_blocking_admin}
    )
    assert result.exit_code == 1
    assert "Error: Request “garbage” not found" in result.output


def test_object_state(mocked_blocking_admin, blocked_origin, blocked_origin2):
    blocked_origin_in_request02 = BlockedOrigin(
        request_slug="request-02",
        url_pattern=blocked_origin.url_pattern,
        state=BlockingState.NON_BLOCKED,
    )
    mocked_blocking_admin.find_blocking_states.return_value = [
        blocked_origin_in_request02,
        blocked_origin,
        blocked_origin2,
    ]
    runner = CliRunner()
    result = runner.invoke(
        object_state,
        [
            data.origins[0].url,
            data.origins[1].url,
            data.origins[2].url,
        ],
        obj={"blocking_admin": mocked_blocking_admin},
    )
    assert_result(result)
    assert result.output == textwrap.dedent(
        """\
        blocked  https://github.com/user1/repo1
                request-02: allowed
                request-01: blocked
        blocked  https://github.com/user2/repo1
                request-01: decision-pending

        """
    )


def test_clear_request(mocker, mocked_blocking_admin, request01):
    mocked_click_edit = mocker.patch(
        "swh.storage.proxies.blocking.cli.click.edit", return_value="request cleared"
    )
    runner = CliRunner()
    result = runner.invoke(
        clear_request,
        ["request-01"],
        obj={"blocking_admin": mocked_blocking_admin},
    )
    assert_result(result)
    assert "Blockings cleared for request “request-01”." in result.output
    mocked_click_edit.assert_called_once()
    mocked_blocking_admin.delete_blocking_states.assert_called_once_with(request01.id)
    mocked_blocking_admin.record_history.assert_called_once_with(
        request01.id, "request cleared"
    )


def test_clear_request_with_message(mocker, mocked_blocking_admin, request01):
    mocked_click_edit = mocker.patch("swh.storage.proxies.blocking.cli.click.edit")
    runner = CliRunner()
    result = runner.invoke(
        clear_request,
        ["--message=request cleared", "request-01"],
        obj={"blocking_admin": mocked_blocking_admin},
    )
    assert_result(result)
    assert "Blockings cleared for request “request-01”." in result.output
    mocked_click_edit.assert_not_called()
    mocked_blocking_admin.delete_blocking_states.assert_called_once_with(request01.id)
    mocked_blocking_admin.record_history.assert_called_once_with(
        request01.id, "request cleared"
    )


def test_clear_request_empty_message_aborts(mocker, mocked_blocking_admin):
    mocker.patch("swh.storage.proxies.blocking.cli.click.edit", side_effect=EditAborted)
    runner = CliRunner()
    result = runner.invoke(
        clear_request,
        ["request-01"],
        obj={"blocking_admin": mocked_blocking_admin},
    )
    assert result.exit_code == 1
    assert "Aborting" in result.output
    mocked_blocking_admin.delete_blocking_states.assert_not_called()
    mocked_blocking_admin.record_history.assert_not_called()


def test_clear_request_not_found(mocked_blocking_admin):
    runner = CliRunner()
    result = runner.invoke(
        clear_request, ["garbage"], obj={"blocking_admin": mocked_blocking_admin}
    )
    assert result.exit_code == 1
    assert "Error: Request “garbage” not found" in result.output
    mocked_blocking_admin.delete_blocking_states.assert_not_called()
    mocked_blocking_admin.record_history.assert_not_called()
