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
from swh.model.swhids import ExtendedSWHID, ValidationError

from ...proxies.masking.cli import (
    EditAborted,
    clear_request,
    edit_message,
    history_cmd,
    list_requests,
    masking_cli_group,
    new_request,
    object_state,
    read_swhids,
    status_cmd,
    update_objects,
)
from ...proxies.masking.db import (
    MaskedObject,
    MaskedState,
    MaskingAdmin,
    MaskingRequest,
    MaskingRequestHistory,
    RequestNotFound,
)


@pytest.mark.parametrize("module_name", ["storage:masking", "storage.proxies.masking"])
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
    assert dbmodule == "storage:masking"
    assert dbversion == MaskingAdmin.current_version
    assert dbflavor is None


@pytest.fixture
def masking_admin_config(masking_db_postgresql):
    return {
        "masking_admin": {"cls": "postgresql", "db": masking_db_postgresql.info.dsn}
    }


def test_masking_admin_not_defined():
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        masking_cli_group,
        ["list-requests"],
        obj={"config": {}},
    )
    assert result.exit_code == 2
    assert "masking_admin" in result.stderr


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("localhost", 0))
        # We explicitly not set SO_REUSEADDR because we want the port
        # to stay free so we can get our connection refused.
        return s.getsockname()[1]


def test_masking_admin_unreachable():
    erroneous_postgresql_port = find_free_port()
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        masking_cli_group,
        ["list-requests"],
        obj={
            "config": {
                "masking_admin": {
                    "db": (
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
        "swh.storage.proxies.masking.cli.click.edit", return_value="Sometimes I'm round"
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
        "swh.storage.proxies.masking.cli.click.edit",
        return_value=textwrap.dedent(
            """\
        Hello!

        # This is a comment"""
        ),
    )
    assert edit_message("RANDOM PROMPT") == "Hello!"


def test_edit_message_empty_message_aborts(mocker):
    mocker.patch("swh.storage.proxies.masking.cli.click.edit", return_value="")
    with pytest.raises(EditAborted):
        edit_message("RANDOM PROMPT")


@pytest.fixture
def request01():
    return MaskingRequest(
        id="da785a27-7e59-4a35-b82a-a5ae3714407c",
        slug="request-01",
        date=dt.datetime.now(dt.timezone.utc),
        reason="one",
    )


@pytest.fixture
def request02():
    return MaskingRequest(
        id="4fd42e35-2b6c-4536-8447-bc213cd0118b",
        slug="request-02",
        date=dt.datetime.now(dt.timezone.utc),
        reason="two",
    )


@pytest.fixture
def masked_content(request01):
    return MaskedObject(
        request_slug=request01.slug,
        swhid=ExtendedSWHID.from_string(
            "swh:1:cnt:d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"
        ),
        state=MaskedState.RESTRICTED,
    )


@pytest.fixture
def masked_content2(request01):
    return MaskedObject(
        request_slug=request01.slug,
        swhid=ExtendedSWHID.from_string(
            "swh:1:cnt:36fade77193cb6d2bd826161a0979d64c28ab4fa"
        ),
        state=MaskedState.DECISION_PENDING,
    )


def masked_content3(request02):
    return MaskedObject(
        request_slug=request02.slug,
        swhid=ExtendedSWHID.from_string(
            "swh:1:cnt:c932c7649c6dfa4b82327d121215116909eb3bea"
        ),
        state=MaskedState.VISIBLE,
    )


@pytest.fixture
def history01(request01):
    return MaskingRequestHistory(
        request=request01.id,
        date=dt.datetime.now(dt.timezone.utc),
        message="record one",
    )


@pytest.fixture
def history02(request01):
    return MaskingRequestHistory(
        request=request01.id,
        date=dt.datetime.now(dt.timezone.utc),
        message="record two",
    )


@pytest.fixture
def mocked_masking_admin(
    mocker, request01, request02, masked_content, masked_content2, history01, history02
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
                masked_content.swhid: masked_content.state,
                masked_content2.swhid: masked_content2.state,
            }
        return RequestNotFound(request_id)

    def mock_get_history(request_id):
        if request_id == request01.id:
            return [history02, history01]
        return RequestNotFound(request_id)

    mock = mocker.Mock(spec=MaskingAdmin)
    mock.find_request.side_effect = mock_find_request
    mock.find_request_by_id.side_effect = mock_find_request_by_id
    mock.get_states_for_request.side_effect = mock_get_states_for_request
    mock.get_history.side_effect = mock_get_history
    mock.get_requests.return_value = [(request02, 2), (request01, 1)]
    return mock


def test_new_request(mocker, mocked_masking_admin, request01):
    mocked_masking_admin.create_request.return_value = request01
    mocker.patch("swh.storage.proxies.masking.cli.click.edit", return_value="one")
    runner = CliRunner()
    result = runner.invoke(
        new_request, ["request-01"], obj={"masking_admin": mocked_masking_admin}
    )
    assert_result(result)
    mocked_masking_admin.create_request.assert_called_once_with("request-01", "one")
    assert "da785a27-7e59-4a35-b82a-a5ae3714407c" in result.output
    assert "request-01" in result.output


def test_new_request_with_message(mocker, mocked_masking_admin, request01):
    mocked_masking_admin.create_request.return_value = request01
    mocked_click_edit = mocker.patch("swh.storage.proxies.masking.cli.click.edit")
    runner = CliRunner()
    result = runner.invoke(
        new_request,
        ["--message=one", "request-01"],
        obj={"masking_admin": mocked_masking_admin},
    )
    assert_result(result)
    mocked_click_edit.assert_not_called()
    mocked_masking_admin.create_request.assert_called_once_with("request-01", "one")
    assert "da785a27-7e59-4a35-b82a-a5ae3714407c" in result.output
    assert "request-01" in result.output


def test_new_request_empty_message_aborts(mocker, mocked_masking_admin):
    mocked_masking_admin.create_request.return_value = request01
    mocker.patch("swh.storage.proxies.masking.cli.click.edit", side_effect=EditAborted)
    runner = CliRunner()
    result = runner.invoke(
        new_request,
        ["request-01"],
        obj={"masking_admin": mocked_masking_admin},
    )
    assert result.exit_code == 1
    assert "Aborting" in result.output
    mocked_masking_admin.create_request.assert_not_called()


def test_list_requests_default(mocked_masking_admin):
    runner = CliRunner()
    result = runner.invoke(
        list_requests, [], obj={"masking_admin": mocked_masking_admin}
    )
    assert_result(result)
    mocked_masking_admin.get_requests.assert_called_once_with(
        include_cleared_requests=False
    )
    assert "request-01" in result.output
    assert "da785a27-7e59-4a35-b82a-a5ae3714407c" in result.output
    assert "request-02" in result.output
    assert "4fd42e35-2b6c-4536-8447-bc213cd0118b" in result.output


def test_list_requests_include_cleared_requests(mocked_masking_admin):
    runner = CliRunner()
    result = runner.invoke(
        list_requests,
        ["--include-cleared-requests"],
        obj={"masking_admin": mocked_masking_admin},
    )
    assert_result(result)
    mocked_masking_admin.get_requests.assert_called_once_with(
        include_cleared_requests=True
    )


SWHIDS_INPUT = """\
# content1
swh:1:cnt:d81cc0710eb6cf9efd5b920a8453e1e07157b6cd

# content2
swh:1:cnt:36fade77193cb6d2bd826161a0979d64c28ab4fa
"""

SWHIDS_INPUT_PARSED = [
    ExtendedSWHID.from_string("swh:1:cnt:d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
    ExtendedSWHID.from_string("swh:1:cnt:36fade77193cb6d2bd826161a0979d64c28ab4fa"),
]


def test_read_swhids():
    file = StringIO(SWHIDS_INPUT)
    assert read_swhids(file) == SWHIDS_INPUT_PARSED


def test_read_swhids_failure():
    file = StringIO("GARBAGE 💩")
    with pytest.raises(ValidationError):
        read_swhids(file)


def test_update_objects(mocker, mocked_masking_admin, request01):
    mocked_click_edit = mocker.patch(
        "swh.storage.proxies.masking.cli.click.edit", return_value="action made"
    )
    runner = CliRunner()
    result = runner.invoke(
        update_objects,
        ["request-01", "decision-pending"],
        input=SWHIDS_INPUT,
        obj={"masking_admin": mocked_masking_admin},
    )
    assert_result(result)
    assert "2 objects" in result.output
    assert "request-01" in result.output
    mocked_click_edit.assert_called_once()
    mocked_masking_admin.set_object_state.assert_called_once_with(
        request01.id, MaskedState.DECISION_PENDING, SWHIDS_INPUT_PARSED
    )
    mocked_masking_admin.record_history.assert_called_once_with(
        request01.id, "action made"
    )


def test_update_objects_with_message(mocker, mocked_masking_admin, request01):
    mocked_click_edit = mocker.patch(
        "swh.storage.proxies.masking.cli.click.edit", return_value="action made"
    )
    runner = CliRunner()
    result = runner.invoke(
        update_objects,
        ["request-01", "decision-pending", "--message=action made"],
        input=SWHIDS_INPUT,
        obj={"masking_admin": mocked_masking_admin},
    )
    assert_result(result)
    mocked_click_edit.assert_not_called()
    mocked_masking_admin.set_object_state.assert_called_once()
    mocked_masking_admin.record_history.assert_called_once_with(
        request01.id, "action made"
    )


def test_update_objects_empty_message_aborts(mocker, mocked_masking_admin):
    mocker.patch("swh.storage.proxies.masking.cli.click.edit", side_effect=EditAborted)
    runner = CliRunner()
    result = runner.invoke(
        update_objects,
        ["request-01", "decision-pending"],
        input=SWHIDS_INPUT,
        obj={"masking_admin": mocked_masking_admin},
    )
    assert result.exit_code == 1
    mocked_masking_admin.record_history.assert_not_called()


def test_update_objects_with_file(tmp_path, mocker, mocked_masking_admin, request01):
    mocker.patch(
        "swh.storage.proxies.masking.cli.click.edit", return_value="action made"
    )
    swhids_file = tmp_path / "swhids"
    swhids_file.write_text(SWHIDS_INPUT)
    runner = CliRunner()
    result = runner.invoke(
        update_objects,
        ["request-01", "decision-pending", f"--file={str(swhids_file)}"],
        obj={"masking_admin": mocked_masking_admin},
    )
    assert_result(result)
    assert "2 objects" in result.output
    mocked_masking_admin.set_object_state.assert_called_once_with(
        request01.id, MaskedState.DECISION_PENDING, SWHIDS_INPUT_PARSED
    )


def test_update_objects_unparseable_swhids(mocked_masking_admin):
    runner = CliRunner()
    result = runner.invoke(
        update_objects,
        ["request-01", "decision-pending", "--message=message"],
        input="GARBAGE 💩",
        obj={"masking_admin": mocked_masking_admin},
    )
    assert result.exit_code == 1
    assert "Invalid SWHID" in result.output
    mocked_masking_admin.set_object_state.assert_not_called()
    mocked_masking_admin.record_history.assert_not_called()


def test_update_objects_no_swhids(mocked_masking_admin):
    runner = CliRunner()
    result = runner.invoke(
        update_objects,
        ["request-01", "decision-pending", "--message=message"],
        input="",
        obj={"masking_admin": mocked_masking_admin},
    )
    assert result.exit_code == 1
    assert "No SWHIDs given!" in result.output
    mocked_masking_admin.set_object_state.assert_not_called()
    mocked_masking_admin.record_history.assert_not_called()


def test_status(mocked_masking_admin, request01):
    runner = CliRunner()
    result = runner.invoke(
        status_cmd, ["request-01"], obj={"masking_admin": mocked_masking_admin}
    )
    assert_result(result)
    mocked_masking_admin.get_states_for_request.assert_called_once_with(request01.id)
    assert result.output == textwrap.dedent(
        """\
        swh:1:cnt:d81cc0710eb6cf9efd5b920a8453e1e07157b6cd restricted
        swh:1:cnt:36fade77193cb6d2bd826161a0979d64c28ab4fa decision-pending
        """
    )


def test_status_request_not_found(mocked_masking_admin):
    runner = CliRunner()
    result = runner.invoke(
        status_cmd, ["garbage"], obj={"masking_admin": mocked_masking_admin}
    )
    assert result.exit_code == 1
    assert "Error: Request “garbage” not found" in result.output


def test_status_request_via_uuid(mocked_masking_admin, request01):
    runner = CliRunner()
    result = runner.invoke(
        status_cmd,
        ["da785a27-7e59-4a35-b82a-a5ae3714407c"],
        obj={"masking_admin": mocked_masking_admin},
    )
    assert_result(result)
    mocked_masking_admin.get_states_for_request.assert_called_once_with(request01.id)


def test_history(mocked_masking_admin, request01):
    runner = CliRunner()
    result = runner.invoke(
        history_cmd, ["request-01"], obj={"masking_admin": mocked_masking_admin}
    )
    assert_result(result)
    mocked_masking_admin.get_history.assert_called_once_with(request01.id)
    assert (
        "History for request “request-01” (da785a27-7e59-4a35-b82a-a5ae3714407c)"
        in result.output
    )
    assert "record two" in result.output
    assert "record one" in result.output


def test_history_not_found(mocked_masking_admin):
    runner = CliRunner()
    result = runner.invoke(
        history_cmd, ["garbage"], obj={"masking_admin": mocked_masking_admin}
    )
    assert result.exit_code == 1
    assert "Error: Request “garbage” not found" in result.output


def test_object_state(mocked_masking_admin, masked_content, masked_content2):
    masked_content_in_request02 = MaskedObject(
        request_slug="request-02", swhid=masked_content.swhid, state=MaskedState.VISIBLE
    )
    mocked_masking_admin.find_masks.return_value = [
        masked_content_in_request02,
        masked_content,
        masked_content2,
    ]
    runner = CliRunner()
    result = runner.invoke(
        object_state,
        [
            "swh:1:cnt:d81cc0710eb6cf9efd5b920a8453e1e07157b6cd",
            "swh:1:cnt:36fade77193cb6d2bd826161a0979d64c28ab4fa",
            "swh:1:cnt:0000000000000000000000000000000000000000",
        ],
        obj={"masking_admin": mocked_masking_admin},
    )
    assert_result(result)
    assert result.output == textwrap.dedent(
        """\
        masked  swh:1:cnt:d81cc0710eb6cf9efd5b920a8453e1e07157b6cd
                request-02: visible
                request-01: restricted
        masked  swh:1:cnt:36fade77193cb6d2bd826161a0979d64c28ab4fa
                request-01: decision-pending

        """
    )


def test_object_state_bad_swhid():
    runner = CliRunner()
    result = runner.invoke(
        object_state, ["GARBAGE"], obj={"masking_admin": mocked_masking_admin}
    )
    assert result.exit_code == 1
    assert "Unable to parse “GARBAGE” as a SWHID" in result.output


def test_clear_request(mocker, mocked_masking_admin, request01):
    mocked_click_edit = mocker.patch(
        "swh.storage.proxies.masking.cli.click.edit", return_value="request cleared"
    )
    runner = CliRunner()
    result = runner.invoke(
        clear_request,
        ["request-01"],
        obj={"masking_admin": mocked_masking_admin},
    )
    assert_result(result)
    assert "Masks cleared for request “request-01”." in result.output
    mocked_click_edit.assert_called_once()
    mocked_masking_admin.delete_masks.assert_called_once_with(request01.id)
    mocked_masking_admin.record_history.assert_called_once_with(
        request01.id, "request cleared"
    )


def test_clear_request_with_message(mocker, mocked_masking_admin, request01):
    mocked_click_edit = mocker.patch("swh.storage.proxies.masking.cli.click.edit")
    runner = CliRunner()
    result = runner.invoke(
        clear_request,
        ["--message=request cleared", "request-01"],
        obj={"masking_admin": mocked_masking_admin},
    )
    assert_result(result)
    assert "Masks cleared for request “request-01”." in result.output
    mocked_click_edit.assert_not_called()
    mocked_masking_admin.delete_masks.assert_called_once_with(request01.id)
    mocked_masking_admin.record_history.assert_called_once_with(
        request01.id, "request cleared"
    )


def test_clear_request_empty_message_aborts(mocker, mocked_masking_admin):
    mocker.patch("swh.storage.proxies.masking.cli.click.edit", side_effect=EditAborted)
    runner = CliRunner()
    result = runner.invoke(
        clear_request,
        ["request-01"],
        obj={"masking_admin": mocked_masking_admin},
    )
    assert result.exit_code == 1
    assert "Aborting" in result.output
    mocked_masking_admin.delete_masks.assert_not_called()
    mocked_masking_admin.record_history.assert_not_called()


def test_clear_request_not_found(mocked_masking_admin):
    runner = CliRunner()
    result = runner.invoke(
        clear_request, ["garbage"], obj={"masking_admin": mocked_masking_admin}
    )
    assert result.exit_code == 1
    assert "Error: Request “garbage” not found" in result.output
    mocked_masking_admin.delete_masks.assert_not_called()
    mocked_masking_admin.record_history.assert_not_called()
