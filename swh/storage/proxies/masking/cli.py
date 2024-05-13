# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, TextIO

import click

from ...cli import storage as storage_cli_group

if TYPE_CHECKING:
    from swh.model.swhids import ExtendedSWHID

    from .db import MaskedState, MaskingRequest


def storage_config_iter(storage_config: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    yield storage_config
    if storage_config["cls"] == "pipeline":
        for step in storage_config["steps"]:
            yield step
    elif "storage" in storage_config:
        yield storage_config["storage"]


@storage_cli_group.group(name="masking")
@click.pass_context
def masking_cli_group(ctx: click.Context) -> click.Context:
    """Configure masking on archived objects

    These tools require read/write access to the masking database.
    An entry must be added to the configuration file as follow::

        \b
        storage:
          …
        \b
        masking_admin:
          masking_db: "service=swh-masking-admin"
    """

    if (
        "masking_admin" not in ctx.obj["config"]
        or "masking_db" not in ctx.obj["config"]["masking_admin"]
    ):
        ctx.fail(
            "You must have a masking_admin, with a masking_db entry, "
            "configured in your config file."
        )

    from psycopg2 import OperationalError

    from .db import MaskingAdmin

    try:
        ctx.obj["masking_admin"] = MaskingAdmin.connect(
            ctx.obj["config"]["masking_admin"]["masking_db"]
        )
    except OperationalError as ex:
        raise click.ClickException(str(ex))

    return ctx


class EditAborted(BaseException):
    pass


def edit_message(prompt: str, extra_lines: List[str] = []):
    """Edit a message through click.edit() adding some extra context, filtering
    comments, and raising an exception on empty messages"""

    message = (
        "\n\n"
        f"# {prompt}\n"
        "# Lines starting with “#” will be ignored. "
        "An empty message will abort the operation.\n"
    )
    if extra_lines:
        message += "#\n# " + "\n# ".join(extra_lines)
    message = click.edit(message)
    if message is None:
        raise EditAborted()
    message = "\n".join(
        line.strip()
        for line in message.lstrip().split("\n")
        if not line.startswith("#")
    ).rstrip()
    if message == "":
        raise EditAborted()
    return message


class RequestType(click.ParamType):
    name = "request slug or uuid"

    def convert(self, value, param, ctx) -> "MaskingRequest":
        # Try from UUID first
        from uuid import UUID

        try:
            uuid = UUID(value)
        except ValueError:
            pass
        else:
            request = ctx.obj["masking_admin"].find_request_by_id(uuid)
            if not request:
                raise click.ClickException(f"Request “{uuid}” not found from id.")
            return request

        # Try from slug
        request = ctx.obj["masking_admin"].find_request(value)
        if not request:
            raise click.ClickException(f"Request “{value}” not found.")
        return request


class SWHIDType(click.ParamType):
    name = "swhid"

    def convert(self, value, param, ctx) -> "ExtendedSWHID":
        from swh.model.swhids import ExtendedSWHID, ValidationError

        try:
            return ExtendedSWHID.from_string(value)
        except ValidationError:
            raise click.ClickException(f"Unable to parse “{value}” as a SWHID.")


@masking_cli_group.command(name="new-request")
@click.option(
    "-m", "--message", "reason", metavar="REASON", help="why the request was made"
)
@click.argument("slug")
@click.pass_context
def new_request(ctx: click.Context, slug: str, reason: Optional[str] = None) -> None:
    """Create a new request to mask objects

    SLUG is a human-readable unique identifier for the request. It is an
    internal identifier that will be used in subsequent commands to address this
    newly recorded request.

    A reason for the request must be specified, either using the `-m` option or
    via the provided editor.
    """

    from .db import DuplicateRequest

    if reason is None or reason == "":
        try:
            reason = edit_message(f"Please enter a reason for the request “{slug}”.")
        except EditAborted:
            raise click.ClickException("Aborting due to an empty reason.")

    try:
        request = ctx.obj["masking_admin"].create_request(slug, reason)
    except DuplicateRequest:
        raise click.ClickException(f"Request “{slug}” already exists.")
    click.echo(
        f"New request “{click.style(slug, fg='green', bold=True)}” "
        f"recorded with identifier {request.id}"
    )


@masking_cli_group.command(name="list-requests")
@click.option(
    "-a",
    "--include-cleared-requests/--exclude-cleared-requests",
    default=False,
    help="Show requests without any masking state",
)
@click.pass_context
def list_requests(ctx: click.Context, include_cleared_requests: bool) -> None:
    """List masking requests"""

    def list_output() -> Iterator[str]:
        for request, mask_count in ctx.obj["masking_admin"].get_requests(
            include_cleared_requests=include_cleared_requests
        ):
            yield f"📄 {click.style(request.slug, bold=True)}\n"
            yield f"📊 {mask_count} object{'s' if mask_count != 1 else ''}\n"
            yield f"🪪 {request.id}\n"
            # XXX: humanize would be nice here as well
            yield f"📅 {request.date.strftime('%c %z')}\n"
            yield from output_message(request.reason)
            yield "\n"

    click.echo_via_pager(list_output())


class MaskedStateType(click.Choice):
    def __init__(self):
        from .db import MaskedState

        super().__init__(
            [format_masked_state(state) for state in MaskedState], case_sensitive=False
        )


def parse_masked_state(str: str) -> "MaskedState":
    from .db import MaskedState

    return MaskedState[str.replace("-", "_").upper()]


def format_masked_state(state: "MaskedState") -> str:
    return state.name.lower().replace("_", "-")


def read_swhids(file: TextIO) -> List["ExtendedSWHID"]:
    import re

    from swh.model.swhids import ExtendedSWHID

    filter_re = re.compile(r"^(#|$)")
    return [
        ExtendedSWHID.from_string(line.strip())
        for line in file.read().split("\n")
        if not filter_re.match(line)
    ]


@masking_cli_group.command(name="update-objects")
@click.option("-m", "--message", help="an explanation for this change")
@click.option("-f", "--file", type=click.File(), help="a file with on SWHID per line")
@click.argument("request", metavar="SLUG", type=RequestType())
@click.argument("new-state", metavar="NEW_STATE", type=MaskedStateType())
@click.pass_context
def update_objects(
    ctx: click.Context,
    request: "MaskingRequest",
    new_state: str,
    message: Optional[str] = None,
    file: Optional[TextIO] = None,
) -> None:
    """Update the state of given objects

    The masked state of the provided SWHIDs will be updated to NEW_STATE for the
    request SLUG.

    NEW_STATE must be one of “visible”, “decision-pending” or “restricted”.

    SWHIDs must be provided one per line, either via the standard input or a
    file specified via the `-f` option. `-` is synonymous for the standard
    input.

    An explanation for this change must be added to the request history. It can
    either be specified by the `-m` option or via the provided editor.
    """

    import sys

    from swh.model.swhids import ValidationError

    from .db import RequestNotFound

    if file is None:
        file = sys.stdin

    masked_state = parse_masked_state(new_state)
    try:
        swhids = read_swhids(file)
    except ValidationError as ex:
        raise click.ClickException(ex.messages[0])

    if len(swhids) == 0:
        raise click.ClickException("No SWHIDs given!")

    if message is None or message == "":
        try:
            message = edit_message(
                f"Please enter an explanation for this update on request “{request.slug}”."
            )
        except EditAborted:
            raise click.ClickException("Aborting due to an empty explanation.")

    try:
        ctx.obj["masking_admin"].set_object_state(request.id, masked_state, swhids)
        ctx.obj["masking_admin"].record_history(request.id, message)
    except RequestNotFound:
        raise click.ClickException(f"Request with id “{request.id}” not found.")

    click.echo(
        f"Updated masking state for {len(swhids)} objects in request “{request.slug}”."
    )


@masking_cli_group.command(name="status")
@click.argument("request", metavar="SLUG", type=RequestType())
@click.pass_context
def status_cmd(ctx: click.Context, request: "MaskingRequest") -> None:
    """Get the masking states defined by a request"""

    states = ctx.obj["masking_admin"].get_states_for_request(request.id)
    for swhid, state in states.items():
        click.echo(f"{swhid} {format_masked_state(state)}")


def output_message(message: str) -> Iterator[str]:
    import textwrap

    for i, line in enumerate(message.splitlines()):
        yield textwrap.fill(
            line,
            initial_indent="🗩  " if i == 0 else "   ",
            subsequent_indent="    ",
            replace_whitespace=False,
        )
        yield "\n"


@masking_cli_group.command(name="history")
@click.argument("request", metavar="SLUG", type=RequestType())
@click.pass_context
def history_cmd(ctx: click.Context, request: "MaskingRequest") -> None:
    """Get the history for a request"""

    def history_output() -> Iterator[str]:
        # XXX: we have no way to tie a record to a set of swhids or their state
        # the only thing we can do is display the messages
        yield f"History for request “{request.slug}” ({request.id}) \n"
        yield from output_message(request.reason)
        yield "\n"
        for record in ctx.obj["masking_admin"].get_history(request.id):
            # XXX: if we agree to bring humanize in the dependencies
            # relative_time = humanize.naturaltime(
            #   dt.datetime.now(tz=dt.timezone.utc) - record.date
            # )
            yield f"📅 {record.date.strftime('%c %z')}\n"
            for i, line in enumerate(record.message.splitlines()):
                yield from output_message(record.message)

    click.echo_via_pager(history_output())


@masking_cli_group.command(name="object-state")
@click.argument("swhids", metavar="SWHID", nargs=-1, type=SWHIDType())
@click.pass_context
def object_state(ctx: click.Context, swhids: List["ExtendedSWHID"]) -> None:
    """Get the masking state for a set of SWHIDs

    If an object given in the arguments is not listed in the output,
    it means no masking state is set in any requests.
    """

    import itertools

    from .db import MaskedState

    STATE_TO_COLORS = {
        MaskedState.VISIBLE: "bright_green",
        MaskedState.DECISION_PENDING: "bright_yellow",
        MaskedState.RESTRICTED: "bright_red",
    }
    STATE_TO_LABEL = {
        MaskedState.VISIBLE: "visible",
        MaskedState.DECISION_PENDING: "decision-pending",
        MaskedState.RESTRICTED: "restricted",
    }

    def state_output() -> Iterator[str]:
        # find_masks() will group masks for the same SWHID
        for swhid, masks_iter in itertools.groupby(
            ctx.obj["masking_admin"].find_masks(swhids), key=lambda mask: mask.swhid
        ):
            masks = list(masks_iter)
            masked = any(mask.state != MaskedState.VISIBLE for mask in masks)
            yield (
                f"{'masked ' if masked else 'visible'} "
                f"{click.style(str(masks[0].swhid), bold=masked)}\n"
            )
            for mask in masks:
                yield click.style(
                    f"        {mask.request_slug}: {STATE_TO_LABEL[mask.state]}\n",
                    fg=STATE_TO_COLORS[mask.state],
                )

    click.echo_via_pager(state_output())


@masking_cli_group.command(name="clear-request")
@click.option("-m", "--message", help="an explanation for this change")
@click.argument("request", metavar="SLUG", type=RequestType())
@click.pass_context
def clear_request(
    ctx: click.Context, request: "MaskingRequest", message: Optional[str]
) -> None:
    """Remove all masking states for the given request"""

    from .db import RequestNotFound

    if message is None or message == "":
        states = ctx.obj["masking_admin"].get_states_for_request(request.id)
        extra_lines = ["Associated object states:", ""]
        for swhid, state in states.items():
            extra_lines.append(f"{swhid} {format_masked_state(state)}")
        try:
            message = edit_message(
                "Please enter an explanation for clearing masks"
                f" from request “{request.slug}”.",
                extra_lines=extra_lines,
            )
        except EditAborted:
            raise click.ClickException("Aborting due to an empty explanation.")

    try:
        ctx.obj["masking_admin"].delete_masks(request.id)
        ctx.obj["masking_admin"].record_history(request.id, message)
    except RequestNotFound:
        raise click.ClickException(f"Request with id “{request.id}” not found.")

    click.echo(f"Masks cleared for request “{request.slug}”.")
