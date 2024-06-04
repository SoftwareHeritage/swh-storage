# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import TYPE_CHECKING, Iterator, List, Optional, TextIO
import warnings

import click

from swh.storage.cli import storage as storage_cli_group
from swh.storage.proxies.masking.cli import EditAborted, edit_message, output_message

if TYPE_CHECKING:
    from .db import BlockingRequest, BlockingState


@storage_cli_group.group(name="blocking")
@click.pass_context
def blocking_cli_group(ctx: click.Context) -> click.Context:
    """Configure blocking of origins, preventing them from being archived

    These tools require read/write access to the blocking database.
    An entry must be added to the configuration file as follow::

        \b
        storage:
          …
        \b
        blocking_admin:
          cls: postgresql
          db: "service=swh-blocking-admin"
    """

    if "blocking_admin" not in ctx.obj["config"] or (
        "blocking_db" not in ctx.obj["config"]["blocking_admin"]
        and "db" not in ctx.obj["config"]["blocking_admin"]
    ):
        ctx.fail(
            "You must have a blocking_admin section, with a db entry, "
            "configured in your config file."
        )

    if "blocking_db" in ctx.obj["config"]["blocking_admin"]:
        warnings.warn(
            "Please use the db field for the blocking admin configuration, which "
            "makes it compatible with the `swh db` command-line utilities",
            DeprecationWarning,
        )

    from psycopg2 import OperationalError

    from .db import BlockingAdmin

    try:
        db = None
        for key in ("db", "blocking_db"):
            db = ctx.obj["config"]["blocking_admin"].get(key)
            if db:
                break
        if not db:
            assert False, "Existence of config entries has been checked earlier"

        ctx.obj["blocking_admin"] = BlockingAdmin.connect(db)

    except OperationalError as ex:
        raise click.ClickException(str(ex))

    return ctx


class RequestType(click.ParamType):
    name = "request slug or uuid"

    def convert(self, value, param, ctx) -> "BlockingRequest":
        # Try from UUID first
        from uuid import UUID

        try:
            uuid = UUID(value)
        except ValueError:
            pass
        else:
            request = ctx.obj["blocking_admin"].find_request_by_id(uuid)
            if not request:
                raise click.ClickException(f"Request “{uuid}” not found from id.")
            return request

        # Try from slug
        request = ctx.obj["blocking_admin"].find_request(value)
        if not request:
            raise click.ClickException(f"Request “{value}” not found.")
        return request


@blocking_cli_group.command(name="new-request")
@click.option(
    "-m", "--message", "reason", metavar="REASON", help="why the request was made"
)
@click.argument("slug")
@click.pass_context
def new_request(ctx: click.Context, slug: str, reason: Optional[str] = None) -> None:
    """Create a new request to block objects

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
        request = ctx.obj["blocking_admin"].create_request(slug, reason)
    except DuplicateRequest:
        raise click.ClickException(f"Request “{slug}” already exists.")
    click.echo(
        f"New request “{click.style(slug, fg='green', bold=True)}” "
        f"recorded with identifier {request.id}"
    )


@blocking_cli_group.command(name="list-requests")
@click.option(
    "-a",
    "--include-cleared-requests/--exclude-cleared-requests",
    default=False,
    help="Show requests without any blocking state",
)
@click.pass_context
def list_requests(ctx: click.Context, include_cleared_requests: bool) -> None:
    """List blocking requests"""

    def list_output() -> Iterator[str]:
        for request, block_count in ctx.obj["blocking_admin"].get_requests(
            include_cleared_requests=include_cleared_requests
        ):
            yield f"📄 {click.style(request.slug, bold=True)}\n"
            yield f"📊 {block_count} object{'s' if block_count != 1 else ''}\n"
            yield f"🪪 {request.id}\n"
            # XXX: humanize would be nice here as well
            yield f"📅 {request.date.strftime('%c %z')}\n"
            yield from output_message(request.reason)
            yield "\n"

    click.echo_via_pager(list_output())


class BlockedStateType(click.Choice):
    def __init__(self):
        from .db import BlockingState

        super().__init__(
            [format_blocked_state(state) for state in BlockingState],
            case_sensitive=False,
        )


def parse_blocked_state(str: str) -> "BlockingState":
    from .db import BlockingState

    return BlockingState[str.replace("-", "_").upper()]


def format_blocked_state(state: "BlockingState") -> str:
    return state.name.lower().replace("_", "-")


def read_origins(file: TextIO) -> List[str]:
    import logging
    import re

    filter_re = re.compile(r"^#")
    urls = []

    for line in file.readlines():
        line = line.strip()

        if not line:
            continue

        if filter_re.match(line):
            continue

        if line.endswith("/"):
            logging.getLogger(__name__).warning(
                "URL pattern ends with /, will only be used for exact matching"
            )

        urls.append(line)
    return urls


@blocking_cli_group.command(name="update-objects")
@click.option("-m", "--message", help="an explanation for this change")
@click.option("-f", "--file", type=click.File(), help="a file with one Origin per line")
@click.argument("request", metavar="SLUG", type=RequestType())
@click.argument("new-state", metavar="NEW_STATE", type=BlockedStateType())
@click.pass_context
def update_objects(
    ctx: click.Context,
    request: "BlockingRequest",
    new_state: str,
    message: Optional[str] = None,
    file: Optional[TextIO] = None,
) -> None:
    """Update the blocking state of given objects

    The blocked state of the provided Origins will be updated to NEW_STATE for the
    request SLUG.

    NEW_STATE must be one of “blocked”, “decision-pending” or “non_blocked”.

    origins must be provided one per line, either via the standard input or a
    file specified via the `-f` option. `-` is synonymous for the standard
    input.

    An explanation for this change must be added to the request history. It can
    either be specified by the `-m` option or via the provided editor.
    """

    import sys

    from .db import RequestNotFound

    if file is None:
        file = sys.stdin

    blocked_state = parse_blocked_state(new_state)
    origins = read_origins(file)

    if len(origins) == 0:
        raise click.ClickException("No origin given!")

    if message is None or message == "":
        try:
            message = edit_message(
                f"Please enter an explanation for this update on request “{request.slug}”."
            )
        except EditAborted:
            raise click.ClickException("Aborting due to an empty explanation.")

    try:
        ctx.obj["blocking_admin"].set_origins_state(request.id, blocked_state, origins)
        ctx.obj["blocking_admin"].record_history(request.id, message)
    except RequestNotFound:
        raise click.ClickException(f"Request with id “{request.id}” not found.")

    click.echo(
        f"Updated blocking state for {len(origins)} origins in request “{request.slug}”."
    )


@blocking_cli_group.command(name="status")
@click.argument("request", metavar="SLUG", type=RequestType())
@click.pass_context
def status_cmd(ctx: click.Context, request: "BlockingRequest") -> None:
    """Get the blocking states defined by a request"""

    states = ctx.obj["blocking_admin"].get_states_for_request(request.id)
    for swhid, state in states.items():
        click.echo(f"{swhid} {format_blocked_state(state)}")


@blocking_cli_group.command(name="history")
@click.argument("request", metavar="SLUG", type=RequestType())
@click.pass_context
def history_cmd(ctx: click.Context, request: "BlockingRequest") -> None:
    """Get the history for a request"""

    def history_output() -> Iterator[str]:
        # XXX: we have no way to tie a record to a set of swhids or their state
        # the only thing we can do is display the messages
        yield f"History for request “{request.slug}” ({request.id}) \n"
        yield from output_message(request.reason)
        yield "\n"
        for record in ctx.obj["blocking_admin"].get_history(request.id):
            # XXX: if we agree to bring humanize in the dependencies
            # relative_time = humanize.naturaltime(
            #   dt.datetime.now(tz=dt.timezone.utc) - record.date
            # )
            yield f"📅 {record.date.strftime('%c %z')}\n"
            for i, line in enumerate(record.message.splitlines()):
                yield from output_message(record.message)

    click.echo_via_pager(history_output())


@blocking_cli_group.command(name="origin-state")
@click.argument("origins", metavar="ORIGIN", nargs=-1, type=str)
@click.pass_context
def object_state(ctx: click.Context, origins: List[str]) -> None:
    """Get the blocking state for a set of Origins

    If an object given in the arguments is not listed in the output,
    it means no blocking state is set in any requests.
    """

    import itertools

    from .db import BlockingState

    STATE_TO_COLORS = {
        BlockingState.NON_BLOCKED: "bright_green",
        BlockingState.DECISION_PENDING: "bright_yellow",
        BlockingState.BLOCKED: "bright_red",
    }
    STATE_TO_LABEL = {
        BlockingState.NON_BLOCKED: "allowed",
        BlockingState.DECISION_PENDING: "decision-pending",
        BlockingState.BLOCKED: "blocked",
    }

    def state_output() -> Iterator[str]:
        # find_blocks() will group blocks for the same Origin
        for origin, blocks_iter in itertools.groupby(
            ctx.obj["blocking_admin"].find_blocking_states(origins),
            key=lambda block: block.url_pattern,
        ):
            blocks = list(blocks_iter)
            blocked = any(block.state != BlockingState.NON_BLOCKED for block in blocks)
            yield (
                f"{'blocked ' if blocked else 'visible'} "
                f"{click.style(str(blocks[0].url_pattern), bold=blocked)}\n"
            )
            for block in blocks:
                yield click.style(
                    f"        {block.request_slug}: {STATE_TO_LABEL[block.state]}\n",
                    fg=STATE_TO_COLORS[block.state],
                )

    click.echo_via_pager(state_output())


@blocking_cli_group.command(name="clear-request")
@click.option("-m", "--message", help="an explanation for this change")
@click.argument("request", metavar="SLUG", type=RequestType())
@click.pass_context
def clear_request(
    ctx: click.Context, request: "BlockingRequest", message: Optional[str]
) -> None:
    """Remove all blocking states for the given request"""

    from .db import RequestNotFound

    if message is None or message == "":
        states = ctx.obj["blocking_admin"].get_states_for_request(request.id)
        extra_lines = ["Associated object states:", ""]
        for url, state in states.items():
            extra_lines.append(f"{url} {format_blocked_state(state)}")
        try:
            message = edit_message(
                "Please enter an explanation for clearing blocks"
                f" from request “{request.slug}”.",
                extra_lines=extra_lines,
            )
        except EditAborted:
            raise click.ClickException("Aborting due to an empty explanation.")

    try:
        ctx.obj["blocking_admin"].delete_blocking_states(request.id)
        ctx.obj["blocking_admin"].record_history(request.id, message)
    except RequestNotFound:
        raise click.ClickException(f"Request with id “{request.id}” not found.")

    click.echo(f"Blockings cleared for request “{request.slug}”.")
