# Copyright (C) 2024-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Generates a graphical representation of the Cassandra schema using
:mod:`swh.storage.cassandra.model`.
"""
import dataclasses
from typing import Tuple, Union

from . import model


def dot_diagram() -> str:
    """Generates a diagram of the database in PlantUML format"""
    import io
    import textwrap

    from .schema import HASH_ALGORITHMS

    out = io.StringIO()

    classes = {
        cls.TABLE: cls for cls in model.__dict__.values() if hasattr(cls, "TABLE")
    }

    out.write(
        textwrap.dedent(
            """
            digraph g {
            graph [
            rankdir = "LR",
            concentrate = true,
            ratio = auto
            ];
            node [
            fontsize = "10",
            shape = record
            ];
            edge [
            ];


            subgraph "logical_grouping" {
                style = rounded;
                bgcolor = gray95;
                color = gray;

                subgraph cluster_content {
                    label = <<b>content</b>>;
                    content;
                    content_by_sha1;
                    content_by_sha1_git;
                    content_by_sha256;
                    content_by_blake2s256;
                }

                subgraph cluster_skipped_content {
                    label = <<b>skipped_content</b>>;
                    skipped_content;
                    skipped_content_by_sha1;
                    skipped_content_by_sha1_git;
                    skipped_content_by_sha256;
                    skipped_content_by_blake2s256;
                }

                subgraph cluster_directory {
                    label = <<b>directories</b>>;
                    directory;
                    directory_entry;
                }

                subgraph cluster_revision {
                    label = <<b>revisions</b>>;
                    revision;
                    revision_parent;
                }

                subgraph cluster_release {
                    label = <<b>releases</b>>;
                    release;
                }

                subgraph cluster_snapshots {
                    label = <<b>snapshots</b>>;
                    snapshot;
                    snapshot_branch;
                }

                subgraph cluster_origins {
                    label = <<b>origins</b>>;
                    origin;
                    origin_visit;
                    origin_visit_status;
                }

                subgraph cluster_metadata {
                    label = <<b>metadata</b>>;
                    metadata_authority;
                    metadata_fetcher;
                    raw_extrinsic_metadata;
                    raw_extrinsic_metadata_by_id;
                }

                subgraph cluster_extid {
                    label = <<b>external identifiers</b>>;
                    extid;
                    extid_by_target;
                }
            }
            """
        )
    )

    def write_table_header(table_name: str) -> None:
        out.write(
            f'"{table_name}" [shape = plaintext, label = < '
            f'<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0">'
            # header row:
            f'<TR ><TD PORT="ltcol0"> </TD> '
            f'<TD bgcolor="grey90" border="1" COLSPAN="4"> \\N </TD> '
            f'<TD PORT="rtcol0"></TD></TR>'
        )

    def get_target_field(field_full_name: str) -> Tuple[str, int]:
        """Given a string like 'table.col', returns the table name and the index of the column
        within that table (1-indexed)"""
        (points_to_table, points_to_col) = points_to.split(".")
        try:
            target_cls = classes[points_to_table]
        except KeyError:
            raise Exception(f"Unknown table {points_to_table}") from None
        target_field_ids = [
            i
            for (i, field) in enumerate(dataclasses.fields(target_cls), start=1)
            if field.name == points_to_col
        ]
        try:
            (target_field_id,) = target_field_ids
        except ValueError:
            raise Exception(
                f"Expected exactly one field {target_cls.__name__}.{points_to_col}, "
                f"got: {target_field_ids}"
            ) from None
        return (points_to_table, target_field_id)

    # write main tables
    for cls in classes.values():
        write_table_header(cls.TABLE)

        for i, field in enumerate(dataclasses.fields(cls), start=1):
            if field.name in cls.PARTITION_KEY:
                assert (
                    field.name not in cls.CLUSTERING_KEY
                ), f"{field.name} is both PK and CK"
                key = "PK"
            elif field.name in cls.CLUSTERING_KEY:
                key = "CK"
            else:
                key = ""

            # TODO: use CQL types instead of Python types
            ty = field.type
            if getattr(ty, "__origin__", None) is Union:
                assert hasattr(ty, "__args__")  # for mypy
                assert (
                    len(ty.__args__) == 2 and type(None) in ty.__args__
                ), f"{cls.__name__}.{field.name} as unsupported type: {ty}"
                # this is Optional[], unwrap it
                (ty,) = [arg for arg in ty.__args__ if arg is not type(None)]  # noqa
            col_type = ty if isinstance(ty, str) else ty.__name__
            out.write(
                textwrap.dedent(
                    f"""
                    <TR><TD PORT="ltcol{i}" ></TD>
                    <TD align="left" > {field.name} </TD>
                    <TD align="left" > {col_type} </TD>
                    <TD align="left" > {key} </TD>
                    <TD align="left" PORT="rtcol{i}"> </TD></TR>
                    """
                )
            )

        out.write("</TABLE>> ];\n")

    # add content_by_* and skipped_content_by_*, which don't have their own Python classes
    for algo in HASH_ALGORITHMS:
        for main_table in ("content", "skipped_content"):
            write_table_header(f"{main_table}_by_{algo}")
            out.write(
                textwrap.dedent(
                    f"""
                    <TR><TD PORT="ltcol1" ></TD>
                    <TD align="left" > {algo} </TD>
                    <TD align="left" > bytes </TD>
                    <TD align="left" > PK </TD>
                    <TD align="left" PORT="rtcol1"> </TD></TR>
                    <TR><TD PORT="ltcol2" ></TD>
                    <TD align="left" > token </TD>
                    <TD align="left" > token </TD>
                    <TD align="left" > CK </TD>
                    <TD align="left" PORT="rtcol2"> </TD></TR>
                    """
                )
            )
            out.write("</TABLE>> ];\n")

            out.write(
                f'"{main_table}_by_{algo}":rtcol2 -> "{main_table}":ltcol0 [style = solid];\n'
            )

    # write "links" between tables
    for cls_name, cls in classes.items():
        for i, field in enumerate(dataclasses.fields(cls), start=1):
            links = []  # pairs of (is_strong, target)
            for points_to in field.metadata.get("fk") or []:
                links.append((True, points_to))
            for points_to in field.metadata.get("points_to") or []:
                links.append((False, points_to))
            for is_strong, points_to in links:
                (target_table, target_field_id) = get_target_field(points_to)
                if is_strong:
                    style = "[style = solid]"
                else:
                    style = "[style = dashed]"
                out.write(
                    f'"{cls.TABLE}":rtcol{i} -> "{target_table}":ltcol{target_field_id} '
                    f"{style};\n"
                )

    out.write("}\n")

    return out.getvalue()
