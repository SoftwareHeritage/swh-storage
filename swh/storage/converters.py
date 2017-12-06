# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from swh.core.utils import decode_with_escape, encode_with_unescape
from swh.model import identifiers


DEFAULT_AUTHOR = {
    'fullname': None,
    'name': None,
    'email': None,
}

DEFAULT_DATE = {
    'timestamp': None,
    'offset': 0,
    'neg_utc_offset': None,
}


def author_to_db(author):
    """Convert a swh-model author to its DB representation.

    Args:
        author: a :mod:`swh.model` compatible author

    Returns:
        dict: a dictionary with three keys: author, fullname and email

    """
    if author is None:
        return DEFAULT_AUTHOR

    return author


def db_to_author(id, fullname, name, email):
    """Convert the DB representation of an author to a swh-model author.

    Args:
        id (long): the author's identifier
        fullname (bytes): the author's fullname
        name (bytes): the author's name
        email (bytes): the author's email

    Returns:
        dict: a dictionary with four keys: id, fullname, name and email, or
        None if the id is None
    """

    if id is None:
        return None

    return {
        'id': id,
        'fullname': fullname,
        'name': name,
        'email': email,
    }


def git_headers_to_db(git_headers):
    """Convert git headers to their database representation.

    We convert the bytes to unicode by decoding them into utf-8 and replacing
    invalid utf-8 sequences with backslash escapes.

    """
    ret = []
    for key, values in git_headers:
        if isinstance(values, list):
            ret.append([key, [decode_with_escape(value) for value in values]])
        else:
            ret.append([key, decode_with_escape(values)])

    return ret


def db_to_git_headers(db_git_headers):
    ret = []
    for key, values in db_git_headers:
        if isinstance(values, list):
            ret.append([key, [encode_with_unescape(value)
                              for value in values]])
        else:
            ret.append([key, encode_with_unescape(values)])

    return ret


def db_to_date(date, offset, neg_utc_offset):
    """Convert the DB representation of a date to a swh-model compatible date.

    Args:
        date (datetime.datetime): a date pulled out of the database
        offset (int): an integer number of minutes representing an UTC offset
        neg_utc_offset (boolean): whether an utc offset is negative

    Returns:
        dict: a dict with three keys:

            - timestamp: a timestamp from UTC
            - offset: the number of minutes since UTC
            - negative_utc: whether a null UTC offset is negative

    """

    if date is None:
        return None

    return {
        'timestamp': {
            'seconds': int(date.timestamp()),
            'microseconds': date.microsecond,
        },
        'offset': offset,
        'negative_utc': neg_utc_offset,
    }


def date_to_db(date_offset):
    """Convert a swh-model date_offset to its DB representation.

    Args:
        date_offset: a :mod:`swh.model` compatible date_offset

    Returns:
        dict: a dictionary with three keys:

            - timestamp: a date in ISO format
            - offset: the UTC offset in minutes
            - neg_utc_offset: a boolean indicating whether a null offset is
              negative or positive.

    """

    if date_offset is None:
        return DEFAULT_DATE

    normalized = identifiers.normalize_timestamp(date_offset)

    ts = normalized['timestamp']
    seconds = ts.get('seconds', 0)
    microseconds = ts.get('microseconds', 0)

    timestamp = datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc)
    timestamp = timestamp.replace(microsecond=microseconds)

    return {
        # PostgreSQL supports isoformatted timestamps
        'timestamp': timestamp.isoformat(),
        'offset': normalized['offset'],
        'neg_utc_offset': normalized['negative_utc'],
    }


def revision_to_db(revision):
    """Convert a swh-model revision to its database representation.
    """

    author = author_to_db(revision['author'])
    date = date_to_db(revision['date'])
    committer = author_to_db(revision['committer'])
    committer_date = date_to_db(revision['committer_date'])

    metadata = revision['metadata']

    if metadata and 'extra_headers' in metadata:
        metadata = metadata.copy()
        extra_headers = git_headers_to_db(metadata['extra_headers'])
        metadata['extra_headers'] = extra_headers

    return {
        'id': revision['id'],
        'author_fullname': author['fullname'],
        'author_name': author['name'],
        'author_email': author['email'],
        'date': date['timestamp'],
        'date_offset': date['offset'],
        'date_neg_utc_offset': date['neg_utc_offset'],
        'committer_fullname': committer['fullname'],
        'committer_name': committer['name'],
        'committer_email': committer['email'],
        'committer_date': committer_date['timestamp'],
        'committer_date_offset': committer_date['offset'],
        'committer_date_neg_utc_offset': committer_date['neg_utc_offset'],
        'type': revision['type'],
        'directory': revision['directory'],
        'message': revision['message'],
        'metadata': metadata,
        'synthetic': revision['synthetic'],
        'parents': [
            {
                'id': revision['id'],
                'parent_id': parent,
                'parent_rank': i,
            } for i, parent in enumerate(revision['parents'])
        ],
    }


def db_to_revision(db_revision):
    """Convert a database representation of a revision to its swh-model
    representation."""

    author = db_to_author(
        db_revision['author_id'],
        db_revision['author_fullname'],
        db_revision['author_name'],
        db_revision['author_email'],
    )
    date = db_to_date(
        db_revision['date'],
        db_revision['date_offset'],
        db_revision['date_neg_utc_offset'],
    )

    committer = db_to_author(
        db_revision['committer_id'],
        db_revision['committer_fullname'],
        db_revision['committer_name'],
        db_revision['committer_email'],
    )
    committer_date = db_to_date(
        db_revision['committer_date'],
        db_revision['committer_date_offset'],
        db_revision['committer_date_neg_utc_offset']
    )

    metadata = db_revision['metadata']

    if metadata and 'extra_headers' in metadata:
        extra_headers = db_to_git_headers(metadata['extra_headers'])
        metadata['extra_headers'] = extra_headers

    parents = []
    if 'parents' in db_revision:
        for parent in db_revision['parents']:
            if parent:
                parents.append(parent)

    ret = {
        'id': db_revision['id'],
        'author': author,
        'date': date,
        'committer': committer,
        'committer_date': committer_date,
        'type': db_revision['type'],
        'directory': db_revision['directory'],
        'message': db_revision['message'],
        'metadata': metadata,
        'synthetic': db_revision['synthetic'],
        'parents': parents,
    }

    if 'object_id' in db_revision:
        ret['object_id'] = db_revision['object_id']

    return ret


def release_to_db(release):
    """Convert a swh-model release to its database representation.
    """

    author = author_to_db(release['author'])
    date = date_to_db(release['date'])

    return {
        'id': release['id'],
        'author_fullname': author['fullname'],
        'author_name': author['name'],
        'author_email': author['email'],
        'date': date['timestamp'],
        'date_offset': date['offset'],
        'date_neg_utc_offset': date['neg_utc_offset'],
        'name': release['name'],
        'target': release['target'],
        'target_type': release['target_type'],
        'comment': release['message'],
        'synthetic': release['synthetic'],
    }


def db_to_release(db_release):
    """Convert a database representation of a release to its swh-model
    representation.
    """

    author = db_to_author(
        db_release['author_id'],
        db_release['author_fullname'],
        db_release['author_name'],
        db_release['author_email'],
    )
    date = db_to_date(
        db_release['date'],
        db_release['date_offset'],
        db_release['date_neg_utc_offset']
    )

    ret = {
        'author': author,
        'date': date,
        'id': db_release['id'],
        'name': db_release['name'],
        'message': db_release['comment'],
        'synthetic': db_release['synthetic'],
        'target': db_release['target'],
        'target_type': db_release['target_type'],
    }

    if 'object_id' in db_release:
        ret['object_id'] = db_release['object_id']

    return ret
