#!/usr/bin/python3

import copy
import itertools
import os
import pickle
import sys

from swh.model import identifiers


def author_date_to_notnegutc(rev):
    rev['date']['negative_utc'] = False


def author_date_to_negutc(rev):
    rev['date']['negative_utc'] = True

DATE_NEGUTC_FIX = ('set author negutc', [
    (None, None),
    (author_date_to_negutc, 'date_neg_utcoffset = true'),
])


def committer_date_to_notnegutc(rev):
    rev['committer_date']['negative_utc'] = False


def committer_date_to_negutc(rev):
    rev['committer_date']['negative_utc'] = True

COMMITTER_DATE_NEGUTC_FIX = ('set committer negutc', [
    (None, None),
    (committer_date_to_negutc, 'committer_date_neg_utcoffset = true'),
])


def message_to_empty(rev):
    rev['message'] = b''

MESSAGE_EMPTY_FIX = ('empty instead of null message', [
    (None, None),
    (message_to_empty, "message = ''"),
])


def message_to_null(rev):
    rev['message'] = None

MESSAGE_NULL_FIX = ('null instead of empty message', [
    (None, None),
    (message_to_null, "message = NULL"),
])


def message_add_nl_end(num_nl):
    def fix(rev, num_nl=num_nl):
        components = [rev['message'] if rev['message'] else b'']
        components.extend([b'\n'] * num_nl)
        rev['message'] = b''.join(components)
    return fix


MESSAGE_ADD_NL_END_FIX = ('add newline to end of message', [
    (None, None),
    (message_add_nl_end(1), "add 1 newline to end of message"),
    (message_add_nl_end(2), "add 2 newlines to end of message"),
    (message_add_nl_end(3), "add 3 newlines to end of message"),
])


def message_add_nl_start(num_nl):
    def fix(rev, num_nl=num_nl):
        components = [b'\n'] * num_nl
        components.append(rev['message'] if rev['message'] else b'')
        rev['message'] = b''.join(components)
    return fix


MESSAGE_ADD_NL_START_FIX = ('add newline to start of message', [
    (None, None),
    (message_add_nl_start(1), "add 1 newline to start of message"),
    (message_add_nl_start(2), "add 2 newlines to start of message"),
    (message_add_nl_start(3), "add 3 newlines to start of message"),
])


def author_name_doublespace(rev):
    rev['author']['name'] = b''.join([rev['author']['name'], b' '])

AUTHOR_NAME_ADD_SPC_FIX = ('author double space', [
    (None, None),
    (author_name_doublespace, 'trailing space author name')
])


def committer_name_doublespace(rev):
    rev['committer']['name'] = b''.join([rev['committer']['name'], b' '])

COMMITTER_NAME_ADD_SPC_FIX = ('committer double space', [
    (None, None),
    (committer_name_doublespace, 'trailing space committer name')
])


def author_name_null(rev):
    rev['author']['name'] = None

AUTHOR_NAME_NULL_FIX = ('author name null', [
    (None, None),
    (author_name_null, 'None author name')
])


def author_email_null(rev):
    rev['author']['email'] = None

AUTHOR_EMAIL_NULL_FIX = ('author email null', [
    (None, None),
    (author_email_null, 'None author email')
])


def committer_name_null(rev):
    rev['committer']['name'] = None

COMMITTER_NAME_NULL_FIX = ('committer name null', [
    (None, None),
    (committer_name_null, 'None committer name')
])


def committer_email_null(rev):
    rev['committer']['email'] = None

COMMITTER_EMAIL_NULL_FIX = ('committer email null', [
    (None, None),
    (committer_email_null, 'None committer email')
])


def author_add_spc(rev):
    rev['author'] = b''.join([
        identifiers.normalize_author(rev['author']), b' '])

AUTHOR_ADD_SPC_FIX = ('add trailing space to author specification', [
    (None, None),
    (author_add_spc, 'add trailing space to author spec')
])


def committer_add_spc(rev):
    rev['committer'] = b''.join([
        identifiers.normalize_author(rev['committer']), b' '])

COMMITTER_ADD_SPC_FIX = ('add trailing space to committer specification', [
    (None, None),
    (committer_add_spc, 'add trailing space to committer spec')
])


def fix_revision(revision):
    data_fixups = []

    id = identifiers.identifier_to_str(revision['id'])

    if revision['message'] is None:
        data_fixups.append(MESSAGE_EMPTY_FIX)

    if revision['message'] == b'':
        data_fixups.append(MESSAGE_NULL_FIX)
    data_fixups.append(MESSAGE_ADD_NL_END_FIX)
    data_fixups.append(MESSAGE_ADD_NL_START_FIX)

    if revision['date']['offset'] == 0 and \
       not revision['date']['negative_utc']:
        data_fixups.append(DATE_NEGUTC_FIX)

    if revision['committer_date']['offset'] == 0 and \
       not revision['committer_date']['negative_utc']:
        data_fixups.append(COMMITTER_DATE_NEGUTC_FIX)

    if not data_fixups:
        computed_id = identifiers.revision_identifier(revision)
        if id == computed_id:
            return

    # Less credible fixups are first in the list, so they run last
    data_fixups.insert(0, COMMITTER_ADD_SPC_FIX)
    data_fixups.insert(0, AUTHOR_ADD_SPC_FIX)

    if revision['author']['name'] == b'':
        data_fixups.insert(0, AUTHOR_NAME_NULL_FIX)

    if revision['author']['email'] == b'':
        data_fixups.insert(0, AUTHOR_EMAIL_NULL_FIX)

    if revision['committer']['name'] == b'':
        data_fixups.insert(0, COMMITTER_NAME_NULL_FIX)

    if revision['committer']['email'] == b'':
        data_fixups.insert(0, COMMITTER_EMAIL_NULL_FIX)

    data_fixups.insert(0, COMMITTER_NAME_ADD_SPC_FIX)
    data_fixups.insert(0, AUTHOR_NAME_ADD_SPC_FIX)

    data_fixup_functions = [functions for title, functions in data_fixups]
    for corrections in itertools.product(*data_fixup_functions):
        sql_fixups = []
        new_revision = copy.deepcopy(revision)

        for fun, sql_fixup in corrections:
            if fun:
                fun(new_revision)
            if sql_fixup:
                sql_fixups.append(sql_fixup)

        computed_id = identifiers.revision_identifier(new_revision)
        if id == computed_id:
            if not sql_fixups:
                return

            return id, sql_fixups
    else:
        return id, []

if __name__ == '__main__':

    for hash in sys.stdin.readlines():
        hash = hash.strip()
        filename = os.path.join('revs', hash[0:2], hash[2:4], hash)
        with open(filename, 'rb') as f:
            revision = pickle.load(f)

        id, fixes = fix_revision(revision)
        if not fixes:
            print(id)
        else:
            print(';'.join([id, 'FIXED'] + fixes))
