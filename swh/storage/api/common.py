# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import pickle

from flask import Request, Response

from swh.core.serializers import msgpack_dumps, msgpack_loads, SWHJSONDecoder


class BytesRequest(Request):
    """Request with proper escaping of arbitrary byte sequences."""
    encoding = 'utf-8'
    encoding_errors = 'surrogateescape'


def encode_data_server(data):
    return Response(
        msgpack_dumps(data),
        mimetype='application/x-msgpack',
    )


def encode_data_client(data):
    try:
        return msgpack_dumps(data)
    except OverflowError as e:
        raise ValueError('Limits were reached. Please, check your input.\n' +
                         str(e))


def decode_request(request):
    content_type = request.mimetype
    data = request.get_data()

    if content_type == 'application/x-msgpack':
        r = msgpack_loads(data)
    elif content_type == 'application/json':
        r = json.loads(data, cls=SWHJSONDecoder)
    else:
        raise ValueError('Wrong content type `%s` for API request'
                         % content_type)

    return r


def decode_response(response):
    content_type = response.headers['content-type']

    if content_type.startswith('application/x-msgpack'):
        r = msgpack_loads(response.content)
    elif content_type.startswith('application/json'):
        r = response.json(cls=SWHJSONDecoder)
    else:
        raise ValueError('Wrong content type `%s` for API response'
                         % content_type)

    return r


def error_handler(exception, encoder):
    # XXX: this breaks language-independence and should be
    # replaced by proper serialization of errors
    response = encoder(pickle.dumps(exception))
    response.status_code = 400
    return response
