# Copyright (C) 2016 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging

import kafka
import msgpack

from swh.core.config import load_named_config
import swh.storage.db

CONFIG_BASENAME = 'storage/listener'
DEFAULT_CONFIG = {
    'database': ('str', 'service=softwareheritage'),
    'brokers': ('list[str]', ['getty.internal.softwareheritage.org']),
    'topic_prefix': ('str', 'swh.tmp_journal.new'),
    'poll_timeout': ('int', 10),
}


def decode_sha(value):
    """Decode the textual representation of a SHA hash"""
    if isinstance(value, str):
        return bytes.fromhex(value)
    return value


def decode_json(value):
    """Decode a JSON value containing hashes and other types"""
    value = json.loads(value)

    return {k: decode_sha(v) for k, v in value.items()}


OBJECT_TYPES = {
    'content': decode_sha,
    'skipped_content': decode_json,
    'directory': decode_sha,
    'revision': decode_sha,
    'release': decode_sha,
    'origin_visit': decode_json,
}


def register_all_notifies(db):
    """Register to notifications for all object types listed in OBJECT_TYPES"""
    with db.transaction() as cur:
        for object_type in OBJECT_TYPES:
            db.register_listener('new_%s' % object_type, cur)


def dispatch_notify(topic_prefix, producer, notify):
    """Dispatch a notification to the proper topic"""
    channel = notify.channel
    if not channel.startswith('new_') or channel[4:] not in OBJECT_TYPES:
        logging.warn("Got unexpected notify %s" % notify)
        return

    object_type = channel[4:]

    topic = '%s.%s' % (topic_prefix, object_type)
    data = OBJECT_TYPES[object_type](notify.payload)
    producer.send(topic, value=data)


def run_from_config(config):
    """Run the Software Heritage listener from configuration"""
    db = swh.storage.db.Db.connect(config['database'])

    def kafka_serializer(data):
        return msgpack.dumps(data, use_bin_type=True)

    producer = kafka.KafkaProducer(
        bootstrap_servers=config['brokers'],
        value_serializer=kafka_serializer,
    )

    register_all_notifies(db)

    topic_prefix = config['topic_prefix']
    poll_timeout = config['poll_timeout']
    try:
        while True:
            for notify in db.listen_notifies(poll_timeout):
                dispatch_notify(topic_prefix, producer, notify)
            producer.flush()
    except Exception:
        logging.exception("Caught exception")
        producer.flush()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(process)d %(levelname)s %(message)s'
    )
    config = load_named_config(CONFIG_BASENAME, DEFAULT_CONFIG)
    run_from_config(config)
