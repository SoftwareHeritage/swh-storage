# Copyright (C) 2016-2018 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging

import kafka
import msgpack

import swh.storage.db

from swh.core.config import load_named_config
from swh.model import hashutil


CONFIG_BASENAME = 'storage/listener'
DEFAULT_CONFIG = {
    'database': ('str', 'service=softwareheritage'),
    'brokers': ('list[str]', ['getty.internal.softwareheritage.org']),
    'topic_prefix': ('str', 'swh.tmp_journal.new'),
    'poll_timeout': ('int', 10),
}


def decode(object_type, obj):
    """Decode a JSON obj of nature object_type. Depending on the nature of
       the object, this can contain hex hashes
       (cf. `/swh/storage/sql/70-swh-triggers.sql`).

    Args:
        object_type (str): Nature of the object
        obj (str): json dict representation whose values might be hex
          identifier.

    Returns:
        dict representation ready for journal serialization

    """
    value = json.loads(obj)

    if object_type in ('origin', 'origin_visit'):
        result = value
    else:
        result = {}
        for k, v in value.items():
            result[k] = hashutil.hash_to_bytes(v)
    return result


OBJECT_TYPES = {
    'content',
    'skipped_content',
    'directory',
    'revision',
    'release',
    'snapshot',
    'origin_visit',
    'origin',
}


def register_all_notifies(db):
    """Register to notifications for all object types listed in OBJECT_TYPES"""
    with db.transaction() as cur:
        for object_type in OBJECT_TYPES:
            db.register_listener('new_%s' % object_type, cur)
            logging.debug(
                'Registered to events for object type %s', object_type)


def dispatch_notify(topic_prefix, producer, notify):
    """Dispatch a notification to the proper topic"""
    logging.debug('topic_prefix: %s, producer: %s, notify: %s' % (
        topic_prefix, producer, notify))
    channel = notify.channel
    if not channel.startswith('new_') or channel[4:] not in OBJECT_TYPES:
        logging.warn("Got unexpected notify %s" % notify)
        return

    object_type = channel[4:]
    topic = '%s.%s' % (topic_prefix, object_type)
    producer.send(topic, value=decode(object_type, notify.payload))


def run_once(db, producer, topic_prefix, poll_timeout):
    for notify in db.listen_notifies(poll_timeout):
        logging.debug('Notified by event %s' % notify)
        dispatch_notify(topic_prefix, producer, notify)
    producer.flush()


def run_from_config(config):
    """Run the Software Heritage listener from configuration"""
    db = swh.storage.db.Db.connect(config['database'])

    def key_to_kafka(key):
        """Serialize a key, possibly a dict, in a predictable way.

        Duplicated from swh.journal to avoid a cyclic dependency."""
        p = msgpack.Packer(use_bin_type=True)
        if isinstance(key, dict):
            return p.pack_map_pairs(sorted(key.items()))
        else:
            return p.pack(key)

    producer = kafka.KafkaProducer(
        bootstrap_servers=config['brokers'],
        value_serializer=key_to_kafka,
    )

    register_all_notifies(db)

    topic_prefix = config['topic_prefix']
    poll_timeout = config['poll_timeout']
    try:
        while True:
            run_once(db, producer, topic_prefix, poll_timeout)
    except Exception:
        logging.exception("Caught exception")
        producer.flush()


if __name__ == '__main__':
    import click

    @click.command()
    @click.option('--verbose', is_flag=True, default=False,
                  help='Be verbose if asked.')
    def main(verbose):
        logging.basicConfig(
            level=logging.DEBUG if verbose else logging.INFO,
            format='%(asctime)s %(process)d %(levelname)s %(message)s'
        )
        _log = logging.getLogger('kafka')
        _log.setLevel(logging.INFO)

        config = load_named_config(CONFIG_BASENAME, DEFAULT_CONFIG)
        run_from_config(config)

    main()
