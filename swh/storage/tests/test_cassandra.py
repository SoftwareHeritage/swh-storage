# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import signal
import socket
import subprocess
import time
import unittest

import pytest
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

from swh.storage.cassandra import CassandraStorage

from swh.storage.tests.test_storage import \
    CommonTestStorage, CommonPropTestStorage


CONFIG_TEMPLATE = '''
data_file_directories:
    - {data_dir}/data
commitlog_directory: {data_dir}/commitlog
hints_directory: {data_dir}/hints
saved_caches_directory: {data_dir}/saved_caches

commitlog_sync: periodic
commitlog_sync_period_in_ms: 10000
partitioner: org.apache.cassandra.dht.Murmur3Partitioner
endpoint_snitch: SimpleSnitch
seed_provider:
    - class_name: org.apache.cassandra.locator.SimpleSeedProvider
      parameters:
          - seeds: "127.0.0.1"

storage_port: {storage_port}
native_transport_port: {native_transport_port}
start_native_transport: true
listen_address: 127.0.0.1
start_rpc: false

'''


def free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_for_peer(addr, port):
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((addr, port))
        except ConnectionRefusedError:
            time.sleep(0.1)
        else:
            sock.close()
            break


@pytest.fixture(scope='session')
def cassandra_cluster(tmpdir_factory):
    cassandra_conf = tmpdir_factory.mktemp('cassandra_conf')
    cassandra_data = tmpdir_factory.mktemp('cassandra_data')
    native_transport_port = free_port()
    storage_port = free_port()
    jmx_port = free_port()

    with open(str(cassandra_conf.join('cassandra.yaml')), 'w') as fd:
        fd.write(CONFIG_TEMPLATE.format(
            data_dir=str(cassandra_data),
            storage_port=storage_port,
            native_transport_port=native_transport_port,
        ))
    with open(str(cassandra_conf.join('jvm.options')), 'w') as fd:
        fd.write('-Xmn=1M -Xms=10M -XMx=100M\n')  # some sane values

    proc = subprocess.Popen(
        [
            '/usr/sbin/cassandra',
            '-Dcassandra.config=file://%s/cassandra.yaml' % cassandra_conf,
            '-Dcassandra.logdir=%s' % cassandra_data.join('log'),
            '-Dcassandra.jmx.local.port=%d' % jmx_port,
        ],
        start_new_session=True,
        env={
            'CASSANDRA_CONF': str(cassandra_conf.join('jvm.options')),
            'MAX_HEAP_SIZE': '100M',
            'HEAP_NEWSIZE': '10M',
        }
    )

    wait_for_peer('127.0.0.1', native_transport_port)

    print('foo')
    yield Cluster(
        ['127.0.0.1'], port=native_transport_port,
        load_balancing_policy=RoundRobinPolicy())
    print('bar')

    pgrp = os.getpgid(proc.pid)
    os.killpg(pgrp, signal.SIGKILL)


@pytest.fixture(scope='class')
def class_cassandra_cluster(request, cassandra_cluster):
    request.cls.cassandra_cluster = cassandra_cluster


@pytest.mark.usefixtures('class_cassandra_cluster')
class TestCassandraStorage(CommonTestStorage, unittest.TestCase):
    """Test the Cassandra storage API

    This class doesn't define any tests as we want identical
    functionality between local and remote storage. All the tests are
    therefore defined in CommonTestStorage.
    """
    def setUp(self):
        super().setUp()
        self.storage = CassandraStorage()
        keyspace = os.urandom(10).hex()
        session = self.cassandra_cluster.connect()
        session.execute('''CREATE KEYSPACE "%s"
                           WITH REPLICATION = {
                               'class' : 'SimpleStrategy',
                               'replication_factor' : 1
                           };
                        ''' % keyspace)
        self.storage._session = self.cassandra_cluster.connect(keyspace)

    @pytest.mark.skip('postgresql-specific test')
    def test_content_add_db(self):
        pass

    @pytest.mark.skip('postgresql-specific test')
    def test_skipped_content_add_db(self):
        pass

    @pytest.mark.skip('postgresql-specific test')
    def test_content_add_metadata_db(self):
        pass

    @pytest.mark.skip(
        'not implemented, see https://forge.softwareheritage.org/T1633')
    def test_skipped_content_add(self):
        pass


@pytest.mark.property_based
class PropTestCassandraStorage(CommonPropTestStorage, unittest.TestCase):
    """Test the Cassandra storage API

    This class doesn't define any tests as we want identical
    functionality between local and remote storage. All the tests are
    therefore defined in CommonPropTestStorage.
    """
    def setUp(self):
        super().setUp()
        self.storage = CassandraStorage()

    def reset_storage_tables(self):
        self.storage = CassandraStorage()
