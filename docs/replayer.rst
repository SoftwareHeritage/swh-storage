.. _swh-storage-replayer:

Storage replayer
================

The storage replayer is a mechanism allowing to replicate a storage database
using the :ref:`swh-journal` (based on Kafka_).

This can be used to feed |swh| mirrors, to asynchronously replicate a given
storage database using a given backend (e.g. Cassandra) to another backend
(e.g. Postgresql), etc.

The replayer itself is a cli tool `swh storage replay` that takes the usual
|swh| configuration file and runs indefinitely, consuming Kafka_ topics from
the kafka cluster and inserting |swh| model objects in the destination storage.

A typical ``replayer-config.yml`` configuration file for a replayer would be:

.. code-block:: yaml

   storage:
     cls: pipeline
     steps:
       - cls: tenacious
       - cls: remote
         url: http://my-storage:5002

   journal_client:
     cls: kafka
     brokers:
       - kafka-broker1:9093
       - kafka-broker2:9093
     group_id: my-user-replayer-01
     privileged: true
     session.timeout.ms: 600000
     socket.timeout.ms: 120000
     fetch.wait.max.ms: 30000
     max.poll.interval.ms: 3600000
     message.max.bytes: 1000000000
     batch_size: 200
     fetch.message.max.bytes: 1048570

   replayer:
     error_reporter:
       host: redis
       port: 6379
       db: 10

Then you can start one replayer worker:

.. code-block:: shell

   (swh) :~$ swh storage -C replayer-config.yml


Error reporting
---------------

Objects retrieved from kafka may not be possible to ingest. This can happen for
several reasons and is generally not an issue (there are generally good reasons
not to ingest them if the local storage's validation says so).

Depending on the type of error occurring while decoding and ingestion the
object, it may be a known case, in which case it is OK to ignore it, but it may
be some other situation in which case there are 2 possibilities:

- you want the replaying to stop ingesting objects in order to identify the
  reason for this error to occur (which generally involve fixing the code
  before being able to resume the relaying process), in which case you should
  not use a ``tenacious`` proxy in your storage configuration, or
- you may chose to ignore it and continue the replaying process, using the
  ``tenacious`` proxy storage.

In this second situation, it is key to keep track of failures.

They will be reported in the logging system, but this is not always the best
way to keep track of these specific errors.

For this, there is an error reporting mechanism provided by the replayer,
typically using a Redis_ key/value DB to collect the errors.

This is enabled by configuring the ``replayer/error_reporter`` section in the
config file.

Each time an object failed to be deserialized from kafka or ingested in the
storage, it will be logged in Redis using the following format:

- the key will be formatted like ``<timestamp>/<object_type>[:<identifier>]`` where:

  - ``timestamp`` is the timestamp, as an ISO date format, when the failure occurred
  - ``object_type`` is one of the possible ``ModelObject.object_type`` constant
    ("origin", "revision", etc.)
  - ``identifier`` is an optional identifier for the object, if available a report time.

- the value will be a yaml-encoded dictionary with 2 or 3 keys:

  - ``obj`` a dict representation of the failed object (as produced by
    ``ModelObject.to_dict()``), if available,
  - ``exc`` a list of strings representing the exception at the root of the
    failure, if any (None otherwise),
  - ``kafka_message`` if present, the dump of the kafka message representing the object




.. _Kafka: https://kafka.apache.org/
.. _Redis: https://redis.io/
