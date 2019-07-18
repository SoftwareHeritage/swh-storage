swh-storage
===========

Abstraction layer over the archive, allowing to access all stored source code
artifacts as well as their metadata.

See the
[documentation](https://docs.softwareheritage.org/devel/swh-storage/index.html)
for more details.

## Quick start

### Dependencies

Python tests for this module include tests that cannot be run without
a local Postgresql database, so you need the Postgresql server executable on
your machine (no need to have a running Postgresql server). On a Debian-like
host:

```
$ sudo apt install libpq-dev postgresql
```

### Installation

It is strongly recommended to use a virtualenv. In the following, we
consider you work in a virtualenv named `swh`. See the
[developer setup guide](https://docs.softwareheritage.org/devel/developer-setup.html#developer-setup)
for a more details on how to setup a working environment.


You can install the package directly from
[pypi](https://pypi.org/p/swh.storage):

```
(swh) :~$ pip install swh.storage
[...]
```

Or from sources:

```
(swh) :~$ git clone https://forge.softwareheritage.org/source/swh-storage.git
[...]
(swh) :~$ cd swh-storage
(swh) :~/swh-storage$ pip install .
[...]
```

Then you can check it's properly installed:
```
(swh) :~$ swh storage --help
Usage: swh storage [OPTIONS] COMMAND [ARGS]...

  Software Heritage Storage tools.

Options:
  -h, --help  Show this message and exit.

Commands:
  rpc-serve  Software Heritage Storage RPC server.
```


## Tests

The best way of running Python tests for this module is to use
[tox](https://tox.readthedocs.io/).

```
(swh) :~$ pip install tox
```

### tox

From the sources directory, simply use tox:

```
(swh) :~/swh-storage$ tox
[...]
========= 315 passed, 6 skipped, 15 warnings in 40.86 seconds ==========
_______________________________ summary ________________________________
  flake8: commands succeeded
  py3: commands succeeded
  congratulations :)
```

## Development

The storage server can be locally started. It requires a configuration file and
a running Postgresql database.

### Sample configuration

A typical configuration `storage.yml` file is:

```
storage:
  cls: local
  args:
    db: "dbname=softwareheritage-dev user=<user> password=<pwd>"
    objstorage:
      cls: pathslicing
      args:
        root: /tmp/swh-storage/
        slicing: 0:2/2:4/4:6
```

which means, this uses:

- a local storage instance whose db connection is to
  `softwareheritage-dev` local instance,

- the objstorage uses a local objstorage instance whose:

  - `root` path is /tmp/swh-storage,

  - slicing scheme is `0:2/2:4/4:6`. This means that the identifier of
    the content (sha1) which will be stored on disk at first level
    with the first 2 hex characters, the second level with the next 2
    hex characters and the third level with the next 2 hex
    characters. And finally the complete hash file holding the raw
    content. For example: 00062f8bd330715c4f819373653d97b3cd34394c
    will be stored at 00/06/2f/00062f8bd330715c4f819373653d97b3cd34394c

Note that the `root` path should exist on disk before starting the server.


### Starting the storage server

If the python package has been properly installed (e.g. in a virtual env), you
should be able to use the command:

```
(swh) :~/swh-storage$ swh storage rpc-serve storage.yml
```

This runs a local swh-storage api at 5002 port.

```
(swh) :~/swh-storage$ curl http://127.0.0.1:5002
<html>
<head><title>Software Heritage storage server</title></head>
<body>
<p>You have reached the
<a href="https://www.softwareheritage.org/">Software Heritage</a>
storage server.<br />
See its
<a href="https://docs.softwareheritage.org/devel/swh-storage/">documentation
and API</a> for more information</p>
```

### And then what?

In your upper layer
([loader-git](https://forge.softwareheritage.org/source/swh-loader-git/),
[loader-svn](https://forge.softwareheritage.org/source/swh-loader-svn/),
etc...), you can define a remote storage with this snippet of yaml
configuration.

```
storage:
  cls: remote
  args:
    url: http://localhost:5002/
```

You could directly define a local storage with the following snippet:

```
storage:
  cls: local
  args:
    db: service=swh-dev
    objstorage:
      cls: pathslicing
      args:
        root: /home/storage/swh-storage/
        slicing: 0:2/2:4/4:6
```
