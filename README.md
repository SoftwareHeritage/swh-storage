swh-storage
===========

Abstraction layer over the archive, allowing to access all stored source code
artifacts as well as their metadata.

See the
[documentation](https://docs.softwareheritage.org/devel/swh-storage/index.html)
for more details.

Tests
-----

Python tests for this module include tests that cannot be run without a local
Postgres database. You are not obliged to run those tests though:

- `make test`:      will run all tests
- `make test-nodb`: will run only tests that do not need a local DB
- `make test-db`:   will run only tests that do need a local DB

If you do want to run DB-related tests, you should ensure you have access zith
sufficient privileges to a Postgresql database.

### Using your system database

You need to ensure that your user is authorized to create and drop DBs, and in
particular DBs named "softwareheritage-test" and "softwareheritage-dev"

Note: the testdata repository (swh-storage-testdata) is not required any more.

### Using pifpaf

[pifpaf](https://github.com/jd/pifpaf) is a suite of fixtures and a
command-line tool that allows to start and stop daemons for a quick throw-away
usage.

It can be used to run tests that need a Postgres database without any other
configuration reauired nor the need to have special access to a running
database:

```bash

$ pifpaf run postgresql make test-db
[snip]
----------------------------------------------------------------------
Ran 124 tests in 56.203s

OK
```

Note that pifpaf is not yet available as a Debian package, so you may have to
install it in a venv.


Development
-----------

A test server could locally be running for tests.

### Sample configuration

In either /etc/softwareheritage/storage/storage.yml,
~/.config/swh/storage.yml or ~/.swh/storage.yml:

```
storage:
  cls: local
  args:
    db: "dbname=softwareheritage-dev user=<user>"
    objstorage:
      cls: pathslicing
      args:
        root: /home/storage/swh-storage/
        slicing: 0:2/2:4/4:6
```

which means, this uses:

- a local storage instance whose db connection is to
  softwareheritage-dev local instance

- the objstorage uses a local objstorage instance whose:

  - root path is /home/storage/swh-storage

  - slicing scheme is 0:2/2:4/4:6. This means that the identifier of
    the content (sha1) which will be stored on disk at first level
    with the first 2 hex characters, the second level with the next 2
    hex characters and the third level with the next 2 hex
    characters. And finally the complete hash file holding the raw
    content. For example: 00062f8bd330715c4f819373653d97b3cd34394c
    will be stored at 00/06/2f/00062f8bd330715c4f819373653d97b3cd34394c

Note that the 'root' path should exist on disk.


### Run server

Command:
```
python3 -m swh.storage.api.server ~/.config/swh/storage.yml
```

This runs a local swh-storage api at 5002 port.


### And then what?

In your upper layer (loader-git, loader-svn, etc...), you can define a
remote storage with this snippet of yaml configuration.

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
