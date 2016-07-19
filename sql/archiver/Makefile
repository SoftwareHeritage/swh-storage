# Depends: postgresql-client, postgresql-autodoc

DBNAME = softwareheritage-archiver-dev
DOCDIR = autodoc

SQL_INIT   = ../swh-init.sql
SQL_SCHEMA = swh-archiver-schema.sql
SQL_DATA   = swh-archiver-data.sql
SQLS = $(SQL_INIT) $(SQL_SCHEMA) $(SQL_DATA)

PSQL_BIN = psql
PSQL_FLAGS = --single-transaction --echo-all -X
PSQL = $(PSQL_BIN) $(PSQL_FLAGS)


all:

createdb: createdb-stamp
createdb-stamp: $(SQL_INIT)
	createdb $(DBNAME)
	touch $@

filldb: filldb-stamp
filldb-stamp: createdb-stamp
	cat $(SQLS) | $(PSQL) $(DBNAME)
	touch $@

dropdb:
	-dropdb $(DBNAME)

dumpdb: swh-archiver.dump
swh.dump: filldb-stamp
	pg_dump -Fc $(DBNAME) > $@

clean:
	rm -rf *-stamp $(DOCDIR)/

distclean: clean dropdb
	rm -f swh.dump

.PHONY: all initdb createdb dropdb doc clean
