# Depends: postgresql-client, postgresql-autodoc

DBNAME = softwareheritage-dev
DOCDIR = autodoc

SQL_INIT   = swh-init.sql
SQL_SCHEMA = swh-schema.sql
SQL_FUNC   = swh-func.sql
SQL_DATA   = swh-data.sql
SQLS = $(SQL_INIT) $(SQL_SCHEMA) $(SQL_FUNC) $(SQL_DATA)

PSQL_BIN = psql
PSQL_FLAGS = --single-transaction --echo-all
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

dumpdb: swh.dump
swh.dump: filldb-stamp
	pg_dump -Fc $(DBNAME) > $@

doc: doc-stamp
doc-stamp: filldb-stamp
	test -d $(DOCDIR)/ || mkdir $(DOCDIR)
	postgresql_autodoc -d $(DBNAME) -f $(DOCDIR)/swh
	bin/dot_add_content $(DOCDIR)/swh.dot clusters.dot
	dot -T pdf $(DOCDIR)/swh.dot > $(DOCDIR)/swh.pdf
	touch $@

clean:
	rm -rf *-stamp $(DOCDIR)/

distclean: clean dropdb
	rm -f swh.dump

.PHONY: all initdb createdb dropdb doc clean
