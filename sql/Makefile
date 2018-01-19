# Depends: postgresql-client, postgresql-autodoc

DBNAME = softwareheritage-dev
DOCDIR = autodoc

SQL_INIT    = swh-init.sql
SQL_ENUMS   = swh-enums.sql
SQL_SCHEMA  = swh-schema.sql
SQL_FUNC    = swh-func.sql
SQL_DATA    = swh-data.sql
SQL_INDEX   = swh-indexes.sql
SQL_TRIGGER = swh-triggers.sql
SQLS = $(SQL_INIT) $(SQL_ENUMS) $(SQL_SCHEMA) $(SQL_FUNC) $(SQL_INDEX) $(SQL_TRIGGER) $(SQL_DATA)

PSQL_BIN = psql
PSQL_FLAGS = --echo-all -X -v ON_ERROR_STOP=1
PSQL = $(PSQL_BIN) $(PSQL_FLAGS)

all:

createdb: createdb-stamp
createdb-stamp: $(SQL_INIT)
	-dropdb $(DBNAME)
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

$(DOCDIR):
	test -d $(DOCDIR)/ || mkdir $(DOCDIR)

doc: autodoc-stamp $(DOCDIR)/db-schema.pdf $(DOCDIR)/db-schema.svg
autodoc-stamp: filldb-stamp $(DOCDIR)
	postgresql_autodoc -d $(DBNAME) -f $(DOCDIR)/db-schema
	cp -a $(DOCDIR)/db-schema.dot $(DOCDIR)/db-schema.dot.orig
	touch $@

$(DOCDIR)/db-schema.dot: clusters.dot autodoc-stamp $(DOCDIR)
	bin/dot_add_content $(DOCDIR)/db-schema.dot.orig clusters.dot > $(DOCDIR)/db-schema.dot

$(DOCDIR)/db-schema.pdf: $(DOCDIR)/db-schema.dot autodoc-stamp
	dot -T pdf $< > $@
$(DOCDIR)/db-schema.svg: $(DOCDIR)/db-schema.dot autodoc-stamp
	dot -T svg $< > $@

clean:
	rm -rf *-stamp $(DOCDIR)/

distclean: clean dropdb
	rm -f swh.dump

.PHONY: all initdb createdb dropdb doc clean
