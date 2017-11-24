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

doc: autodoc-stamp $(DOCDIR)/swh.pdf $(DOCDIR)/swh.svg
autodoc-stamp: filldb-stamp
	test -d $(DOCDIR)/ || mkdir $(DOCDIR)
	postgresql_autodoc -d $(DBNAME) -f $(DOCDIR)/swh
	cp -a $(DOCDIR)/swh.dot $(DOCDIR)/swh.dot.orig
	touch $@

$(DOCDIR)/swh.dot: clusters.dot autodoc-stamp
	bin/dot_add_content $(DOCDIR)/swh.dot.orig clusters.dot > $(DOCDIR)/swh.dot

$(DOCDIR)/swh.pdf: $(DOCDIR)/swh.dot
	dot -T pdf $< > $@
$(DOCDIR)/swh.svg: $(DOCDIR)/swh.dot
	dot -T svg $< > $@

clean:
	rm -rf *-stamp $(DOCDIR)/

distclean: clean dropdb
	rm -f swh.dump

.PHONY: all initdb createdb dropdb doc clean
