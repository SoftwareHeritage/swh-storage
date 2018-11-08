# Depends: postgresql-client, postgresql-autodoc

DBNAME = softwareheritage-dev
DOCDIR = autodoc

SQL_INIT    = 10-swh-init.sql
SQL_ENUMS   = 20-swh-enums.sql
SQL_SCHEMA  = 30-swh-schema.sql
SQL_FUNC    = 40-swh-func.sql
SQL_INDEX   = 60-swh-indexes.sql
SQL_TRIGGER = 70-swh-triggers.sql
SQLS = $(SQL_INIT) $(SQL_ENUMS) $(SQL_SCHEMA) $(SQL_FUNC) $(SQL_INDEX) $(SQL_TRIGGER)
SQL_FILES = $(abspath $(addprefix $(CURDIR)/../swh/storage/sql/,$(SQLS)))

PSQL_BIN = psql
PSQL_FLAGS = --echo-errors -X -v ON_ERROR_STOP=1
PSQL = $(PSQL_BIN) $(PSQL_FLAGS)

PIFPAF=$(findstring postgresql://,$(PIFPAF_URLS))

all:

createdb: createdb-stamp
createdb-stamp: $(SQL_FILES)
ifeq ($(PIFPAF),)
	-dropdb $(DBNAME)
endif
	createdb $(DBNAME)
ifeq ($(PIFPAF),)
	touch $@
else
	rm -f $@
endif

filldb: filldb-stamp
filldb-stamp: createdb-stamp
	cat $(SQL_FILES) | $(PSQL) $(DBNAME)
ifeq ($(PIFPAF),)
	touch $@
else
	rm -f $@
endif

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
ifeq ($(PIFPAF),)
	touch $@
else
	rm -f $@
endif

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
