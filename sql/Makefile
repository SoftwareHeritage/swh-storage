# Depends: postgresql-client, postgresql-autodoc

DBNAME = softwareheritage-dev
DOCDIR = autodoc

SQL_FILES = $(sort $(wildcard $(CURDIR)/../swh/storage/sql/*.sql))

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
