# Depends: postgresql-client, postgresql-autodoc

DBNAME = softwareheritage-dev
SQL = swh.sql
SQL_DATA = swh-data.sql
DOCDIR = autodoc


all:

initdb: initdb-stamp
initdb-stamp: createdb-stamp $(SQL)
	psql $(DBNAME) < $(SQL)
	touch $@

createdb: createdb-stamp
createdb-stamp: $(SQL)
	dropdb --if-exists $(DBNAME)
	createdb $(DBNAME)
	touch $@

filldb: filldb-stamp
filldb-stamp: createdb-stamp initdb-stamp $(SQL_DATA)
	psql $(DBNAME) < $(SQL_DATA)
	touch $@

dropdb:
	dropdb $(DBNAME)

doc: doc-stamp
doc-stamp: initdb-stamp $(SQL)
	test -d $(DOCDIR)/ || mkdir $(DOCDIR)
	postgresql_autodoc -d $(DBNAME) -f $(DOCDIR)/swh
	dot -T pdf $(DOCDIR)/swh.dot > $(DOCDIR)/swh.pdf
	touch $@


clean:
	rm -rf *-stamp $(DOCDIR)/

.PHONY: all initdb createdb dropdb doc clean
