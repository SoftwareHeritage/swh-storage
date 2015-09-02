PKGNAME = swh.$(shell basename `pwd` | sed 's/.*-//')

NOSE = nosetests3
NOSEFLAGS = -v

all:

.PHONY: test
test:
	$(NOSE) $(NOSEFLAGS)

.PHONY: coverage
coverage:
	$(NOSE) $(NOSEFLAGS) --with-coverage --cover-package $(PKGNAME)
