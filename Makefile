BASENAME := $(shell basename `pwd` | sed 's/.*-//')

PKGNAME = swh.$(BASENAME)
PKGDIR = swh/$(BASENAME)
EXTRA_DIRS := $(shell test -d bin && echo bin)

NOSE = nosetests3
NOSEFLAGS = -v
FLAKE = flake8
FLAKEFLAGS =

all:

.PHONY: test
test:
	$(NOSE) $(NOSEFLAGS)

.PHONY: coverage
coverage:
	$(NOSE) $(NOSEFLAGS) --with-coverage --cover-package $(PKGNAME)

.PHONY: check
check:
	$(FLAKE) $(FLAKEFLAGS) $(PKGDIR) $(EXTRA_DIRS)

-include Makefile.local
