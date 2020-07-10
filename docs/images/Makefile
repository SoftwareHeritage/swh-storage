
BUILD_TARGETS =
BUILD_TARGETS += swh-archive-copies.pdf swh-archive-copies.svg

all: $(BUILD_TARGETS)

%.svg: %.dia
	dia -e $@ $<

%.pdf: %.svg
	set -e; if [ $$(inkscape --version 2>/dev/null | grep -Eo '[0-9]+' | head -1) -gt 0 ]; then \
	  inkscape -o $@ $< ; \
	else \
	  inkscape -A $@ $< ; \
	fi

clean:
	-rm -f $(BUILD_TARGETS)
