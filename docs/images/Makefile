
BUILD_TARGETS =
BUILD_TARGETS += swh-archive-copies.pdf swh-archive-copies.svg

all: $(BUILD_TARGETS)


%.svg: %.dia
	inkscape -l $@ $<

%.pdf: %.dia
	inkscape -A $@ $<


clean:
	-rm -f $(BUILD_TARGETS)
