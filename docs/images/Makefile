
BUILD_TARGETS =
BUILD_TARGETS += archive-copies.pdf archive-copies.svg

all: $(BUILD_TARGETS)


# dia exporters

%.eps: %.dia
	dia -t eps --export $@ $<

%.svg: %.dia
	dia -t svg --export $@ $<

# generic converters

%.pdf: %.eps
	epstopdf $<


clean:
	-rm -f $(BUILD_TARGETS)
