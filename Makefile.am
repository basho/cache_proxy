MAINTAINERCLEANFILES = Makefile.in aclocal.m4 configure config.h.in config.h.in~ stamp-h.in

ACLOCAL_AMFLAGS = -I m4

SUBDIRS = contrib src

unittests-nutcracker:
	cp src/nutcracker tests/_binaries
	cd tests && ./nosetests.sh $(NOSE_TESTS)

test: unittests-nutcracker

# generate sequence charts
mscgen:
	for i in `ls notes/*.msc`; do mscgen -T png -o $$i.png -i $$i ; done

dist_man_MANS = man/nutcracker.8

EXTRA_DIST = README.md NOTICE LICENSE ChangeLog conf scripts notes
