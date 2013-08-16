# Makefile.am for HBase-based test.

CLEANFILES = *.err *.out

download:
	fab download

noinst_LIBRARIES = libvsbench-mongodb.a

AM_CXXFLAGS = \
  -I${top_srcdir}/lib/vsfs/lib/vobla \
  -I${top_srcdir}/lib/vsfs \
  -isystem mongo-cxx-driver-nightly/src

AM_LDFLAGS = -Lvsbench/mongodb/mongo-cxx-driver-nightly/build

libvsbench_mongodb_a_SOURCES = \
  mongodb_driver.h \
  mongodb_driver.cpp

.PHONY: download
