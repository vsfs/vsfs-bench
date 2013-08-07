# Makefile.am for HBase-based test.

noinst_LIBRARIES = libvsbench-voltdb.a

AM_CPPFLAGS = \
  -I${top_srcdir}/lib/vsfs/lib/vobla \
  -I${top_srcdir}/lib/vsfs \
  -isystem${top_srcdir}/voltdb/voltdb-client-cpp-linux-x86_64-3.0/include

libvsbench_voltdb_a_SOURCES = \
  voltdb_driver.h \
  voltdb_driver.cpp

download:
	fab download

.PHONY: download
