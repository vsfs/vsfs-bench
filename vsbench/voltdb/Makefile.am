# Makefile.am for HBase-based test.

noinst_LIBRARIES = libvsbench-voltdb.a
BUILT_SOURCES = download

AM_CPPFLAGS = \
  -I${top_srcdir}/lib/vsfs \
  -I${top_srcdir}/lib/vsfs/lib/vobla \
  -isystem ${top_srcdir}/vsbench/voltdb/voltdb-client-cpp-linux-x86_64-3.0/include

libvsbench_voltdb_a_SOURCES = \
  voltdb_driver.cpp \
  voltdb_driver.h

libvsbench_voltdb_a_LIBADD = \
  voltdb-client-cpp-linux-x86_64-3.0/libvoltdbcpp.a \
  voltdb-client-cpp-linux-x86_64-3.0/libevent.a \
  voltdb-client-cpp-linux-x86_64-3.0/libevent_pthreads.a

download:
	fab download

.PHONY: download
