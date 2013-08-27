# Makefile.am for VoltDB test.

noinst_LIBRARIES = libvsbench-voltdb.a
BUILT_SOURCES = download vsfs.jar

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

CLASSPATH=./voltdb-3.4/voltdb/voltdb-3.4.jar:./voltdb-3.4/lib/*.jar
download:
	fab download

vsfs.jar: vsfs.sql src/vsbench/procedures/*.java
	javac -classpath ${CLASSPATH} \
		src/vsbench/procedures/*.java -d obj
	voltdb-3.*/bin/voltdb compile --classpath obj -o vsfs.jar vsfs.sql


.PHONY: download
