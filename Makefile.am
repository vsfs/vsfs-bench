# vim: ts=8:st=8:noexpandtab

AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS = -I m4

MPI_COMPILE_FLAGS = $(shell mpicc --showme:compile)
MPI_LINK_FLAGS = $(shell mpicc --showme:link)

AM_CPPFLAGS = $(libthrift_CFLAGS) $(MYSQL_CFLAGS) -I$(top_srcdir)/lib/vobla \
	      -I${top_srcdir}/lib/vsfs \
	      -I${top_srcdir}/lib/vsfs/lib/gmock-1.6.0/gtest/include \
	      -isystem vsbench/mongodb/mongo-cxx-driver-nightly/src \
	      $(MPI_COMPILE_FLAGS)

AM_LDFLAGS = $(MPI_LINK_FLAGS) \
	     -Lvsbench/mongodb/mongo-cxx-driver-nightly/build \
	     -Lvoltdb/voltdb-client-cpp-linux-x86_64-3.0

LDADD = \
  libvsfs-vsbench.a \
  ${top_srcdir}/lib/vsfs/vsfs/client/libvsfs-client.a \
  ${top_srcdir}/lib/vsfs/vsfs/index/libvsfs-index.a \
  ${top_srcdir}/lib/vsfs/vsfs/rpc/libvsfs-rpc.a \
  ${top_srcdir}/lib/vsfs/vsfs/common/libvsfs-common.a \
  ${top_srcdir}/lib/vsfs/lib/vobla/vobla/libvobla.la \
  -lboost_thread -lmongoclient \
  ${top_srcdir}/vsbench/mongodb/libvsbench-mongodb.a \
  ${top_srcdir}/voltdb/libvsbench-voltdb.a \
  ${top_srcdir}/voltdb/voltdb-client-cpp-linux-x86_64-3.0/libvoltdbcpp.a \
  ${top_srcdir}/voltdb/voltdb-client-cpp-linux-x86_64-3.0/libevent.a \
  ${top_srcdir}/voltdb/voltdb-client-cpp-linux-x86_64-3.0/libevent_pthreads.a \
  ${MYSQLPP_LIBS} ${BOOST_FILESYSTEM_LIB} ${BOOST_SYSTEM_LIB} \
  ${libthrift_LIBS} \
  -lmpi_cxx

SUBDIRS = \
  hbase \
  vsbench/mongodb \
  voltdb

noinst_LIBRARIES = libvsfs-vsbench.a
libvsfs_vsbench_a_SOURCES = \
  hbase/hbase/Hbase.cpp \
  hbase/hbase/Hbase.h \
  hbase/hbase/Hbase_constants.cpp \
  hbase/hbase/Hbase_constants.h \
  hbase/hbase/Hbase_types.cpp \
  hbase/hbase/Hbase_types.h \
  hbase/hbase_driver.cpp \
  hbase/hbase_driver.h \
  mysql/mysql_driver.cpp \
  mysql/mysql_driver.h \
  vsbench/driver.cpp \
  vsbench/driver.h \
  vsbench/util.cpp \
  vsbench/util.h \
  vsfs/vsfs_driver.cpp \
  vsfs/vsfs_driver.h

noinst_PROGRAMS = \
  bin/vsbench \
  hbase/hbase_testtool

hbase_hbase_testtool_SOURCES = hbase/hbase_testtool.cpp
bin_vsbench_SOURCES = vsbench/vsbench.cpp
