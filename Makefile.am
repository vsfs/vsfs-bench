# vim: ts=8:st=8:noexpandtab

AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS = -I m4

CXX=mpic++
MPI_COMPILE_FLAGS = $(shell mpicc --showme:compile)
MPI_LINK_FLAGS = $(shell mpicc --showme:link)

AM_CPPFLAGS = $(libthrift_CFLAGS) $(MYSQL_CFLAGS) \
	      -I$(top_srcdir)/lib/vobla \
	      -I${top_srcdir}/lib/vsfs \
	      -I${top_srcdir}/lib/vsfs/lib/gmock-1.6.0/gtest/include \
	      $(MPI_COMPILE_FLAGS)

AM_LDFLAGS = $(MPI_LINK_FLAGS)

LDADD = \
  libvsfs-vsbench.a \
  ${top_srcdir}/lib/vsfs/vsfs/client/libvsfs-client.a \
  ${top_srcdir}/lib/vsfs/vsfs/index/libvsfs-index.a \
  ${top_srcdir}/lib/vsfs/vsfs/rpc/libvsfs-rpc.a \
  ${top_srcdir}/lib/vsfs/vsfs/common/libvsfs-common.a \
  ${top_srcdir}/lib/vsfs/lib/vobla/vobla/libvobla.la \
  -lboost_thread \
  ${MYSQLPP_LIBS} ${BOOST_FILESYSTEM_LIB} ${BOOST_SYSTEM_LIB} \
  ${BOOST_THREAD_LIB} \
  ${libthrift_LIBS} -lmpi_cxx

noinst_LIBRARIES = libvsfs-vsbench.a
SUBDIRS = vsbench/vsfs
noinst_PROGRAMS = bin/vsbench

libvsfs_vsbench_a_SOURCES = \
  vsbench/driver.cpp \
  vsbench/driver.h \
  vsbench/util.cpp \
  vsbench/util.h

if TEST_HADOOP
SUBDIRS += vsbench/hadoop
libvsfs_vsbench_a_SOURCES += \
  vsbench/hadoop/hbase/Hbase.cpp \
  vsbench/hadoop/hbase/Hbase.h \
  vsbench/hadoop/hbase/Hbase_constants.cpp \
  vsbench/hadoop/hbase/Hbase_constants.h \
  vsbench/hadoop/hbase/Hbase_types.cpp \
  vsbench/hadoop/hbase/Hbase_types.h \
  vsbench/hadoop/hbase_driver.cpp \
  vsbench/hadoop/hbase_driver.h
noinst_PROGRAMS += vsbench/hadoop/hbase_testtool
vsbench_hadoop_hbase_testtool_SOURCES = vsbench/hadoop/hbase_testtool.cpp
endif

if TEST_MONGODB
SUBDIRS += vsbench/mongodb
AM_CPPFLAGS += -isystem vsbench/mongodb/mongo-cxx-driver-nightly/src
AM_LDFLAGS += -Lvsbench/mongodb/mongo-cxx-driver-nightly/build
LDADD += ${top_srcdir}/vsbench/mongodb/libvsbench-mongodb.a -lmongoclient.a
endif

if TEST_VOLTDB
SUBDIRS += vsbench/voltdb
LDADD += \
  ${top_srcdir}/vsbench/voltdb/libvsbench-voltdb.a \
  ${top_srcdir}/vsbench/voltdb/voltdb-client-cpp-linux-x86_64-3.0/libvoltdbcpp.a \
  ${top_srcdir}/vsbench/voltdb/voltdb-client-cpp-linux-x86_64-3.0/libevent.a \
  ${top_srcdir}/vsbench/voltdb/voltdb-client-cpp-linux-x86_64-3.0/libevent_pthreads.a
endif

if TEST_MYSQL
libvsfs_vsbench_a_SOURCES += \
  vsbench/mysql/mysql_driver.cpp \
  vsbench/mysql/mysql_driver.h
endif

bin_vsbench_SOURCES = vsbench/vsbench.cpp
