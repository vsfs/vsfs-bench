# vim: ts=8:st=8:noexpandtab

AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS = -I m4
AM_CPPFLAGS = $(libthrift_CFLAGS) $(MYSQL_CFLAGS) -I$(top_srcdir)/lib/vobla \
	      -I${top_srcdir}/lib/vsfs

LDADD = \
  libvsfs-perf.a \
  ${top_srcdir}/vsfs/client/libvsfs-client.a \
  ${top_srcdir}/vsfs/index/libvsfs-index.a \
  ${top_srcdir}/vsfs/rpc/libvsfs-rpc.a \
  ${top_srcdir}/vsfs/common/libvsfs-common.a \
  ${top_srcdir}/base/libbase.a \
  ${MYSQLPP_LIBS} \
  ${libthrift_LIBS}

SUBDIRS = \
  hbase

noinst_LIBRARIES = libvsfs-perf.a
libvsfs_perf_a_SOURCES = \
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
bin_vsbench_SOURCES = vsbench.cpp
