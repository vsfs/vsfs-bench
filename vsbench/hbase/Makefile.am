# Makefile.am for HBase-based test.

AM_CXXFLAGS = \
	      -I${top_srcdir}/lib/vsfs/lib/vobla \
	      -I${top_srcdir}/lib/vsfs \
	      $(libthrift_CFLAGS)

BUILT_SOURCES = hbase/Hbase.h
noinst_LIBRARIES = libvsbench-hbase.a

libvsbench_hbase_a_SOURCES = \
  hbase/Hbase.h \
  hbase/Hbase.cpp \
  hbase/Hbase_constants.cpp \
  hbase/Hbase_constants.h \
  hbase/Hbase_types.cpp \
  hbase/Hbase_types.h \
  hbase_driver.cpp \
  hbase_driver.h
libvsbench_hbase_a_LIBADD = \
  ${top_srcdir}/lib/vsfs/vsfs/common/libvsfs-common.a

HBASE_DIR=$(shell fab --hide=running,status hbase_dir)

build_thrift: download  hbase/Hbase.h

hbase/Hbase.h: ${HBASE_DIR}/src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift
	if [ ! -d hbase ]; then \
		mkdir -p hbase; \
		thrift --gen cpp -out hbase ${HBASE_DIR}/src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift; \
	fi

download:
	if [ ! -d ${HBASE_DIR} ]; then fab download; fi

.PHONY: download
