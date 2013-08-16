# Makefile.am for HBase-based test.

BUILT_SOURCES = hbase/Hbase.h

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
