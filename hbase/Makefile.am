# Makefile.am for HBase-based test.

BUILT_SOURCES = hbase

HBASE_DIR=$(shell fab --hide=running,status hbase_dir)
hbase: download ${HBASE_DIR}/src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift
	if [ ! -d hbase ]; then mkdir hbase; fi
	thrift --gen cpp -out hbase ${HBASE_DIR}/src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift

download:
	if [ ! -d ${HBASE_DIR} ]; then fab d

.PHONY: download
