# Makefile.am for MySQL
#
# Author: Lei Xu

noinst_LIBRARIES = libvsbench-mysql.a
AM_CPPFLAGS = $(MYSQL_CFLAGS) \
	      -I$(top_srcdir)/lib/vobla \
	      -I$(top_srcdir)/lib/vsfs

LDADD = ${MYSQLPP_LIBS}

libvsbench_mysql_a_SOURCES = \
  mysql_driver.cpp \
  mysql_driver.h
