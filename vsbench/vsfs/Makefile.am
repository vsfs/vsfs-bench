# Makefile.am for Vsfs test.

noinst_LIBRARIES = libvsbench-vsfs.a

AM_CPPFLAGS = \
  $(libthrift_CFLAGS) \
  -I${top_srcdir}/lib/vsfs \
  -I${top_srcdir}/lib/vsfs/lib/vobla

libvsbench_vsfs_a_SOURCES = \
  vsfs_driver.cpp \
  vsfs_driver.h
