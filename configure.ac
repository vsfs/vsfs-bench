#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.61])
AC_INIT([vsbench], [0.0.1], [eddyxu@gmail.com])
AM_INIT_AUTOMAKE
AC_CONFIG_MACRO_DIR([m4])
AC_LIBTOOL_DLOPEN
AC_PROG_LIBTOOL
AC_CONFIG_SRCDIR([vsbench/vsbench.cpp])
AC_CONFIG_HEADERS([config.h])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_LANG([C++])
AC_PROG_CPP

# Checks for libraries.
AX_BOOST_BASE
PKG_CHECK_MODULES([libthrift], [thrift])
AC_CHECK_LIB([glog], [openlog], [], [AC_MSG_ERROR(google-glog was not found.)])

AX_LIB_MYSQL
AC_CHECK_LIB([mysqlpp], [main], [AC_SUBST([MYSQLPP_LIBS], [-lmysqlpp])],
		     [AC_MSG_ERROR(mysqlpp was not found)])

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h sys/time.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_CHECK_HEADER_STDBOOL
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT64_T
AX_CXX_COMPILE_STDCXX_11(noext)
if test "$ac_cv_cxx_compile_cxx11" = no ; then
	AC_MSG_ERROR("It requires C++11 to build.!")
fi

# Checks for library functions.
AC_FUNC_ERROR_AT_LINE

AC_CONFIG_FILES([Makefile
                 apps/mvd/Makefile
                 hbase/Makefile])
AC_OUTPUT
