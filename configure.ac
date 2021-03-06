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

AC_ARG_ENABLE([hadoop],
	      AS_HELP_STRING([--enable-hadoop], [enable hadoop tests]),
		  AC_DEFINE([HAVE_HADOOP], [1], [Define if enable hadoop tests.]))
AM_CONDITIONAL([TEST_HADOOP], [test x"$enable_hadoop" = "xyes"])

AC_ARG_ENABLE([mongodb],
			  AS_HELP_STRING([--enable-mongodb], [enable mongodb tests]),
			  AC_DEFINE([HAVE_MONGODB], [1], [Define if enable mongodb tests.]))
AM_CONDITIONAL([TEST_MONGODB], [test x"$enable_mongodb" = "xyes"])

AC_ARG_ENABLE([mysql],
			  AS_HELP_STRING([--enable-mysql], [enable mysql tests]),
			  AC_DEFINE([HAVE_MYSQL], [1], [Define if enable mysql tests.]))
AM_CONDITIONAL([TEST_MYSQL], [test x"$enable_mysql" = "xyes"])

AC_ARG_ENABLE([voltdb],
			  AS_HELP_STRING([--enable-voltdb], [enable voltdb tests]),
			  AC_DEFINE([HAVE_VOLTDB], [1], [Define if enable voltdb tests.]))
AM_CONDITIONAL([TEST_VOLTDB], [test x"$enable_voltdb" = "xyes"])

# Checks for libraries.
AX_BOOST_BASE
AX_BOOST_FILESYSTEM
AX_BOOST_SYSTEM
PKG_CHECK_MODULES([libthrift], [thrift])
AC_CHECK_LIB([glog], [openlog], [], [AC_MSG_ERROR(google-glog was not found.)])
AC_CHECK_LIB([gflags],[getenv],[],[AC_MSG_ERROR([google-gflags was not found.])])

AS_IF([test "x$enable_mysql" = "xyes"], [
AX_LIB_MYSQL
AC_CHECK_LIB([mysqlpp], [main], [AC_SUBST([MYSQLPP_LIBS], [-lmysqlpp])],
		     [AC_MSG_ERROR(mysqlpp was not found)])
])

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h sys/time.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
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
				 vsbench/hadoop/Makefile
				 vsbench/mongodb/Makefile
				 vsbench/mysql/Makefile
				 vsbench/voltdb/Makefile
				 vsbench/vsfs/Makefile
				 ])
AC_OUTPUT
