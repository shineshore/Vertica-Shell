############################
# Vertica Analytic Database
#
# Makefile to build export user defined functions
#
# To run under valgrind:
#   make RUN_VALGRIND=1 run
#
# Copyright 2012 Vertica, 2012
############################

SDK_HOME?=/opt/vertica/sdk
VSQL?=/opt/vertica/bin/vsql

# Define the .so name here (and update the references in install.sql and uninstall.sql)
PACKAGE_LIBNAME=lib/ParallelExport.so

CXX=g++
# CXXFLAGS=-I ${SDK_HOME}/include -g -Wall -Wno-unused-value -shared -fPIC 

# CXXFLAGS := $(CXXFLAGS) -I $(SDK_HOME)/include -I $(SDK_HOME)/examples/HelperLibraries -O0 -g -fPIC -shared 
# CXXFLAGS := $(CXXFLAGS) -std=c++0x -I $(SDK_HOME)/include -I $(SDK_HOME)/examples/HelperLibraries -O0 -g -Wall -Wno-unused-value -fPIC -shared -DNO_SUDO
CXXFLAGS := $(CXXFLAGS) -std=c++0x -I $(SDK_HOME)/include -I $(SDK_HOME)/examples/HelperLibraries -O0 -g -Wno-unused-value -fPIC -shared -DNO_SUDO

node_list = $(shell vsql -t -c "SELECT node_address FROM nodes;")
current_folder = $(shell pwd)

ifdef RUN_VALGRIND
VALGRIND=valgrind --leak-check=full
endif

ifndef TZ
$(error Timezone (TZ) is NOT set.  This can cause errors when exporting time values)
endif

.PHONY: simulator run test clean clean_all

all: ${PACKAGE_LIBNAME}

${PACKAGE_LIBNAME}: src/*.cpp src/*.c ${SDK_HOME}/include/Vertica.cpp $(SDK_HOME)/include/BuildInfo.h $(SDK_HOME)/examples/HelperLibraries/LoadArgParsers.h
	mkdir -p lib
	$(CXX) $(CXXFLAGS) -o $@ src/*.cpp src/*.c ${SDK_HOME}/include/Vertica.cpp 

# Targets to install and uninstall the library and functions
install: $(PACKAGE_LIBNAME) uninstall ddl/install.sql
	$(VSQL) -f ddl/install.sql
uninstall: ddl/uninstall.sql
	$(VSQL) -f ddl/uninstall.sql

# run examples
run: $(PACKAGE_LIBNAME) install test
	[ -f test/output ] && rm test/output || true
	$(VSQL) -f test/test.sql | tee testresult.txt
	$(VSQL) -A -t -o test/output.verify -c "SELECT * FROM exportdataTEST.all_fields;" | tee testresult.txt
	diff -q test/output test/output.verify | tee testresult.txt
	$(VSQL) -c "DROP SCHEMA exportdataTEST CASCADE;"

# run examples
test: clean test/test.sql
	$(VSQL) -f test/test.sql
	mkdir -p test/export test/export.timestamptz
	$(foreach node, $(node_list), scp $(node):$(current_folder)/test/output test/export/output.$(node);)
	cat test/export/* | sort | tee testresult.txt > test/output
	$(VSQL) -A -t -c "SELECT a,b,c,d,e,f,g,h,i,k,l,j FROM exportdataTEST.all_fields;" | sort| tee testresult.txt > test/output.verify
	diff -q test/output test/output.verify | tee testresult.txt

clean_all:
	$(foreach node, $(node_list), ssh $(node) "[ -f $(current_folder)/test/output ] && rm -f $(current_folder)/test/output || true";)
		
clean: clean_all
	[ -d lib ] && rm -rf lib || true
	[ -f testresult.txt ] && rm -f testresult.txt || true
	[ -f test/output ] && rm -f test/output || true
	[ -f test/output.verify ] && rm -f test/output.verify || true
	[ -d test/export ] && rm -rf test/export || true
	[ -d test/export.timestamptz ] && rm -rf test/export.timestamptz || true
