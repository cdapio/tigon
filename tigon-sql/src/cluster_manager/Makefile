#!/bin/make
#SHELL = /bin/sh

all: cluster_manager 

.SUFFIXES:.cpp .cc

CXX=g++

#MACHTYPE = iris64
#SMANDIR = /home/strategy/projects/emf_dev/dist_db/SocketMan-1.3
INCLUDES = -I../../include -I../../include/hfta  -I../ftacmp
# LDLIBS = -lc -lnsl -lsocket -lm
LDLIBS = -L../../lib -lm -lgscphostaux -lgscpapp -lgscphost -lgscpinterface -lgscprts -lgscplftaaux -lgscpaux -lcurl
# C++OPTS = -g -n -D_BSD_SIGNALS -DYYDEBUG=1 $(INCLUDES) 
C++OPTS = -g  -D_BSD_SIGNALS $(INCLUDES) 
C++LDOPTS = -g 
#OUTPUT_DIR = $(MACHTYPE)

# For Driver
CC_ARGS = \
	-g 
CC_OBJECT_FLAG = -o
CC_COMPILE_FLAG = -c
CC_EXECUTABLE_FLAG   = -o 

CPPFLAGS=$(C++OPTS)

TARGETS = cluster_manager
SRCS = $(TARGETS:=.cpp)

# we need to suppress writable strings warnings on Mac for flex/bison generated C++ code
OS= $(shell uname -s)
ifeq ($(OS),Darwin)
	WARN=-Wno-deprecated-writable-strings
endif

INCDIR=../../include 
HFTA_DIR=../../include/hfta

default_targets: $(TARGETS)

%.tab.o: %.tab.cc
	$(CXX) $(CPPFLAGS) $(WARN) $(INCLUDES) -c $< -o $@

%lexer.o: %lexer.cc
	$(CXX) $(CPPFLAGS) $(WARN) $(INCLUDES) -c $< -o $@	

%.o: %.cc
	$(CXX) $(CPPFLAGS) $(INCLUDES) -c $< -o $@

cluster_manager: cluster_manager.o clearinghouse.o res.tab.o reslexer.o app.o qtree.o iface_q.o ifq.tab.o ifqlexer.o cluster.o
	$(CXX) $(CC_ARGS) $? $(LDLIBS) $(CC_EXECUTABLE_FLAG) $@

cluster_manager.cc : xml_parser.h iface_q.h app.h cluster.h $(INCDIR/clearinghouseregistries.h) cluster_manager.h

clearinghouse.cc : $(INCDIR/lapp.h) $(INCDIR/clearinghouseregistries.h) 

res.tab.cc : xml_parser.h app.h iface_q.h qtree.h

reslexer.cc : res.tab.cc.h ../ftacmp/parse_fta.h

app.cc :  app.h

qtree.cc : qtree.h

iface_q.cc : iface_q.h

ifq.tab.cc : ../ftacmp/parse_fta.h 

ifqlexer.cc : ../ftacmp/parse_fta.h ifq.tab.cc.h

cluster.cc : cluster.h

clean:
	rm -f *.o core $(TARGETS) ./ii_files/*.ii

del: 
	rm -f core $(TARGETS)

cleandrive:
	rm -f *.o *.a core driver ./ii_files/*.ii

install: all
	cp cluster_manager ../../bin/ 

# DO NOT DELETE THIS LINE -- make depend depends on it.

