#!/bin/make

# ------------------------------------------------
#   Copyright 2014 AT&T Intellectual Property
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# -------------------------------------------

#SHELL = /bin/sh

.SUFFIXES:.cpp .cc

CXX=g++

INCLUDES = -I../../../../include -I../../../../include/hfta
LDLIBS = -L../../../../lib -lm # -lgscphostaux 
C++OPTS = -ggdb -O3  -D_BSD_SIGNALS $(INCLUDES) -DPREFILTER_OK  
C++LDOPTS = -ggdb 

# For Driver
CC_ARGS = \
	-g 
#CC_OBJECT_FLAG = -o
#CC_COMPILE_FLAG = -c
#CC_EXECUTABLE_FLAG   = -o 

CPPFLAGS= $(C++OPTS)

TARGETS = translate_fta gen_tuple_access
SRCS = $(TARGETS:=.cpp)

# we need to suppress writable strings warnings on Mac for flex/bison generated C++ code
OS= $(shell uname -s)
ifeq ($(OS),Darwin)
	WARN=-Wno-deprecated-writable-strings
endif
ifeq ($(OS),Linux)
	WARN=-Wno-write-strings
endif

all: translate_fta gen_tuple_access interfacelib.a
	cp interfacelib.a ../../../../lib/libgscpinterface.a ; ranlib ../../../../lib/libgscpinterface.a

default_targets: $(TARGETS)

%.tab.o: %.tab.cc
	$(CXX) $(CPPFLAGS) $(WARN) $(INCLUDES) -c $< -o $@

%lexer.o: %lexer.cc
	$(CXX) $(CPPFLAGS) $(WARN) $(INCLUDES) -c $< -o $@	

%.o: %.cc
	$(CXX) $(CPPFLAGS) $(INCLUDES) -c $< -o $@

#FTA_OBJECTS:=analyze_fta.o ext_fcns.tab.o ext_fcnslexer.o fta.tab.o ftalexer.o generate_lfta_code.o generate_utils.o parse_schema.o query_plan.o stream_query.o translate_fta.o type_objects.o iface_q.o ifq.tab.o ifqlexer.o res.tab.o reslexer.o niclexer.o nic_def.o nic.tab.o generate_nic_code.o  partn.tab.o partnlexer.o niclexer.o parse_partn.o xmllexer.o xml.tab.o field_list.o

# without niclexer.o
FTA_OBJECTS:=analyze_fta.o ext_fcns.tab.o ext_fcnslexer.o fta.tab.o ftalexer.o generate_lfta_code.o generate_utils.o parse_schema.o query_plan.o stream_query.o translate_fta.o type_objects.o iface_q.o ifq.tab.o ifqlexer.o res.tab.o reslexer.o nic_def.o nic.tab.o generate_nic_code.o  partn.tab.o partnlexer.o niclexer.o parse_partn.o xmllexer.o xml.tab.o field_list.o


GEN_OBJECTS:=fta.tab.o ftalexer.o generate_utils.o parse_schema.o gen_tuple_access.o type_objects.o

LIB_OBJECTS:=fta.tab.o ftalexer.o parse_schema.o type_objects.o schemaparser.o

translate_fta: $(FTA_OBJECTS)
	$(CXX) $(CC_ARGS) $(LDLIBS) $(CC_EXECUTABLE_FLAG) $(FTA_OBJECTS) -o $@

gen_tuple_access: $(GEN_OBJECTS)
	$(CXX) $(CC_ARGS) $(LDLIBS) $(CC_EXECUTABLE_FLAG) $(GEN_OBJECTS) -o $@

interfacelib.a: $(LIB_OBJECTS)
	ar -r $@ $(LIB_OBJECTS) 

#interfacelib.a: fta.tab.o ftalexer.o parse_schema.o type_objects.o schemaparser.o
#	ar -r $@  $?


test_interfacelib: test_interfacelib.o interfacelib.a
	$(CXX) $(CC_ARGS) $(LDLIBS) $(CC_EXECUTABLE_FLAG) $(LIB_OBJECTS) $@

test_interfacelib.cc : schemaparser.h schemaparser_impl.h

# translate_fta source files
translate_fta.cc : parse_fta.h parse_schema.h parse_ext_fcns.h analyze_fta.h query_plan.h generate_lfta_code.h stream_query.h generate_utils.h nic_def.h generate_nic_code.h parse_partn.h print_plan.h

analyze_fta.cc : parse_fta.h parse_schema.h parse_ext_fcns.h analyze_fta.h type_objects.h 

ext_fcnslexer.cc : parse_ext_fcns.h ext_fcns.tab.cc.h 

fta.tab.cc : parse_fta.h parse_schema.h

ftalexer.cc : parse_fta.h parse_schema.h fta.tab.cc.h

generate_lfta_code.cc : parse_fta.h parse_schema.h analyze_fta.h generate_utils.h query_plan.h generate_lfta_code.h generate_nic_code.h

generate_utils.cc : generate_utils.h ../../../../include/lapp.h

parse_schema.cc : parse_fta.h parse_schema.h type_objects.h

query_plan.cc : query_plan.h analyze_fta.h generate_utils.h

stream_query.cc : stream_query.h generate_utils.h analyze_fta.h 

type_objects.cc : type_objects.h

iface_q.cc : iface_q.h

ifq.tab.cc : parse_fta.h

ifqlexer.cc : parse_fta.h ifq.tab.cc.h

res.tab.cc : iface_q.h

reslexer.cc : parse_fta.h res.tab.cc.h

niclexer.cc : nic_def.h nic.tab.cc.h

nic_def.cc : nic_def.h 

nic.tab.cc : nic_def.h 

generate_nic_code.cc : parse_fta.h parse_schema.h analyze_fta.h generate_utils.h query_plan.h generate_nic_code.h analyze_fta.h

partn.tab.cc : parse_partn.h

partnlexer.cc : parse_partn.h partn.tab.cc.h 

niclexer.cc : nic_def.h nic.tab.cc.h

parse_partn.cc : parse_partn.h parse_schema.h analyze_fta.h

xmllexer.cc : parse_fta.h xml.tab.hh 

xml.tab.cc : xml_t.h

field_list.cc : xml_t.h field_list.h

# gen_tuple_access source files
schemaparser.cc : schemaparser_impl.h ../../../../include/schemaparser.h parse_fta.h parse_schema.h generate_utils.h ../../../../include/hfta/host_tuple.h ../../../../include/lapp.h

clean:
	rm -f *.o core $(TARGETS) ./ii_files/*.ii

del: 
	rm -f core $(TARGETS)

cleandrive:
	rm -f *.o *.a core driver ./ii_files/*.ii

install: all
	cp translate_fta ../../../../bin/ ; cp gen_tuple_access ../../../../bin/ ; cp interfacelib.a ../../../../lib/libgscpinterface.a ; ranlib ../../../../lib/libgscpinterface.a

# DO NOT DELETE THIS LINE -- make depend depends on it.

