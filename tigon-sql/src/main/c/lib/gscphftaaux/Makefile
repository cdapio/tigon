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

CXX= g++ -O3  -fexpensive-optimizations -g -I ../gscpaux/ -I ../../../../../include  -I ../../../../../include/hfta -I ../../../../../include/lfta
CC= gcc  -fexpensive-optimizations -O3 -I ../../../../../include -I ../../../../../include/lfta 

INCDIR=../../../include
HFTA_DIR=$(INCDIR/hfta)
LFTA_DIR=$(INCDIR/lfta)

all: libgscphftaaux.a

install: all
	cp libgscphftaaux.a ../../../../../lib ; ranlib ../../../../../lib/libgscphftaaux.a

libgscphftaaux.a:   SSstateful_count_distinct.o Manku_HH.o hfta_runtime_library.o  Makefile  hfta_udaf.o  
	ar  rc libgscphftaaux.a  SSstateful_count_distinct.o Manku_HH.o  hfta_runtime_library.o   hfta_udaf.o  

hfta_runtime_library.o: hfta_runtime_library.cc

hfta_runtime_library.cc : $(HFTA_DIR/host_tuple.h) $(INCDIR/fta.h)

hfta_udaf.o: hfta_udaf.cc

hfta_udaf.cc : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) $(HFTA_DIR/hfta_udaf.h) $(LFTA_DIR/rts_udaf.h)

Manku_HH.o: Manku_HH.cc
	$(CXX) -c -o Manku_HH.o Manku_HH.cc

SSstateful_count_distinct.o: SSstateful_count_distinct.cc
	$(CXX) -c -o SSstateful_count_distinct.o SSstateful_count_distinct.cc


clean:
	rm -f *.o *.a core

