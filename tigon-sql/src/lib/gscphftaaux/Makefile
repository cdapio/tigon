CXX= g++ -O3  -fexpensive-optimizations -g -I ../gscpaux/ -I ../../../include  -I ../../../include/hfta -I ../../../include/lfta
CC= gcc  -fexpensive-optimizations -O3 -I ../../../include -I ../../../include/lfta 

INCDIR=../../../include
HFTA_DIR=$(INCDIR/hfta)
LFTA_DIR=$(INCDIR/lfta)

all: libgscphftaaux.a

install: all
	cp libgscphftaaux.a ../../../lib ; ranlib ../../../lib/libgscphftaaux.a

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

