CPPFLAGS = -DNDEBUG -DSTAND_ALONE -I. -I../../../include/
CC = gcc  -O4 -g
CPP = g++ -O4 -g

all: libgscpaux.a

install : all
	cp libgscpaux.a ../../../lib/

libgscpaux.a :  gshub.o json.o block_allocator.o
		ar cr $@ $?
		( ranlib $@ ) 2>/dev/null || true
	cp libgscpaux.a ../../../lib/

INCDIR=../.././include
LFTA_DIR=$(INCDIR/lfta)
HFTA_DIR=$(INCDIR/hfta)


clean: 
	rm -f */*.o */*/*.o *.o libgscpaux.a
