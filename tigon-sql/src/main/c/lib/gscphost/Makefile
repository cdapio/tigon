CC=gcc   -O3  -fexpensive-optimizations  -g -I ./include/ -I ../../../include/ -DCLEARINGHOUSE_HEARTBEAT

SOURCE = callbackinterface.c callbackregistries.c lappinterface.c lappregistries.c  lfta.c gscpipc.c 

OBJECTS = $(SOURCE:.c=.o)

all: install

INCDIR=../../include

libgscphost.a: $(OBJECTS) Makefile 
	ar  rc libgscphost.a $(OBJECTS) 

libclearinghouse.a: clearinghouseregistries.o
	ar  rc libclearinghouse.a clearinghouseregistries.o

clearinghouseregistries.o: clearinghouseregistries.c

callbackinterface.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) $(INCDIR/rdtsc.h)

callbackregistries.c : include/callbackregistries.h $(INCDIR/lapp.h) include/gscpipc.h $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h)

lappinterface.c : include/gscpipc.h $(INCDIR/rdtsc.h) $(INCDIR/lapp.h)

lappregistries.c : include/lappregistries.h

lfta.c :$(INCDIR/gsconfig.h) $(INCDIR/fta.h) $(INCDIR/rdtsc.h) $(INCDIR/packet.h)

gscpipc.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) 

clearinghouseregistries.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) include/gscpipc.h

install: libgscphost.a libclearinghouse.a
	cp libgscphost.a libclearinghouse.a ../../../lib/ ; ranlib   ../../../lib/libclearinghouse.a ; ranlib  ../../../lib/libgscphost.a

clean:
	rm -f *.o *.a core
