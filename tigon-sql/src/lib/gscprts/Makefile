CC=gcc  -g -O3  -fexpensive-optimizations  -I /usr/include/pcap -I libdag/include -I ../../../include/lfta/ -I ../../../include/ -I ./include/ -I ../gscphost/include

SOURCE =   rts_main.c rts_env.c rts_csv.c rts_gdat.c

OBJECTS = $(SOURCE:.c=.o)


all: libgscprts.a 

INCDIR=../../../include
LFTA_DIR=$(INCDIR/lfta)

libgscprts.a: $(OBJECTS) Makefile 
	ar  rc libgscprts.a $(OBJECTS) 

rts_main.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) $(INCDIR/lapp.h) $(INCDIR/fta.h) 

rts_env.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) $(INCDIR/lapp.h) $(INCDIR/fta.h) $(LFTA_DIR/rts.h)

rts_csv.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) $(INCDIR/lapp.h) $(INCDIR/fta.h) $(LFTA_DIR/rts.h) $(INCDIR/packet.h)

rts_gdat..c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) $(INCDIR/lapp.h) $(INCDIR/fta.h) $(LFTA_DIR/rts.h) $(INCDIR/packet.h)

install: all
	cp libgscprts.a  ../../../lib/ ; ranlib ../../../lib/libgscprts.a

clean:
	rm -f *.o *.a core
