CC=gcc -g -O3  -fexpensive-optimizations -I ../gscpaux/acl/ -I ../gscpaux/ -I ../../../include/lfta/ -I ../../../include/

SOURCE =  rts_string.c rts_byteswap.c rts_sample.c rts_udaf.c 

OBJECTS = $(SOURCE:.c=.o)

INCDIR=../../../include
HFTA_DIR=$(INCDIR/hfta)
LFTA_DIR=$(INCDIR/lfta)

all: libgscplftaaux.a
	cp libgscplftaaux.a ../../../lib/ ; ranlib ../../../lib/libgscplftaaux.a

libgscplftaaux.a: $(OBJECTS) Makefile 
	ar  rc libgscplftaaux.a $(OBJECTS) 

rts_string.c : $(LFTA_DIR/rts_external.h) $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h)

rts_byteswap.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h)

rts_sample.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) $(LFTA_DIR/rts_external.h)

rts_udaf.c : $(LFTA_DIR/rts_udaf.h) $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) 

lpm.c : $(LFTA_DIR/rts_external.h) $(INCDIR/fta.h) $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h)

flip_udaf.c : $(LFTA_DIR/rts_udaf.h) $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h)

install: all

clean:
	rm -f *.o *.a core

