CC=gcc -O3  -fexpensive-optimizations  -I ../../../include

SOURCE = appinterface.c

OBJECTS = $(SOURCE:.c=.o)

all: libgscpapp.a
	cp libgscpapp.a ../../../lib; ranlib ../../../lib/libgscpapp.a

libgscpapp.a: $(OBJECTS) Makefile 
	ar  rc libgscpapp.a $(OBJECTS) 

INCDIR=../../../include

appinterface.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) $(INCDIR/app.h) $(INCDIR/fta.h) $(INCDIR/lapp.h) 

install: all

clean:
	rm -f *.o *.a core

