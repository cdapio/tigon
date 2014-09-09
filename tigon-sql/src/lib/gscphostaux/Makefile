
CC=gcc -O3  -fexpensive-optimizations -I ../../../include

SOURCE =  byteswap.c

OBJECTS = $(SOURCE:.c=.o)

all: install

install: libgscphostaux.a
	cp libgscphostaux.a ../../../lib ; ranlib ../../../lib/libgscphostaux.a

INCDIR=../../../include

byteswap.c : $(INCDIR/gstypes.h) $(INCDIR/gsconfig.h)

libgscphostaux.a: $(OBJECTS) Makefile 
	ar  rc libgscphostaux.a $(OBJECTS) 

clean:
	rm -f *.o *.a core


$(OBJECTS): 
