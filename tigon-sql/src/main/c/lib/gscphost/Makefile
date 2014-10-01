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

CC=gcc   -O3  -fexpensive-optimizations  -g -I ./include/ -I ../../../../../include/ -DCLEARINGHOUSE_HEARTBEAT

SOURCE = callbackinterface.c callbackregistries.c lappinterface.c lappregistries.c  lfta.c gscpipc.c 

OBJECTS = $(SOURCE:.c=.o)

all: install

INCDIR=../../../../include

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
	cp libgscphost.a libclearinghouse.a ../../../../../lib/ ; ranlib   ../../../../../lib/libclearinghouse.a ; ranlib  ../../../../../lib/libgscphost.a

clean:
	rm -f *.o *.a core
