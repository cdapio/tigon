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

CC=gcc  -g -O3  -fexpensive-optimizations  -I /usr/include/pcap -I libdag/include -I ../../../../../include/lfta/ -I ../../../../../include/ -I ./include/ -I ../gscphost/include

SOURCE =   rts_main.c rts_env.c rts_csv.c rts_gdat.c

OBJECTS = $(SOURCE:.c=.o)


all: libgscprts.a 

INCDIR=../../../../../include
LFTA_DIR=$(INCDIR/lfta)

libgscprts.a: $(OBJECTS) Makefile 
	ar  rc libgscprts.a $(OBJECTS) 

rts_main.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) $(INCDIR/lapp.h) $(INCDIR/fta.h) 

rts_env.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) $(INCDIR/lapp.h) $(INCDIR/fta.h) $(LFTA_DIR/rts.h)

rts_csv.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) $(INCDIR/lapp.h) $(INCDIR/fta.h) $(LFTA_DIR/rts.h) $(INCDIR/packet.h)

rts_gdat..c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h) $(INCDIR/lapp.h) $(INCDIR/fta.h) $(LFTA_DIR/rts.h) $(INCDIR/packet.h)

install: all
	cp libgscprts.a  ../../../../../lib/ ; ranlib ../../../../../lib/libgscprts.a

clean:
	rm -f *.o *.a core
