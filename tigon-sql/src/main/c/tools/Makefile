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

CC= gcc -O4 -g   -I ../../../../include -D_FILE_OFFSET_BITS=64 

INCDIR=../../../../include

# we need to suppress writable strings warnings on Mac for flex/bison generated C++ code
OS= $(shell uname -s)
ifeq ($(OS),Darwin)
	WARN=-Wno-deprecated-writable-strings
endif
ifeq ($(OS),Linux)
	WARN=-Wno-write-strings
endif

all:   gdatcat gsprintconsole gsgdatprint  gdat2ascii gsexit gssinksim process_logs

install: all
	cp gsprintconsole  gsgdatprint  gdat2ascii  process_logs gsexit ../../../../bin/ 

gsgdatprint: gsgdatprint.o ../../../../lib/libgscphostaux.a ../../../../lib/libgscphost.a ../../../../lib/libgscpinterface.a ../../../../lib/libgscpapp.a
	g++ -g -o gsgdatprint gsgdatprint.o -L../../../../lib  -lgscpapp  -lgscphostaux -lgscphost -lgscpinterface -lgscplftaaux -lclearinghouse -lgscpaux 

gsgdatprint.o: gsgdatprint.c

gsexit: gsexit.o ../../../../lib/libgscphostaux.a ../../../../lib/libgscphost.a ../../../../lib/libgscpinterface.a ../../../../lib/libgscpapp.a
	g++ -g -o gsexit gsexit.o -L../../../../lib  -lgscpapp  -lgscphostaux -lgscphost -lgscpinterface -lgscplftaaux -lclearinghouse -lgscpaux 

gsexit.o: gsexit.c


gsgdatprint.c: $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h)

gdat2ascii: gdat2ascii.o ../../../../lib/libgscphostaux.a  ../../../../lib/libgscpinterface.a 
	g++ -g -o gdat2ascii gdat2ascii.o -L../../../../lib   -lgscphostaux  -lgscpinterface -lgscpaux 

gdat2ascii.o: gdat2ascii.c

gdat2ascii.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h)

gsprintconsole: gsprintconsole.o ../../../../lib/libgscphostaux.a ../../../../lib/libgscphost.a ../../../../lib/libgscpinterface.a ../../../../lib/libgscpapp.a
	g++ -g -o gsprintconsole gsprintconsole.o -L../../../../lib -lgscpapp  -lgscphostaux -lgscphost -lgscpinterface -lgscplftaaux -lclearinghouse -lgscpaux  

gsprintconsole.o: gsprintconsole.c

gsprintconsole.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h)

gssinksim: gssinksim.o ../../../../lib/libgscphostaux.a ../../../../lib/libgscphost.a ../../../../lib/libgscpinterface.a ../../../../lib/libgscpapp.a
	g++ -g -o gssinksim gssinksim.o -L../../../../lib -lgscpapp  -lgscphostaux -lgscphost -lgscpinterface -lgscplftaaux -lclearinghouse -lgscpaux 

gssinksim.o: gssinksim.c

gssource: gssource.o
	g++ -g -o gssource gssource.o -L../../../../lib   -lgscpaux 

gssource.o: gssource.c

gdatcat: gdatcat.o
	$(CC) -o gdatcat gdatcat.o

process_logs: process_logs.o xml.tab.o xmllexer.o xml_t.o
	g++ -g -o process_logs process_logs.o xml.tab.o xmllexer.o xml_t.o

xml.tab.o: xml.tab.cc
	g++ $(WARN) -g -o xml.tab.o -c xml.tab.cc

xmllexer.o: xmllexer.cc
	g++ $(WARN) -g -o xmllexer.o -c xmllexer.cc

xml_t.o: xml_t.cc
	g++ -g -o xml_t.o -c xml_t.cc

process_logs.o: process_logs.cc
	g++ -g -o process_logs.o -c process_logs.cc

gdatcat.o: gdatcat.c

gdatcat.c : $(INCDIR/gsconfig.h) $(INCDIR/gstypes.h)

clean:
	rm -ff gsprintconsole  gsgdatprint  gdat2ascii gdat2hex process_logs gsexit *.o

