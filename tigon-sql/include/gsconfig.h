/* ------------------------------------------------
 Copyright 2014 AT&T Intellectual Property
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 ------------------------------------------- */
#ifndef GSCONFIG
#define GSCONFIG

// Contains all defines etc... . As we don't have a proper configure script yet at least that way all the defines are in one place.

// ==================================================
// General defines
// ==================================================


// if set we use 64 BIT GS
#define GS64

// HOST NAME LENGTH FOR LOGGING
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX 255
#endif

// STATISTICS LOGGING INTERVAL IN SECONDS
#define GSLOGINTERVAL 10

// Start size of dynamiclly growing internal arrays
#define STARTSZ 1024

// Use blocking ringbuffer
#define BLOCKRINGBUFFER

//#define LOCALIP // restricts sockets to localhost IP


// maximume number of print streams
#define MAXPRINTFILES 16

// allows sampling on the schema level
//#define LOWSAMPLE 0

// if defined outputs logs larger then that level to stderr
#define LOGSTDERR LOG_WARNING


// ==================================================
// Callback  stuff
// ==================================================

// If POLLING is defined HFTA's poll the ring buffers every 100msec rather then
// block on a message receive
#define POLLING


// ==================================================
// LAP  stuff
// ==================================================

// ==================================================
// Tool  stuff
// ==================================================

// BLOCK size for gdatcat
#define CATBLOCKSZ 200*1024*1024

// Enables bzip2 compression rather then gzip compression in gsgdatprint
//#define GSBZIP2

// Uses the gzip library rather then forking a gzip process in gsgdaprint
//#define ZLIB


// ==================================================
// IPC  stuff
// ==================================================

#define MAXMSGSZ 10240
#define MAXTUPLESZ 10240
#define MAXRES MAXMSGSZ

#define MAXSZ (((MAXTUPLESZ+128)>MAXMSGSZ?MAXTUPLESZ+128:MAXMSGSZ))

#define SOCK_BUF_SZ 160*(MAXMSGSZ+4)
#define SOCKET_HASH_SZ 512
#define MAX_NUMBER_OF_SHM 64

//#define PRINTMSG // prints low level IPC messages for debugging VERY VERBOSE

/* maximum size of an FTA name */
#define MAXFTANAME 256
#define MAXPRINTSTRING 256

/* maximum size of a schema definition */
#define MAXSCHEMASZ (8*1024)


// ==================================================
// DAG related defines
// ==================================================

#define DAGMAXPACKETSZ 2*4096
#define DAGTIMEOUT 50 /* timeout in msec  */

/* GPP record type defines  has to match dagapi.h*/
#ifndef TYPE_LEGACY
#define TYPE_LEGACY       0
#define TYPE_HDLC_POS     1
#define TYPE_ETH          2
#define TYPE_ATM          3
#define TYPE_AAL5     4
#endif

/* GSCP special types */
#define TYPE_PPP_POS 5
#define TYPE_ETH_PPP 6
#define TYPE_CRS1_OC768_POS 7
#define TYPE_ETH_VPLS_GRE 8
#define TYPE_ETH_GTP 9

//#define DAGDEBUG

//#define LLDUPREM

//#define GTPONLY // then only the GTP parsing code gets compiled which saves a couple of milion cycles at high speeds.

//#define DOUBLEHASH

#define VMETR // detailed RTS stats

#define DAGPOSCONFIGSTRING ""
#define DAGETHCONFIGSTRING ""
#define DAGATMAAL5CONFIGSTRING ""

// ==================================================
// PCAP  stuff
// ==================================================


#define PCAPTIMEOUT 50 /* timeout in msec for calls to pcap_process_packets */
#define PCAPMAXPACKETSZ 1560
#define PCAPBATCHSZ 1000
#define PCAPMINBATCHSZ 30

// ==================================================
// REPLAY  stuff
// ==================================================

#define PCAPINITDELAY 120 /* time we wait before processing packets */
#define PCAPREPLAYBATCHSZ 1
//#define REPLAYNOW // if defined timestamps in pcap file are ignored

// ==================================================
// system stuff
// ==================================================

#ifdef sun // make sure the code which uses variables with the name sun
// still works on a sun ....
#undef sun
#endif

#ifndef AF_LOCAL
#ifdef AF_UNIX
#define AF_LOCAL AF_UNIX
#endif
#endif

#ifndef SUN_LEN
#define SUN_LEN(su) \
(sizeof(*(su)) - sizeof((su)->sun_path) + strlen((su)->sun_path))
#endif

#ifdef __linux__
#include <sys/ioctl.h>
#endif
#ifndef IPC_RALL
#define IPC_RALL           000444 /* read permission */
#endif
#ifndef IPC_WALL
#define IPC_WALL           000222  /* write/alter permission */
#endif
#ifndef IPC_R
#define IPC_R           000400  /* read permission */
#endif
#ifndef IPC_W
#define IPC_W           000200  /* write/alter permission */
#endif
#ifndef IPC_CREAT
#define IPC_CREAT       001000  /* create entry if key does not exist */
#endif

#ifndef ENOMSG
#define ENOMSG EAGAIN  /* for MacOSX */
#endif

#endif

