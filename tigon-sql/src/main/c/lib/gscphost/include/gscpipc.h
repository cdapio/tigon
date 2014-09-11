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
/*
 * gscpipc.h:: defines the interface of the IPC channels used in
 * the gigascope framework to communicate tuples between different
 * processes
 */

#ifndef GSCPIPC_H
#define GSCPIPC_H

#include "gsconfig.h"
#include "gstypes.h"
#include "fta.h"

#define RESERVED_FOR_LOW_LEVEL 0

#define FTACALLBACK 1

extern gs_uint64_t intupledrop;
extern gs_uint64_t outtupledrop;
extern gs_uint64_t intuple;
extern gs_uint64_t outtuple;
extern gs_uint64_t inbytes;
extern gs_uint64_t outbytes;
extern gs_uint64_t cycles;


/* shared ringbuffer data structure used */

#if defined(__sparc__) && defined(__sun__)
#define ALIGN64
#endif

struct tuple {
    FTAID f;
    gs_int32_t  sz;
    gs_uint32_t  next;
#ifdef ALIGN64
    gs_uint32_t  alignment;
#endif
    gs_int8_t  data[1];
}__attribute__ ((__packed__));

struct ringbuf {
    gs_uint32_t   mqhint;
    gs_uint32_t   writer;
    gs_uint32_t   reader;
    gs_uint32_t   end;
    gs_int32_t  length;
#ifdef ALIGN64
    gs_uint32_t  alignment;
#endif
    FTAID  srcid;
    FTAID  destid;
    gs_int8_t  start[1];
}__attribute__ ((__packed__));

/* adds a buffer to the end of the sidequeue*/
gs_retval_t sidequeue_append(FTAID ftaid, gs_sp_t buf, gs_int32_t length);

/* removes a buffer from the top of the sidequeue*/
gs_retval_t sidequeue_pop(FTAID * ftaid, gs_sp_t buf, gs_int32_t * length);

/*
 *used to contact the clearinghouse process returns the MSGID of
 * the current process negative result indicates a problem
 */

gs_retval_t  gscpipc_init(gs_int32_t  clearinghouse);

/* used to disassociate process from clearinghouse */
gs_retval_t  gscpipc_free();

/* sends a message to a process */
gs_retval_t  gscpipc_send(FTAID f, gs_int32_t  operation, gs_sp_t buf, gs_int32_t  length, gs_int32_t  block);

/* retrieve a message buf has to be at least of size MAXMSGSZ returns 0
 if no message is available -1 on an error and 1 on sucess*/
gs_retval_t  gscpipc_read(FTAID * f, gs_int32_t  * operation, gs_sp_t buf, gs_int32_t  * lenght, gs_int32_t  block);

/* allocate a ringbuffer which allows receiving data from
 * the other process returns 0 if didn't succeed.
 * returns an existing buffer if it exists  and increments the refcnt*/

struct ringbuf * gscpipc_createshm(FTAID f, gs_int32_t  length);

/* finds a ringbuffer to send which was allocated by
 * gscpipc_creatshm and return 0 on an error */

struct ringbuf * gscpipc_getshm(FTAID f);

/* frees shared memory to a particular proccess identified
 * by ftaid if reference counter reaches 0
 */
gs_retval_t  gscpipc_freeshm(FTAID f);


/* returns true if on any sending ringbuffer the mqhint bit is true
 * can be used in lfta rts to indicate that the message queue should
 * be checked.
 */

gs_retval_t  gscpipc_mqhint();

/* Access macros for ringbuffer */

#ifdef ALIGN64
#define UP64(x) ((((x)+7)/8)*8)
#else
#define UP64(x) x
#endif

#define CURWRITE(buf)  ((struct tuple *)(&((buf)->start[(buf)->writer])))
#define ADVANCEWRITE(buf)  CURWRITE(buf)->next=\
((buf)->end > ((buf)->writer+UP64(CURWRITE(buf)->sz))+sizeof(struct tuple)-1)\
? ((buf)->writer+UP64(CURWRITE(buf)->sz)+sizeof(struct tuple)-1) : 0; \
(buf)->writer=CURWRITE(buf)->next;
#define CURREAD(buf)  ((struct tuple *)(&((buf)->start[(buf)->reader])))
#define ADVANCEREAD(buf) (buf)->reader=\
CURREAD(buf)->next
#define UNREAD(buf) ((buf)->reader != (buf)->writer)
#define SPACETOWRITE(buf)   ((((buf)->reader <= (buf)->writer) \
&& ((buf)->reader+(buf)->end-(buf)->writer) > MAXTUPLESZ)\
|| (((buf)->reader > (buf)->writer) && (((buf)->reader-(buf)->writer)>MAXTUPLESZ)))
#define HOWFULL(buf) (((((buf)->writer)-(buf)->reader)%((buf)->end)*1000)/((buf)->end))
/* conservative estimate of how many tuples of size tuplesz fit in a ringbuffer */
#define TUPLEFIT(buf,tuplesz) ((((((buf)->writer)-(buf)->reader)%((buf)->end))-MAXTUPLESZ)/ \
((UP64(tuplesz))+sizeof(struct tuple)-1))
#endif
