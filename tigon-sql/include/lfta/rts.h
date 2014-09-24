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
 * rts.h: ethernet card run-time system dfns
 */
#ifndef RTS_H
#define RTS_H
#include "gstypes.h"
#include "gsconfig.h"
#include "byteswap.h"
#include "rts_external.h"

struct string;	/* forward decl. */

#ifndef NULL
#define NULL 0
#endif

#include "fta.h"


/*
 * dynamic memory allocation for FTAs (so they can store state, etc.).
 * note that the allocator keeps track of which FTA owns what blocks and
 * can free all allocated blocks for a given FTA in one go.
 */

void *fta_alloc(struct FTA *, gs_int32_t size);
void fta_free(struct FTA *, void *mem);
void fta_free_all(struct FTA *);

/* memory allocation for scratchpad */

#define sp_fta_alloc fta_alloc
#define sp_fta_free fta_free
#define sp_fta_free_all fta_free_all


/* functions used to parse packet  (list is not complete)*/
#include "schema_prototypes.h"

/*
 * FTA output: FTAs output tuples to the host using the post_tuple RTS function.
 * Before the tuple can be output a tuple memory block has to be allocated
 * in the tuple memory space. The allocate_tuple function is used for this.
 * If an FTA is removed before post_tuple is called all allocated memory
 * in the FTA's tuple space is reclaimed. If the FTA is removed after
 * post_tuple is called the tuple will be deliverd to the host.
 * If 0 is returned at no none suspended ringbuffer has not enough memory to
 * allocate the tuple
 */

void * allocate_tuple(struct FTA *,  gs_int32_t size);

/* post tuple posts the tuple if the atomic reingbuffer set can not suceed -1 is returned and the tuple is not
 posted otherwise 0 is returned*/

gs_retval_t post_tuple(void *tuple);


/* the following function fills in the FTAID array r with the FTAIDs of processes receiving
 * tuplese from the FTA specified in f. The space array indicates how many tuples of tuplesz
 * can still fit in the ringbuffer corresponding to the FTAID of the process with the same index
 * in r. The return values indicates how many ring buffers are valid in r. szr indicates the memory
 * length (in elements) of  r and space. The function will fill at most szr elements into these array.
 * If the return value is larger then szr it is an indication that not enough memory in r and space
 * were provided to hold all results. A return value of -1 indicates an error.
 */

gs_retval_t get_ringbuf_space(struct FTA * f, FTAID * r, gs_int32_t * space,  gs_int32_t szr, gs_int32_t tuplesz);


/* the following function can be used to put a ringbuffer to a process identified by the FTAID
 * process into one of the following 3 modes. ATOMIC makes the ringbuffer part of the atomic set.
 * A post to the atomic set either succeed or fails entirely. BESTEFORT is the besteffort set
 * posts to BESTEFORT ringbuffers might fail quitely. SUSPEND will prevent any tuples of being
 * posted to that proccess from the specified FTA. NOTE: Calling set_ringbuf_space between
 * allocate_tuple and post_tuple might have unpredictable results for the tuple already
 * allocated.
 */

#define LFTA_RINGBUF_ATOMIC 1
#define LFTA_RINGBUF_BESTEFFORT 2
#define LFTA_RINGBUF_SUSPEND 3

gs_retval_t set_ringbuf_type(struct FTA * f, FTAID process, gs_int32_t state);

/* The following function returns TRUE if there is enough room in the post tuple
 * queue for a speedy posting of the tuple. It should be used by FTAs which could
 * possibly delay the posting of a tuple if FALSE is returned.
 */

gs_retval_t post_ready(void);

/*
 * the following function gives hints to the heap for binning
 */

void fta_add_alloc_bin_size(struct FTA *, gs_int32_t sz, gs_int32_t maxcnt);
void fta_remove_alloc_bin_size(struct FTA *, gs_int32_t sz, gs_int32_t maxcnt);


gs_retval_t print_error(char *string);	/* XXXCDC: right place? */

/* Function to get the attributes of interfaces in an lfta */

gs_sp_t get_iface_properties (const gs_sp_t iface_name, const gs_sp_t property_name);


#endif
