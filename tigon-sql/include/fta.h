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
 * fta.h: fta definition for applications
 */
#ifndef FTA_H
#define FTA_H

#include "gslog.h"
#include "gsconfig.h"

typedef struct FTAID {
    gs_uint32_t ip;  /* identifies the node */
    gs_uint32_t port; /* identifies a process */
    gs_uint32_t index;  /* the index identifies a particular FTA in the process */
    gs_p_t streamid; /* streamid if set identifies an instance */
} FTAID;



typedef struct PRINTFUNC {
    gs_sp_t header;
    gs_sp_t path;
    gs_sp_t basename;
    FILE * fa[MAXPRINTFILES];
    gs_uint32_t nexttime;
    gs_uint32_t split;
    gs_uint32_t base;
    gs_uint32_t itt;
    gs_uint32_t delta;
    gs_schemahandle_t schemaid;
    gs_int32_t split_field;
    gs_int32_t temporal_field;
    gs_int32_t in_use;
} PRINTFUNC;

/* the following defines define the FTA commands */

#define FTA_COMMAND_LOAD_PARAMS 1
#define FTA_COMMAND_FLUSH 2
#define FTA_COMMAND_GET_TEMP_STATUS 3
#define FTA_COMMAND_SET_SAMPLING_RATE 4
#define FTA_COMMAND_FILE_DONE 5

typedef gs_csp_t FTAname;

typedef gs_csp_t DEVname;

#define DEFAULTDEV 0

//	Use gs_param_handle_t defined in gscpv3/include/gstypes.h instead
// typedef gs_uint32_t param_handle_t;

struct FTA {
    
    /* reference counts managed by the RTS */
    gs_uint32_t refcnt;
    gs_uint32_t runrefcnt;
    
    /* the ID gets filled in by the runtime system */
    FTAID ftaid;
    
    /* indicates if the FTA instance can be reused. Set by FTA 0 indicates
     * that it can not be reused.
     */
    gs_uint32_t reusable;
    
    /* ftaids the HFTA is subscribed to*/
    FTAID stream_subscribed[64];
    
    /* number of valid entries in stream_subscribe [0..stream_subscribed_cnt-1] */
    gs_uint32_t stream_subscribed_cnt;
    
    /* State needed to print directly to a file out of a HFTA */
    
    PRINTFUNC printfunc;
    
    /* value of prefilter only used in lftas */
    gs_uint64_t prefilter;
    
    /* allocates and initializes a paramterized instance of a FTA
     */
    struct FTA * (*alloc_fta) (FTAID ftaid, gs_uint32_t reusable,
                               gs_int32_t command, gs_int32_t sz, void * value );
    
    /* release all resources associated with FTA (including the FTA) */
    gs_retval_t (*free_fta) (struct FTA *, gs_uint32_t recursive);
    
    /* send control data from host to an instance of an FTA, may return status */
    gs_retval_t (*control_fta) (struct FTA *, gs_int32_t command, gs_int32_t sz, void * value);
    
    /* gets called once every second. Might be delayed until the current thread execution
     * returns into the run time system. If delayed for more then 1 second then only one
     * call will be made at the next oportunity.
     */
    gs_retval_t (*clock_fta) (struct FTA *);
    
    /*
     * process a packet received on the interface or a tuple received from
     * another FTA. If ftaid is 0 then packet is of
     * type struct packet otherwise it is a tuple as defined by the
     * ftaid.
     */
    gs_retval_t (*accept_packet)(struct FTA *, FTAID * f, void * packet, gs_int32_t sz);
};

/* The following definitions are used or provided in automatically generated files */

typedef  struct FTA * (*alloc_fta) (FTAID ftaid, gs_uint32_t reusable,
gs_int32_t command, gs_int32_t sz, void * value ) ;


/* FTA INTERFACE */
/* ============== */
/* These functions are used by FTAs in addition to the LOW and HIGH
 * level application interface functions.
 */

/* start_service is called to process requests from the message queue
 which would invoke callback functions registered by fta_register.
 Number indicates how many seconds should be waited before the
 function returns. If number < 0 then the function is an endless
 loop (should be used for HFTA) if the number == 0 all currently
 outstanding requests are processed before the function returns.
 */
gs_retval_t fta_start_service(gs_int32_t number);

/* the dev name indicates on which device the FTA is supposed to execute.
 * HFTA should always use DEFAULTDEV and a snaplen of -1
 */
FTAID fta_register(FTAname name, gs_uint32_t reusable, DEVname device,
                   alloc_fta fta_alloc_functionptr, gs_csp_t schema,gs_int32_t snaplen,
                   gs_uint64_t prefilter);

gs_retval_t fta_unregister(FTAID f);


/*
 * get the maximum snap length of all ftas registered within the current proccess
 */

gs_retval_t fta_max_snaplen();

/*
 * Post a tupple back to the clearing house This tuple cannot
 * use the lfta tuple allocation functions. NOTE: post tuple will
 * return -1 if at least one ringbuffer for that FTA which is in the atomic
 * set has not enought space. Otherwise 0 is returned.
 */
gs_retval_t hfta_post_tuple(struct FTA * self, gs_int32_t sz, void *tuple);

/* the following function fills in the FTAID array r with the FTAIDs of processes receiving
 * tuplese from the FTA specified in f. The space array indicates how many tuples of tuplesz
 * can still fit in the ringbuffer corresponding to the FTAID of the process with the same index
 * in r. The return values indicates how many ring buffers are valid in r. szr indicates the memory
 * length (in elements) of  r and space. The function will fill at most szr elements into these array.
 * If the return value is larger then szr it is an indication that not enough memory in r and space
 * were provided to hold all results. A return value of -1 indicates an error.
 */

gs_retval_t hfta_get_ringbuf_space(struct FTA * f, FTAID * r, gs_int32_t* space, gs_int32_t szr, gs_int32_t tuplesz);


/* the following function can be used to put a ringbuffer to a process identified by the FTAID
 * process into one of the following 3 modes. ATOMIC makes the ringbuffer part of the atomic set.
 * A post to the atomic set either succeed or fails entirely. BESTEFFORT is the besteffort set
 * posts to BESTEFFORT ringbuffers might fail quitely. SUSPEND will prevent any tuples of being
 * posted to that proccess from the specified FTA.
 */

#define HFTA_RINGBUF_ATOMIC 1
#define HFTA_RINGBUF_BESTEFFORT 2
#define HFTA_RINGBUF_SUSPEND 3

gs_retval_t hfta_set_ringbuf_type(struct FTA * f, FTAID process, gs_int32_t state);

#endif
