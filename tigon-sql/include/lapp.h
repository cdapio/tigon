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
 * lapp.h: low level interface for applications
 */
#ifndef LAPP_H
#define LAPP_H

#include <fta.h>
#include <systat.h>
#include <fta_stat.h>

#define REGULAR_TUPLE 0
#define TEMPORAL_TUPLE 1
#define EOF_TUPLE 2

/* LOW LEVEL APPLICATION INTERFACE */
/* =============================== */

/* NOTE: A streamid is only unique in the context of a particular
 * producer. Therefore, all the fields in the FTAID need to be compared
 * to decided if to FTA instances are the same
 */

#define CLEARINGHOUSE 1
#define APP 2
#define HFTA 3
#define LFTA 4

/* deviceid should be set to DEFAULTDEV mapcnt and map to 0 for HFTAs and apps */

gs_retval_t hostlib_init(gs_int32_t type, gs_int32_t recvbuffersz, gs_int32_t deviceid,  gs_int32_t mapcnt, gs_sp_t map[]);

void hostlib_free();

/* find an FTA message queue ID, index and schema based on an FTA key
 * if reuse is set we return an already running instance if possible*/

gs_retval_t fta_find(FTAname name, gs_uint32_t  reuse,  FTAID * ftaid,
                     gs_sp_t schema , gs_int32_t buffersz );

/* the following is used to send a heart beat message to the clearinghouse. Self is the FTAID of the sending
 * fta instance*/

gs_retval_t fta_heartbeat(FTAID self, gs_uint64_t trace_id,
                          gs_uint32_t  sz, fta_stat * trace);

/*
 * FTA's cannot be dynamically installed (since we currently don't
 * have a run-time linker in the card).  however, new parameterized
 * instances of previously installed FTAs can be created.  The FTA
 * template IDs have to defined in an include file.  the following
 * call will result in a call to the FTA's alloc_fta function.
 * if sucessfull the streamid is set in the ftaid.
 */

gs_retval_t fta_alloc_instance(FTAID subscriber,
                               FTAID * ftaid, FTAname name, gs_sp_t schema,
                               gs_uint32_t  reusable,
                               gs_int32_t command, gs_int32_t sz, void *  data);

/* see app.h for description of the additional parameter */
gs_retval_t fta_alloc_print_instance(FTAID subscriber,
                                     FTAID * ftaid,
                                     FTAname name, gs_sp_t schema,gs_uint32_t  reusable,
                                     gs_int32_t command, gs_int32_t sz, void *  data,
                                     gs_sp_t path,gs_sp_t basename,
                                     gs_sp_t temporal_field, gs_sp_t split_field,
                                     gs_uint32_t  delta, gs_uint32_t  split);


/*
 * the next two functions result in callouts to the corresponding FTA
 * functions (fta_free, control_fta).
 */

gs_retval_t fta_free_instance(FTAID subscriber,FTAID ftaid, gs_uint32_t  recursive);


gs_retval_t fta_control(FTAID subscriber,
                        FTAID ftaid, gs_int32_t command, gs_int32_t sz, void * value);


/*
 * gscp_get_buffer: get a tuple from a read shared buffer
 * established with any other porcess  when an FTA
 * was established. Returns -1 on error, 0 on success and 1 on timeout and 2 for a temporal tuple.
 * On a timeout the streamid is set to 1 and the length to 0.
 * The timeout is in seconds. A timeout of 0 makes the call blocking
 * a timeout of -1 returns imidiatly if no buffer is availabe.
 */
gs_retval_t gscp_get_buffer(FTAID * ftaid, gs_int32_t * size, void *tbuffer,
                            gs_int32_t tbuf_size, gs_int32_t timeout);

/* the following function send a control callback to every
 active FTA in a particular process identified by the
 IP and port part of the ftaid. The result is < 0 if not
 successful if the control operation is returning a result it is ignored.
 The main purpose of this message is to support a
 flush of all FTAs in a proccess*/

gs_retval_t process_control(FTAID ftaid, gs_int32_t command,
                            gs_int32_t sz, void * value);


/* called by an FTA if one of its producers fails. Self is the FTAID of the instance
 * sending the notification */

gs_retval_t fta_notify_producer_failure(FTAID self, FTAID producer);

/* returns FTAID of current process */
FTAID  gscpipc_getftaid();

extern gs_uint64_t shared_memory_full_warning;

/* returns 1 if GSCP ringbuffers are in blocking mode */
gs_retval_t gscp_blocking_mode();

#endif
