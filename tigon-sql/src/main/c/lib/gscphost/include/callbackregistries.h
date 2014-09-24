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
#ifndef CALLBACKREGISTRIES_H
#define CALLBACKREGISTRIES_H
#include "gsconfig.h"
#include "gstypes.h"

#include "fta.h"
#include "gscpipc.h"


/* This registry is used to keep track of FTAswithin the current process
 * they are registerd when they are initiated and unregisterd if
 * they are freed
 */

/* registers an alloc function of an FTA and returns a unique index */
gs_retval_t ftacallback_add_alloc(FTAname name, alloc_fta fta_alloc_functionptr, gs_uint64_t prefilter);

/* unregisters an alloc function of an FTA and makes the index available
 * for reuse
 */

gs_retval_t ftacallback_rm_alloc(gs_uint32_t index);

/* returns the function pointer of the callback function for a given
 * index
 */

alloc_fta ftacallback_get_alloc(gs_int32_t index);

/* returns the prefilter for a given
 * index
 */

gs_uint64_t ftacallback_get_prefilter(gs_int32_t index);

/* The second registry is used to keep track of to which process a particular
 stream ID should be posted too.
 */

/* associate ringbuffer with streamid (using refcounting) */
gs_retval_t ftacallback_add_streamid(struct ringbuf * r, gs_uint32_t streamid);

/* unassosciate a ringbuffer from a streamid */

gs_retval_t ftacallback_rm_streamid(struct ringbuf * r, gs_uint32_t streamid);

/* starts an itteration through all ringbuffers for a particular streamid */

gs_retval_t ftacallback_start_streamid(gs_int32_t streamid);

/* returns all the ringbuffer associated with the streamid passed in
 * ftacallback_start_streamid
 */
struct ringbuf * ftacallback_next_streamid(gs_int32_t * state);

/* set the state for a given streamid and destination process */

gs_retval_t ftacallback_state_streamid(gs_int32_t streamid,FTAID process, gs_int32_t state);

/* The third registry is used to keep track of which process is blocked
 on which ringbuf
 */

/* associate msgid with ringbuf  */
gs_retval_t ftacallback_add_wakeup(FTAID ftaid, struct ringbuf * r);

/* starts an itteration through all msgids associated with
 a streamid. This also uses data kept in the second
 registry
 */
gs_retval_t ftacallback_start_wakeup(gs_uint32_t streamid);

/* returns all the msgid blocked on the streamid passed in
 * ftacallback_start_streamid and removes the msgid from
 * the wakeup list
 */
FTAID * ftacallback_next_wakeup();


/*
 * the following two functions insert and remove an  instance
 * of an FTA from the active list.
 */

gs_retval_t ftaexec_insert(struct FTA * after, struct FTA * new);
gs_retval_t ftaexec_remove(struct FTA * id);

/*
 * FTA's cannot be dynamically installed (since we currently don't
 * have a run-time linker in the card).  however, new parameterized
 * instances of previously installed FTAs can be created.  The FTA
 * template IDs have to defined in an include file.  the following
 * call will result in a call to the FTA's alloc_fta function.
 */

struct FTA * ftaexec_alloc_instance(gs_uint32_t index, struct FTA * reuse,
                                    gs_uint32_t reusable,
                                    gs_int32_t command, gs_int32_t sz, void *  data);


/*
 * the next two functions result in callouts to the corresponding FTA
 * functions (fta_free, control_fta).
 */

gs_retval_t ftaexec_free_instance(struct FTA * FTA_id, gs_uint32_t recursive);

gs_retval_t ftaexec_control(struct FTA * FTA_id, gs_int32_t command, gs_int32_t sz, void * value);

gs_retval_t ftaexec_process_control(gs_int32_t command, gs_int32_t sz, void * value);

/* Start itteration through list of active FTA */

gs_retval_t ftaexec_start();

/* get one FTA at a time */

struct FTA * ftaexec_next();


/* HFTA internal print function*/

gs_retval_t add_printfunction_to_stream(struct FTA * ftaid, gs_sp_t schema, gs_sp_t path, gs_sp_t basename,
                                        gs_sp_t temporal_field, gs_sp_t split_field, gs_uint32_t delta, gs_uint32_t split);

gs_retval_t print_stream(struct FTA * self, gs_int32_t sz, void *tuple);


#endif
