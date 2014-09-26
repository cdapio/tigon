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
#ifndef CLEARINGHOUSEREGISTRIES_H
#define CLEARINGHOUSEREGISTRIES_H

#include "fta.h"
#include "systat.h"
#include "fta_stat.h"

/* The table is used to maintain the location information
 *  of FTAs within the clearinghouse it is not used outherwise
 * Behaviour is undefined if the same name
 * is registered more then once.
 */

/* These functions are used for both FTAs and instances of FTAs
 * if ftaid.streamid == 0 then the function is called for a FTA
 * if not it is called for an FTA instance.
 * if an FTA is registered reusable must be set to 1
 * if an FTA instance is registered reusable is either set to
 *   0 or 1 depending if the fta can be reused
 * if reuse is set to 1 an existing instance is returned
 * if reuse is set to 0 the FTA and not an instance is returned
 */

/* Adds a FTA to the lookup table if FTAID schema */
gs_retval_t ftalookup_register_fta(FTAID subscriber,
                                   FTAID ftaid,
                                   FTAname name,
                                   gs_uint32_t reusable,
                                   gs_csp_t schema);

/* Removes the FTA from the lookup table */
gs_retval_t ftalookup_unregister_fta(FTAID subscriber,
                                     FTAID ftaid);

/* Looks an FTA up by name */
gs_retval_t ftalookup_lookup_fta_index(FTAID caller,
                                       FTAname name,
                                       gs_uint32_t reuse,
                                       FTAID * ftaid,
                                       gs_csp_t * schema);

/* Gets called when a consumer stoped receiving data */

gs_retval_t ftalookup_producer_failure(FTAID caller,FTAID producer);

/* Gets called when a FTA sends a heart beat */

gs_retval_t ftalookup_heartbeat(FTAID caller, gs_uint64_t trace_id,
                                gs_uint32_t sz, fta_stat * trace );
#endif
