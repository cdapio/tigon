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
#ifndef LAPPREGISTRIES_H
#define LAPPREGISTRIES_H
#include "gsconfig.h"
#include "gstypes.h"
#include <fta.h>
#include <gscpipc.h>

/* adds the remote streamid with its associated msgid and ringbuf
 */
gs_retval_t streamregistry_add(FTAID f,struct ringbuf * r);

/* removes streamid from registry for specific msgid */
void streamregistry_remove(FTAID f);

/* the following two functions are used to cycle
 through all ringbuffers
 */
gs_retval_t streamregistry_getactiveringbuf_reset();
struct ringbuf * streamregistry_getactiveringbuf();

/* the following two functions are used to cycle
 through all msgids
 */
gs_retval_t streamregistry_getactiveftaid_reset();
FTAID * streamregistry_getactiveftaid();

#endif
