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
 * app.h: interface for applications
 */
#ifndef APP_H
#define APP_H


#include "fta.h"
#include "lapp.h"

/* HIGH LEVEL APPLICATION INTERFACE */
/* ================================ */




/* will call hostlib_init using the specified ringbuffer size
 */
gs_retval_t ftaapp_init(gs_uint32_t bufsz);

/* this should be used before exit() to make sure everything gets
 cleaned up
 */
gs_retval_t ftaapp_exit();

/* adds an FTA by key returns unique streamid which can be used to reference FTA*/

/* reuse:
 if reuse is set to 0, only FTA information will be returned
 if reuse is set to 1, instance will be returned provided there is an
 instance that is marked as reusable by fta/app who instantiated it
 */

/* reusable:
 if reusable is 0, new instance will be created and it will be marked as
 non-reusable regardless of how reuse flag is set by query writer
 
 if reusable is 1, new instance will be created and it will be marked as
 reusable regardless of how reuse flag is set by query writer
 
 if reusable is set to 2. new instance will be created with reusability flag
 matching the reusability flag of the query being instantiated
 */

FTAID ftaapp_add_fta(FTAname name, gs_uint32_t reuse, gs_uint32_t reusable,
                     gs_int32_t command, gs_int32_t sz, void *  data);

/* adds an FTA by key returns unique streamid which can be used to reference
 FTA. This FTA does not produce any local data. Instead it generates a
 compressed outpufile in gdat format. The path specifies the directory
 the file will be put in. base specifies the base part of the name
 the <temporal field value> will be prepented to the name and gdat.gz
 will be appended. The temporal field needs to be specified which
 can be an uint, int, ullong or llong. The delta is an integer defining how
 much the temoral value has to change before a new file is generated.
 The same ftaapp_remote_fta function is used to remove these FTAs.
 */
FTAID ftaapp_add_fta_print(FTAname name, gs_uint32_t reuse, gs_uint32_t reusable,
                           gs_int32_t command, gs_int32_t sz,
                           void *  data, gs_sp_t path,
                           gs_sp_t basename, gs_sp_t temporal_field, gs_sp_t split_field,
                           gs_uint32_t delta, gs_uint32_t split);

/* get the schema handle definition for the FTA associated with the streamid
 the returned handle will be freed by the app library when the corresponding
 fta is removed
 */
gs_schemahandle_t ftaapp_get_fta_schema(FTAID f);

/* get a schema handle definition for the FTA associated with the FTA name
 this handle has to be freed by the caller using the schema interface */
gs_schemahandle_t ftaapp_get_fta_schema_by_name(gs_sp_t name);

/* get an ascii representation of the schema definition
 for the FTA associated with the FTA name. The ascii representation
 is stored in a static buffer. The value will therefore change
 after subsequent calls to this function and the function is not
 multithread safe.*/
gs_sp_t ftaapp_get_fta_ascii_schema_by_name(gs_sp_t name);

/* control operations keyed of one to one mapping of stream id */
gs_retval_t ftaapp_control(FTAID f, gs_int32_t command, gs_int32_t sz, void *  data);

/* remove FTA keyed of stream id */
/* if recursive is true the children of f will get removed too */
gs_retval_t ftaapp_remove_fta(FTAID f, gs_uint32_t recursive);

/* same as sgroup_get_buffer just repeated to have a complet ftapp interface */
gs_retval_t ftaapp_get_tuple(FTAID * f, gs_uint32_t * size, void *tbuffer,
                             gs_int32_t tbuf_size, gs_int32_t timeout);

#endif
