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
#ifndef IPCENCODING_H
#define IPCENCODING_H
#include "gsconfig.h"
#include "gstypes.h"

#include <fta.h>
#include <systat.h>
#include <gscpipc.h>
#include <fta_stat.h>


#define FTA_LOOKUP 1
#define FTA_ALLOC_INSTANCE 2
#define FTA_ALLOC_PRINT_INSTANCE 3
#define FTA_FREE_INSTANCE 4
#define FTA_CONTROL 5
#define FTA_PRODUCER_FAILURE 6
#define FTA_HEARTBEAT 7

#define GSCP_GET_BUFFER 8

#define FTA_REGISTER 9
#define FTA_UNREGISTER 10

#define PROCESS_CONTROL 11

#define RESULT_OPCODE_BASE 128
#define STANDARD_RESULT 129
#define FTAFIND_RESULT 130
#define FTA_RESULT 131

/* the following result codes are ignored in standard
 * processing they are only used when blocking for them
 * directly
 */

#define RESULT_OPCODE_IGNORE 256
#define WAKEUP  266
#define TIMEOUT 267

struct processtate {
    gs_int32_t active;
    gs_int32_t type;
    gs_int32_t buffersize;
    gs_int32_t deviceid;
    gs_int32_t mapcnt;
    gs_sp_t *map;
};

extern struct processtate curprocess;

struct hostcall {
    gs_int32_t callid;
    gs_int32_t size;
};


struct fta_find_arg {
    struct hostcall h;
    gs_int8_t  name[MAXFTANAME];
    gs_uint32_t reuse;
};

struct fta_alloc_instance_arg {
    struct hostcall h;
    FTAID subscriber;
    FTAID f;
    gs_int8_t  name[MAXFTANAME];
    gs_int8_t  schema[MAXSCHEMASZ];
    gs_uint32_t reusable;
    gs_int32_t command;
    gs_int32_t sz;
    gs_int8_t  path[MAXPRINTSTRING];
    gs_int8_t  basename[MAXPRINTSTRING];
    gs_int8_t  temporal_field[MAXPRINTSTRING];
	gs_int8_t  split_field[MAXPRINTSTRING];
	gs_uint32_t split;
    gs_uint32_t delta;
    gs_int8_t  data[1];
};

struct fta_free_instance_arg {
    struct hostcall h;
    FTAID subscriber;
    FTAID f;
    gs_uint32_t recursive;
};

struct fta_control_arg {
    struct hostcall h;
    FTAID subscriber;
    FTAID f;
    gs_int32_t command;
    gs_int32_t sz;
    gs_int8_t  data[1];
};


struct fta_notify_producer_failure_arg {
    struct hostcall h;
    FTAID sender;
    FTAID producer;
};

struct fta_heartbeat_arg {
    struct hostcall h;
    FTAID sender;
    gs_uint64_t trace_id;
    gs_int32_t sz;
    fta_stat data[1];
};

struct gscp_get_buffer_arg {
    struct hostcall h;
    gs_int32_t timeout;
};

struct fta_register_arg {
    struct hostcall h;
    gs_uint32_t reusable;
    gs_int8_t  name[MAXFTANAME];
    gs_int8_t  schema[MAXSCHEMASZ];
    FTAID f;
    FTAID subscriber;
};

struct fta_unregister_arg {
    struct hostcall h;
    FTAID f;
    FTAID subscriber;
};

struct tuple_post_arg {
    struct hostcall h;
    gs_int32_t stream_id;
    gs_int32_t sz;
    void * tuple;
};

struct process_control_arg {
    struct hostcall h;
    gs_int32_t command;
    gs_int32_t sz;
    gs_int8_t  data[1];
};

struct standard_result {
    struct hostcall h;
    gs_int32_t result;
};

struct wakeup_result {
    struct hostcall h;
};

struct timeout_result {
    struct hostcall h;
};


struct fta_result {
    struct hostcall h;
    gs_int32_t result;
    FTAID f;
};

struct ftafind_result {
    struct hostcall h;
    gs_int32_t result;
    FTAID f;
    gs_int8_t  schema[MAXSCHEMASZ];
};

#endif
