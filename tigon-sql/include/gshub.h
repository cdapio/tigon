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
 * gshub.h: interface to GSHUB to announce and discover gs instances, sources and sinks
 */

#ifndef GSHUB_H
#define GSHUB_H

#include "gstypes.h"
#include "fta.h"
#include "fta_stat.h"

#define DISCOVER_INSTANCE_URL "/v1/discover-instance"
#define DISCOVER_INITINSTANCE_URL "/v1/discover-initialized-instance"
#define DISCOVER_SOURCE_URL "/v1/discover-source"
#define DISCOVER_SINK_URL "/v1/discover-sink"
#define DISCOVER_STARTPROCESSING_URL "/v1/discover-start-processing"
#define ANNOUNCE_INSTANCE_URL "/v1/announce-instance"
#define ANNOUNCE_INITINSTANCE_URL "/v1/announce-initialized-instance"
#define ANNOUNCE_SOURCE_URL "/v1/announce-source"
#define ANNOUNCE_SINK_URL "/v1/announce-sink"
#define ANNOUNCE_STARTPROCESSING_URL "/v1/announce-start-processing"
#define ANNOUNCE_STREAM_SUBSCRIPTION "/v1/announce-stream-subscription"
#define ANNOUNCE_FTA_INSTANCE "/v1/announce-fta-instance"
#define ANNOUNCE_METRICS "/v1/log-metrics"

typedef struct {
	gs_uint32_t ip;
	gs_uint16_t port;
} endpoint;


#ifdef __cplusplus
extern "C" {
#endif

/* saving and retrival of gshub endpoint */
gs_retval_t set_hub(endpoint gshub);
gs_retval_t get_hub(endpoint* gshub);

/* savinf and retrieving instance name */
gs_retval_t set_instance_name(gs_sp_t instancename);
gs_sp_t get_instance_name();

/* Discover gs instance endpoint by name.
 * Return value of 0 indicates success. If blocking mode is used return value > 0 indicates that retry is needed. Return value < 0 indicates an error. */
gs_retval_t get_instance(endpoint gshub, gs_sp_t instance_name, endpoint* instance, gs_bool_t block);

/* Discover initialized gs instance endpoint by name.
 * Return value of 0 indicates success. If blocking mode is used return value > 0 indicates that retry is needed. Return value < 0 indicates an error. */
gs_retval_t get_initinstance(endpoint gshub, gs_sp_t instance_name, endpoint* instance, gs_bool_t block);

/* Discover stream source endpoint by name.
 * Return value of 0 indicates success. If blocking mode is used return value > 0 indicates that retry is needed. Return value < 0 indicates an error. */
gs_retval_t get_streamsource(endpoint gshub, gs_sp_t source_name, endpoint* source, gs_bool_t block);

/* Discover stream sink endpoint by name.
 * Return value of 0 indicates success. If blocking mode is used return value > 0 indicates that retry is needed. Return value < 0 indicates an error. */
gs_retval_t get_streamsink(endpoint gshub, gs_sp_t sink_name, endpoint* sink, gs_bool_t block);

/* Discover if an isntance should start processing
 * Return value of 0 indicates success. If blocking mode is used return value > 0 indicates that retry is needed. Return value < 0 indicates an error. */
gs_retval_t get_startprocessing(endpoint gshub, gs_sp_t instance_name, gs_bool_t block);

/* Announce gs instance endpoint to gshub. Return value of 0 indicates success. */
gs_retval_t set_instance(endpoint gshub, gs_sp_t instance_name, endpoint instance);

/* Announce initialized gs instance endpoint to gshub. Return value of 0 indicates success. */
gs_retval_t set_initinstance(endpoint gshub, gs_sp_t instance_name);

/* Announce stream source endpoint to gshub. Return value of 0 indicates success. */
gs_retval_t set_streamsource(endpoint gshub, gs_sp_t source_name, endpoint source);

/* Announce stream source endpoint to gshub. Return value of 0 indicates success. */
gs_retval_t set_streamsink(endpoint gshub, gs_sp_t sink_name, endpoint sink);

/* Announce to gshub that an instance can start processin. Return value of 0 indicates success. */
gs_retval_t set_startprocessing(endpoint gshub, gs_sp_t instance_name);

/* Announce stream subscription to gshub. Return value of 0 indicates success. */
gs_retval_t set_streamsubscription(endpoint gshub, gs_sp_t instance_name, gs_sp_t sink_name);

/* Announce new fta instantiation to gshub. Return value of 0 indicates success. */
gs_retval_t set_ftainstance(endpoint gshub, gs_sp_t instance_name, gs_sp_t ftainstance_name, FTAID* id);

/* Announce fta instance stats to gshub. Return value of 0 indicates success. */
gs_retval_t set_instancestats(endpoint gshub, gs_sp_t instance_name, fta_stat* stats);

#ifdef __cplusplus
}
#endif

#endif		/* GSHUB_H */
