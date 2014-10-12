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

#include "gshub.h"
#include "gslog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "simple_http.h"
#include "json.h"

// maximum length of URL for http requests
#define MAX_URL_LEN 64 * 1024

// maximu length of JSON response
#define MAX_JSON_LEN 64 * 1024

// sleep time between HTTP request retries
#define HTTP_RETRY_INTERVAL 5

// GSHUB endpoint
endpoint hub;

// GSHUB instance_name
gs_sp_t instance_name=0;


// retrieve the endpoint of gs instance, source or sink identified by url
gs_retval_t get_service_endpoint (endpoint gshub, gs_csp_t url, endpoint* instance, gs_bool_t block) {
	int res, ret;
	gs_uint32_t http_code;
	char json[MAX_JSON_LEN];

	while (true) {
		res = http_get_request(gshub, url, &http_code, json);
		if(res) {
			fprintf(stderr, "http_get_request() failed\n");
			ret = -1;
		}
		else {
			// in blocking mode we will keep retrying
			if (http_code == 400) {
				if (block) {
					sleep(HTTP_RETRY_INTERVAL);
					continue;
				} else {
					ret = 1;
					break;
				}

			}
			if (http_code == 200 ) {
				// if instance is NULL there is no need to parse json
				if (!instance) {
					ret = 0;
					break;
				}

				// now parse json response
				char *errorPos = 0;
				char *errorDesc = 0;
				int errorLine = 0;
				block_allocator allocator(1 << 10); // 1 KB per block

				json_value *root = json_parse(json, &errorPos, (const char**)&errorDesc, &errorLine, &allocator);
				if (!root) {
					fprintf(stderr, "GSHUB returned invalid json response, error description - %s, error line - %d\n", errorDesc, errorLine);
					ret = -1;
				} else {
					unsigned long json_ip = 0;
					bool ip_set = false;
					unsigned json_port = 0;
					bool port_set = false;
					for (json_value *it = root->first_child; it; it = it->next_sibling) {
						if (it->name) {
							if (!strcmp(it->name, "ip") && it->type == JSON_STRING) {
								inet_pton(AF_INET, it->string_value, &json_ip);
								ip_set = true;
							} else if (!strcmp(it->name, "port") && it->type == JSON_INT) {
								json_port = htons(it->int_value);
								port_set = true;
							}
						}
					}

					if (!ip_set || !port_set) {
						fprintf(stderr, "GSHUB returned json response with missing ip or port fields");
						ret = -1;
					}
					instance->ip = json_ip;
					instance->port = json_port;

					ret = 0;
				}
			} else
				ret = -1;
		}
		break;
	}

	return ret;

}

// announce gs instance/sink/source to gshub. GSHUB is identified by URL, instance/sink/source information is in application/json
gs_retval_t set_endpoint_info (endpoint gshub, gs_csp_t url, char* info) {
	int res;
	gs_uint32_t http_code;

	res = http_post_request(gshub, url, info, &http_code);
	if (res) {
		fprintf(stderr, "http_post_request() failed\n");
		return -1;
	}

	if (http_code == 200)
		return 0;
	else
		return -1;
}

extern "C" gs_retval_t set_instance_name(gs_sp_t instancename){
	instance_name=strdup(instancename);

	return 0;
}

extern "C" gs_sp_t get_instance_name() {
	return instance_name;
}

// save gshub endpoint
extern "C" gs_retval_t set_hub(endpoint gshub) {
	hub.ip = gshub.ip;
	hub.port = gshub.port;

	return 0;
}

// retrieve gsbub endpoint
extern "C" gs_retval_t get_hub(endpoint* gshub) {

	if (hub.ip==0)
		return -1;
	gshub->ip = hub.ip;
	gshub->port = hub.port;

	return 0;
}

// Discover gs instance endpoint by name.
extern "C" gs_retval_t get_instance(endpoint gshub, gs_sp_t instance_name, endpoint* instance, gs_bool_t block) {
	char url[MAX_URL_LEN];

	sprintf(url, "%s/%s", DISCOVER_INSTANCE_URL, instance_name);

	return get_service_endpoint(gshub, url, instance, block);
}

// Discover initialized gs instance endpoint by name.
extern "C" gs_retval_t get_initinstance(endpoint gshub, gs_sp_t instance_name, endpoint* instance, gs_bool_t block) {
	char url[MAX_URL_LEN];

	sprintf(url, "%s/%s", DISCOVER_INITINSTANCE_URL, instance_name);

	return get_service_endpoint(gshub, url, instance, block);
}

// Discover stream source endpoint by name.
extern "C" gs_retval_t get_streamsource(endpoint gshub, gs_sp_t source_name, endpoint* source, gs_bool_t block) {
	char url[MAX_URL_LEN];

	sprintf(url, "%s/%s", DISCOVER_SOURCE_URL, source_name);

	return get_service_endpoint(gshub, url, source, block);
}

// Discover stream sink endpoint by name.
extern "C" gs_retval_t get_streamsink(endpoint gshub, gs_sp_t sink_name, endpoint* sink, gs_bool_t block) {
	char url[MAX_URL_LEN];

	sprintf(url, "%s/%s", DISCOVER_SINK_URL, sink_name);

	return get_service_endpoint(gshub, url, sink, block);
}

// Discover if an isntance should start processing
gs_retval_t get_startprocessing(endpoint gshub, gs_sp_t instance_name, gs_bool_t block) {
	char url[MAX_URL_LEN];

	sprintf(url, "%s/%s", DISCOVER_STARTPROCESSING_URL, instance_name);

	return get_service_endpoint(gshub, url, NULL, block);
}

// Announce gs instance endpoint to gshub
extern "C" gs_retval_t set_instance(endpoint gshub, gs_sp_t instance_name, endpoint instance) {
	char ipstr[16];
	char info[MAX_JSON_LEN];

	inet_ntop(AF_INET, &instance.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(info, "{\"name\": \"%s\", \"ip\": \"%s\", \"port\": %d}", instance_name, ipstr, ntohs(instance.port));

	return set_endpoint_info(gshub, ANNOUNCE_INSTANCE_URL, info);
}

// Announce initialized gs instance endpoint to gshub
extern "C" gs_retval_t set_initinstance(endpoint gshub, gs_sp_t instance_name) {
	char info[MAX_JSON_LEN];

	sprintf(info, "{\"name\": \"%s\"}", instance_name);

	return set_endpoint_info(gshub, ANNOUNCE_INITINSTANCE_URL, info);
}

// Announce stream source endpoint to gshub
extern "C" gs_retval_t set_streamsource(endpoint gshub, gs_sp_t source_name, endpoint source) {
	char ipstr[16];
	char info[MAX_JSON_LEN];

	inet_ntop(AF_INET, &source.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(info, "{\"name\": \"%s\", \"ip\": \"%s\", \"port\": %d}", source_name, ipstr, ntohs(source.port));

	return set_endpoint_info(gshub, ANNOUNCE_SOURCE_URL, info);
}

// Announce stream source endpoint to gshub
extern "C" gs_retval_t set_streamsink(endpoint gshub, gs_sp_t sink_name, endpoint sink) {
	char ipstr[16];
	char info[MAX_JSON_LEN];

	inet_ntop(AF_INET, &sink.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(info, "{\"name\": \"%s\", \"ip\": \"%s\", \"port\": %d}", sink_name, ipstr, ntohs(sink.port));

	return set_endpoint_info(gshub, ANNOUNCE_SINK_URL, info);
}

// Announce to gshub that an instance can start processin
extern "C" gs_retval_t set_startprocessing(endpoint gshub, gs_sp_t instance_name) {
	char info[MAX_JSON_LEN];

	sprintf(info, "{\"name\": \"%s\"}", instance_name);

	return set_endpoint_info(gshub, ANNOUNCE_STARTPROCESSING_URL, info);
}

// Announce stream subscriptino to gshub
extern "C" gs_retval_t set_streamsubscription(endpoint gshub, gs_sp_t instance_name, gs_sp_t sink_name) {
	char info[MAX_JSON_LEN];

	sprintf(info, "{\"name\": \"%s\", \"sink\": \"%s\"}", instance_name, sink_name);

	return set_endpoint_info(gshub, ANNOUNCE_STREAM_SUBSCRIPTION, info);
}

// Announce new fta instantiation to gshub
extern "C" gs_retval_t set_ftainstance(endpoint gshub, gs_sp_t instance_name, gs_sp_t ftainstance_name, FTAID* id) {
	char info[MAX_JSON_LEN];

	sprintf(info, "{\"name\": \"%s\", \"fta_name\": \"%s\", \"ftaid\": {\"ip\": %u, \"port\": %u, \"index\": %u, \"streamid\": %lli}}",
		instance_name, ftainstance_name, id->ip, id->port, id->index, id->streamid);

	return set_endpoint_info(gshub, ANNOUNCE_FTA_INSTANCE, info);
}

// Announce fta instance stats to gshub
extern "C" gs_retval_t set_instancestats(endpoint gshub, gs_sp_t instance_name, fta_stat* stats) {
	char url[MAX_URL_LEN];
	char info[MAX_JSON_LEN];

	sprintf(info, "{\"name\": \"%s\", \"ftaid\": {\"ip\": %u, \"port\": %u, \"index\": %u, \"streamid\": %llu}, "
		"\"metrics\": {\"in_tuple_cnt\": %u, \"out_tuple_cnt\": %u, \"out_tuple_sz\": %u, \"accepted_tuple_cnt\": %u, \"cycle_cnt\": %llu, \"collision_cnt\": %u, \"eviction_cnt\": %u, \"sampling_rate\": %f}}",
		instance_name, stats->ftaid.ip, stats->ftaid.port, stats->ftaid.index, stats->ftaid.streamid,
		stats->in_tuple_cnt, stats->out_tuple_cnt, stats->out_tuple_sz, stats->accepted_tuple_cnt, stats->cycle_cnt, stats->collision_cnt, stats->eviction_cnt, stats->sampling_rate);

	return set_endpoint_info(gshub, ANNOUNCE_METRICS, info);
}

