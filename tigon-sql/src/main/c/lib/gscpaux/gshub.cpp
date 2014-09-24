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

#include <curl/curl.h>
#include "json.h"

// maximum length of URL for http requests
#define MAX_URL_LEN 64 * 1024

// maximu length of JSON response
#define MAX_JSON_LEN 64 * 1024

// sleep time between HTTP request retries
#define HTTP_RETRY_INTERVAL 5

typedef struct {
	char *memory;
	size_t size;
} memstruct;

// GSHUB endpoint
endpoint hub;

// GSHUB instance_name
gs_sp_t instance_name=0;

// callback for curl to write retrived http response into memory buffer
static size_t writemem_callback(void *contents, size_t size, size_t nmemb, void *userp) {
	size_t realsize = size * nmemb;
	memstruct* mem = (memstruct*)userp;

	mem->memory = (char*)realloc(mem->memory, mem->size + realsize + 1);
	if(mem->memory == NULL) {
		/* out of memory! */
		fprintf(stderr, "not enough memory (realloc returned NULL)\n");
		return 0;
	}
	memcpy(&(mem->memory[mem->size]), contents, realsize);
	mem->size += realsize;
	mem->memory[mem->size] = 0;

	return realsize;
}

// retrieve the endpoint of gs instance, source or sink identified by url
gs_retval_t get_service_endpoint (char* url, endpoint* instance, gs_bool_t block) {
	int ret;

	CURL *curl_handle;
	CURLcode res;
	memstruct chunk;

	// memory chunk for HTTP retrieval will be grown as needed by realloc
	chunk.memory = (char*)malloc(1);
	chunk.size = 0;

	// curl intialization
	curl_global_init(CURL_GLOBAL_ALL);
	curl_handle = curl_easy_init();

	// prepare options for curl HTTP GET retrieval
	curl_easy_setopt(curl_handle, CURLOPT_URL, url);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, writemem_callback);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&chunk);
	curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");

	while (true) {
		res = curl_easy_perform(curl_handle);
		if(res != CURLE_OK) {
			fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
			ret = -1;
		}
		else {
			// ask for HTTP return code
			long http_code;
			if(curl_easy_getinfo(curl_handle, CURLINFO_HTTP_CODE, &http_code) != CURLE_OK) {
				fprintf(stderr, "Unable to extract HTTP return code, %s\n", curl_easy_strerror(res));
				ret = -1;
			}

			// in blocking mode we will keep retrying
			if (http_code == 400) {
				if(chunk.memory) {
					free(chunk.memory);
					chunk.memory = NULL;
				}
				if (block) {
					sleep(HTTP_RETRY_INTERVAL);
					continue;
				} else {
					ret = 1;
					break;
				}

			}

			// ask for HTTP content-type
			char *ct;
			if (curl_easy_getinfo(curl_handle, CURLINFO_CONTENT_TYPE, &ct) != CURLE_OK || !ct) {
				fprintf(stderr, "Unable to extract HTTP content-type, %s\n", curl_easy_strerror(res));
				ret = -1;
			}

			if (http_code == 200 && !strcmp(ct, "application/json")) {

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

				json_value *root = json_parse(chunk.memory, &errorPos, (const char**)&errorDesc, &errorLine, &allocator);
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

	// curl cleanup
	curl_easy_cleanup(curl_handle);
	if(chunk.memory)
		free(chunk.memory);
	curl_global_cleanup();

	return ret;

}

// announce gs instance/sink/source to gshub. GSHUB is identified by URL, instance/sink/source information is in application/json
gs_retval_t set_endpoint_info (char* url, char* info) {
	int ret;

	CURL *curl_handle;
	CURLcode res;

	// curl intialization
	curl_global_init(CURL_GLOBAL_ALL);
	curl_handle = curl_easy_init();

	// prepare options for curl HTTP POST retrieval
	curl_easy_setopt(curl_handle, CURLOPT_URL, url);
	curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");
	// specify POST data
	curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, info);

	// specify headers
	struct curl_slist *headers = NULL;
	headers  = curl_slist_append(headers, "Content-Type: application/json");
	curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, headers);

	// do POST
	res = curl_easy_perform(curl_handle);
	if(res != CURLE_OK) {
		fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
		ret = -1;
	}
	else {
		// ask for HTTP return code
		long http_code;
		if(curl_easy_getinfo(curl_handle, CURLINFO_HTTP_CODE, &http_code) != CURLE_OK) {
			fprintf(stderr, "Unable to extract HTTP return code, %s\n", curl_easy_strerror(res));
			ret = -1;
		}

		if (http_code == 200)
			ret = 0;
		else
			ret = -1;
	}

	// curl cleanup
	curl_slist_free_all(headers); /* free the lheaders list*/
	curl_easy_cleanup(curl_handle);
	curl_global_cleanup();

	return ret;

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
	char ipstr[16];

	inet_ntop(AF_INET, &gshub.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(url, "http://%s:%d%s/%s", ipstr, ntohs(gshub.port), DISCOVER_INSTANCE_URL, instance_name);

	return get_service_endpoint(url, instance, block);
}

// Discover initialized gs instance endpoint by name.
extern "C" gs_retval_t get_initinstance(endpoint gshub, gs_sp_t instance_name, endpoint* instance, gs_bool_t block) {
	char url[MAX_URL_LEN];
	char ipstr[16];

	inet_ntop(AF_INET, &gshub.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(url, "http://%s:%d%s/%s", ipstr, ntohs(gshub.port), DISCOVER_INITINSTANCE_URL, instance_name);

	return get_service_endpoint(url, instance, block);
}

// Discover stream source endpoint by name.
extern "C" gs_retval_t get_streamsource(endpoint gshub, gs_sp_t source_name, endpoint* source, gs_bool_t block) {
	char url[MAX_URL_LEN];
	char ipstr[16];

	inet_ntop(AF_INET, &gshub.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(url, "http://%s:%d%s/%s", ipstr, ntohs(gshub.port), DISCOVER_SOURCE_URL, source_name);

	return get_service_endpoint(url, source, block);
}

// Discover stream sink endpoint by name.
extern "C" gs_retval_t get_streamsink(endpoint gshub, gs_sp_t sink_name, endpoint* sink, gs_bool_t block) {
	char url[MAX_URL_LEN];
	char ipstr[16];

	inet_ntop(AF_INET, &gshub.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(url, "http://%s:%d%s/%s", ipstr, ntohs(gshub.port), DISCOVER_SINK_URL, sink_name);

	return get_service_endpoint(url, sink, block);
}

// Discover if an isntance should start processing
gs_retval_t get_startprocessing(endpoint gshub, gs_sp_t instance_name, gs_bool_t block) {
	char url[MAX_URL_LEN];
	char ipstr[16];

	inet_ntop(AF_INET, &gshub.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(url, "http://%s:%d%s/%s", ipstr, ntohs(gshub.port), DISCOVER_STARTPROCESSING_URL, instance_name);

	return get_service_endpoint(url, NULL, block);
}

// Announce gs instance endpoint to gshub
extern "C" gs_retval_t set_instance(endpoint gshub, gs_sp_t instance_name, endpoint instance) {
	char url[MAX_URL_LEN];
	char ipstr[16];
	char info[MAX_JSON_LEN];

	inet_ntop(AF_INET, &gshub.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(url, "http://%s:%d%s", ipstr, ntohs(gshub.port), ANNOUNCE_INSTANCE_URL);

	inet_ntop(AF_INET, &instance.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(info, "{\"name\": \"%s\", \"ip\": \"%s\", \"port\": %d}", instance_name, ipstr, ntohs(instance.port));

	return set_endpoint_info(url, info);
}

// Announce initialized gs instance endpoint to gshub
extern "C" gs_retval_t set_initinstance(endpoint gshub, gs_sp_t instance_name) {
	char url[MAX_URL_LEN];
	char ipstr[16];
	char info[MAX_JSON_LEN];

	inet_ntop(AF_INET, &gshub.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(url, "http://%s:%d%s", ipstr, ntohs(gshub.port), ANNOUNCE_INITINSTANCE_URL);

	sprintf(info, "{\"name\": \"%s\"}", instance_name);

	return set_endpoint_info(url, info);
}

// Announce stream source endpoint to gshub
extern "C" gs_retval_t set_streamsource(endpoint gshub, gs_sp_t source_name, endpoint source) {
	char url[MAX_URL_LEN];
	char ipstr[16];
	char info[MAX_JSON_LEN];

	inet_ntop(AF_INET, &gshub.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(url, "http://%s:%d%s", ipstr, ntohs(gshub.port), ANNOUNCE_SOURCE_URL);

	inet_ntop(AF_INET, &source.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(info, "{\"name\": \"%s\", \"ip\": \"%s\", \"port\": %d}", source_name, ipstr, ntohs(source.port));

	return set_endpoint_info(url, info);
}

// Announce stream source endpoint to gshub
extern "C" gs_retval_t set_streamsink(endpoint gshub, gs_sp_t sink_name, endpoint sink) {
	char url[MAX_URL_LEN];
	char ipstr[16];
	char info[MAX_JSON_LEN];

	inet_ntop(AF_INET, &gshub.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(url, "http://%s:%d%s", ipstr, ntohs(gshub.port), ANNOUNCE_SINK_URL);

	inet_ntop(AF_INET, &sink.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(info, "{\"name\": \"%s\", \"ip\": \"%s\", \"port\": %d}", sink_name, ipstr, ntohs(sink.port));

	return set_endpoint_info(url, info);
}

// Announce to gshub that an instance can start processin
extern "C" gs_retval_t set_startprocessing(endpoint gshub, gs_sp_t instance_name) {
	char url[MAX_URL_LEN];
	char ipstr[16];
	char info[MAX_JSON_LEN];

	inet_ntop(AF_INET, &gshub.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(url, "http://%s:%d%s", ipstr, ntohs(gshub.port), ANNOUNCE_STARTPROCESSING_URL);

	sprintf(info, "{\"name\": \"%s\"}", instance_name);

	return set_endpoint_info(url, info);
}

// Announce stream subscriptino to gshub
extern "C" gs_retval_t set_streamsubscription(endpoint gshub, gs_sp_t instance_name, gs_sp_t sink_name) {
	char url[MAX_URL_LEN];
	char ipstr[16];
	char info[MAX_JSON_LEN];

	inet_ntop(AF_INET, &gshub.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(url, "http://%s:%d%s", ipstr, ntohs(gshub.port), ANNOUNCE_STREAM_SUBSCRIPTION);

	sprintf(info, "{\"name\": \"%s\", \"sink\": \"%s\"}", instance_name, sink_name);

	return set_endpoint_info(url, info);
}

// Announce new fta instantiation to gshub
extern "C" gs_retval_t set_ftainstance(endpoint gshub, gs_sp_t instance_name, gs_sp_t ftainstance_name, FTAID* id) {
	char url[MAX_URL_LEN];
	char ipstr[16];
	char info[MAX_JSON_LEN];

	inet_ntop(AF_INET, &gshub.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(url, "http://%s:%d%s", ipstr, ntohs(gshub.port), ANNOUNCE_FTA_INSTANCE);

	sprintf(info, "{\"name\": \"%s\", \"fta_name\": \"%s\", \"ftaid\": {\"ip\": %u, \"port\": %u, \"index\": %u, \"streamid\": %lli}}",
		instance_name, ftainstance_name, id->ip, id->port, id->index, id->streamid);

	return set_endpoint_info(url, info);
}

// Announce fta instance stats to gshub
extern "C" gs_retval_t set_instancestats(endpoint gshub, gs_sp_t instance_name, fta_stat* stats) {
	char url[MAX_URL_LEN];
	char ipstr[16];
	char info[MAX_JSON_LEN];

	inet_ntop(AF_INET, &gshub.ip, ipstr, INET_ADDRSTRLEN);
	sprintf(url, "http://%s:%d%s", ipstr, ntohs(gshub.port), ANNOUNCE_METRICS);

	sprintf(info, "{\"name\": \"%s\", \"ftaid\": {\"ip\": %u, \"port\": %u, \"index\": %u, \"streamid\": %llu}, "
		"\"metrics\": {\"in_tuple_cnt\": %u, \"out_tuple_cnt\": %u, \"out_tuple_sz\": %u, \"accepted_tuple_cnt\": %u, \"cycle_cnt\": %llu, \"collision_cnt\": %u, \"eviction_cnt\": %u, \"sampling_rate\": %f}}",
		instance_name, stats->ftaid.ip, stats->ftaid.port, stats->ftaid.index, stats->ftaid.streamid,
		stats->in_tuple_cnt, stats->out_tuple_cnt, stats->out_tuple_sz, stats->accepted_tuple_cnt, stats->cycle_cnt, stats->collision_cnt, stats->eviction_cnt, stats->sampling_rate);

	return set_endpoint_info(url, info);
}

