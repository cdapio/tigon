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
 * filter.h: hash table function which match the interface
 */
#ifndef FILTER_H
#define FILTER_H

/* FILTER FUNCTIONS */
/* ===============  */

/* The following functions are used to manage filter sets for flow
   and connection filters
*/

struct filter {
    gs_uint32_t size;
    gs_uint8_t data[1];
};

struct filter * ftafilter_create(gs_int32_t size);
void ftafilter_delete(struct filter * filter);
void ftafilter_add_flow ( struct filter * filter, gs_uint32_t ip, gs_uint32_t port);
void fyafilter_add_connection ( struct filter * filter, gs_uint32_t ip1,
			     gs_uint32_t port1, gs_uint32_t ip2, gs_uint32_t port2);
void ftafilter_rm_flow ( struct filter * filter, gs_uint32_t ip, gs_uint32_t port);
void ftafilter_rm_connection ( struct filter * filter, gs_uint32_t ip1,
			    gs_uint32_t port1, gs_uint32_t ip2, gs_uint32_t port2);
struct vstring * ftafilter_to_vstring(struct filter * f );
void ftafilter_vstring_free(struct vstring * res);


#endif
