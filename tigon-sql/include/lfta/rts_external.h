#ifndef RTS_STRING_H
#define RTS_STRING_H

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

#include "gsconfig.h"
#include "gstypes.h"

//	struct string32 matches vstring32, its the packed record format.
//	struct string is the in-memory format.

struct string32{
	gs_int32_t length;
	gs_int32_t offset;
	gs_int32_t reserved;
};


/* struct string has to match definition in host.h however, the data
 types differ */

struct string {
    gs_int32_t length;
    gs_sp_t data;
    struct FTA * owner;
};

#include "fta.h"
#include "rts.h"


/* Basic string operation */

gs_retval_t str_assign_with_copy(struct FTA *, struct string * dest, struct string * src);

gs_retval_t str_assign_with_copy_in_tuple(struct string32 * dest, struct string * src,
                                          gs_sp_t start, gs_sp_t buf);

#define str_destroy(s) {if ((s)->owner!=0) fta_free((s)->owner,(s)->data);}

gs_retval_t str_replace(struct FTA *,  struct string * dest, struct string * src );

#define str_length(s) (s)->length

/* Searching within a string */

gs_retval_t str_exists_substr( struct string * str1, struct string * str2);

/* String comparison */

gs_retval_t str_compare( struct string * str1, struct string * str2);


/*	Construct a string constant */

gs_retval_t str_constructor(struct string *s, gs_sp_t l);


/* External Function definitions */

#define LLMIN(x,y) ((x)<(y)?(x):(y))
#define LLMAX(x,y) ((x)<(y)?(y):(x))
#define UMIN(x,y) ((x)<(y)?(x):(y))
#define UMAX(x,y) ((x)<(y)?(y):(x))
// type conversion
#define INT(c) ((int)(c))
#define UINT(c) ((unsigned int)(c))
#define ULLONG(c) ((unsigned long long)(c))
#define FLOAT(c) ((double)(c))

//	Cast away temporality
#define non_temporal(x)(x)



gs_uint32_t str_match_offset( gs_uint32_t offset, struct string * s1, struct string * s2);
gs_uint32_t byte_match_offset( gs_uint32_t offset, gs_uint32_t val, struct string * s2);
gs_param_handle_t register_handle_for_str_regex_match_slot_1(struct FTA * f,
                                                             struct string* pattern);
gs_uint32_t str_regex_match(struct string* str, gs_param_handle_t pattern_handle);
gs_retval_t deregister_handle_for_str_regex_match_slot_1(gs_param_handle_t handle);

gs_param_handle_t register_handle_for_str_partial_regex_match_slot_1(struct FTA * f,
                                                                     struct string* pattern);
gs_uint32_t str_partial_regex_match(struct string* str,
                                    gs_param_handle_t pattern_handle,
                                    gs_uint32_t maxlen);
gs_retval_t deregister_handle_for_str_partial_regex_match_slot_1(
                                                                 gs_param_handle_t
                                                                 handle);

inline static gs_retval_t str_truncate(struct string * result, struct string *str, gs_uint32_t length) {
	result->data=str->data;
	result->length=(str->length<length)?str->length:length;
	result->owner=0;
	return 0;
}

//	constant string conversions
gs_param_handle_t register_handle_for_strtoi_c_slot_0(struct FTA * f, struct string* istr) ;
gs_retval_t deregister_handle_for_strtoi_c_slot_0(gs_param_handle_t h) ;
#define strtoi_c(h) ((unsigned int)(h))

gs_param_handle_t register_handle_for_strtoip_c_slot_0(struct FTA * f, struct string* istr) ;
gs_retval_t deregister_handle_for_strtoip_c_slot_0(gs_param_handle_t h) ;
#define strtoip_c(h) ((unsigned int)(h))



////////////////////////////////////////////////
///		IPV6

#ifndef IPV6_STR
#define IPV6_STR
struct ipv6_str{
	gs_uint32_t v[4];
};
#endif

gs_int32_t ipv6_compare( struct ipv6_str  i1, struct ipv6_str  i2);
gs_int32_t Ipv6_Constructor(struct ipv6_str *s, char *l);
struct ipv6_str And_Ipv6(const struct ipv6_str i1, const struct ipv6_str i2);
struct ipv6_str Or_Ipv6(const struct ipv6_str i1, const struct ipv6_str i2);
struct ipv6_str hton_ipv6(struct ipv6_str s);
struct ipv6_str ntoh_ipv6(struct ipv6_str s);

#endif
