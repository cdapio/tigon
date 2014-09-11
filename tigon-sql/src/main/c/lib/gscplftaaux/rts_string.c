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

#include "rts_external.h"
#include <stdio.h>
#include <regex.h>
#include "gsconfig.h"
#include "gstypes.h"


#include "stdlib.h"


gs_retval_t str_assign_with_copy(struct FTA * f, struct string * dest, struct string * src)
{
    if ((dest->data = fta_alloc(f,src->length))==0) {
	return -1;
    }
    dest->length=src->length;
    dest->owner=f;
    memcpy(dest->data,src->data,src->length);
    return 0;
}

gs_retval_t str_assign_with_copy_in_tuple(struct string32 * dest, struct string * src,
				  gs_sp_t start, gs_sp_t buf)
{
    gs_uint32_t st;
    st=(unsigned int)(buf-start);
//    dest->length=htonl(src->length);
//    dest->reserved=htonl(0);
//    dest->offset=htonl(st);
    dest->length=src->length;
    dest->reserved=0;
    dest->offset=st;
    memcpy(buf,src->data,src->length);
    return 0;
}



gs_retval_t str_replace( struct FTA * f, struct string * dest, struct string * src )
{
    str_destroy(dest);
    return str_assign_with_copy(f, dest, src);
}

/* Searching within a string */


gs_retval_t str_exists_substr( struct string * s1, struct string * s2)
{
  register gs_uint8_t * st1 = (gs_uint8_t *)(s1->data);
  register gs_uint8_t * st2 = (gs_uint8_t *)(s2->data);
  register gs_uint8_t s2f=st2[0];
  register gs_int32_t len1 = s1->length-s2->length; /* the lates point I have to find
						a match of the first char */
  register gs_int32_t len2 = s2->length;
  register gs_int32_t x,y;


  for (x=0; x<len1 ; x++)
  {
      if (st1[x]==s2f) {
	  for (y=0; y<len2 && st1[x+y]==st2[y];y++);
	  if (y==len2) {
	      return 1;
	  }
      }
  }
  return 0;
}


gs_uint32_t str_match_offset( gs_uint32_t offset, struct string * s1, struct string * s2) {
  register gs_uint8_t * st1 = (gs_uint8_t *)(s1->data);
  register gs_uint8_t * st2 = (gs_uint8_t *)(&s2->data[offset]);
  register gs_int32_t x;
  register gs_int32_t len2 = s2->length-offset;
  register gs_int32_t len1 = s1->length;
  if (len2<len1) return 0;
  for(x=0; x<len1; x++) {
    if (st1[x]!=st2[x]) return 0;
  }
  return 1;
}


gs_uint32_t byte_match_offset( gs_uint32_t offset, gs_uint32_t val, struct string * s2) {
  register gs_uint8_t * st2 = (gs_uint8_t *)(s2->data);
  register gs_uint8_t v = (unsigned char) val;
//  if ((s2->length <= offset)||(offset<0)) return 0;
  if (s2->length <= offset) return 0;
  return (st2[offset]==v)?1:0;
}


gs_retval_t str_compare( struct string * str1, struct string * str2)
{
    gs_int32_t len;
    gs_int32_t x;
    len = (str1->length>str2->length)?str2->length:str1->length;
    for(x=0;x<len;x++) {
	if (str1->data[x]>str2->data[x]) {
	    return 1;
	}
	if (str1->data[x]<str2->data[x]) {
	    return -1;
	}
    }

    if (str1->length>str2->length) {
	return 1;
    }
    if (str2->length>str1->length) {
	return -1;
    }
    return 0;
}

gs_retval_t str_constructor(struct string *s, gs_sp_t l){
    s->data =  l;
    s->owner = NULL;
    s->length = 0;
    while(l[s->length] != '\0') s->length++;
    return(0);
}


gs_param_handle_t register_handle_for_str_regex_match_slot_1(struct FTA * f,
					struct string* pattern) {
    regex_t * reg;
    //XXX bug should allocate towards FTA
    if ((reg=(regex_t *) fta_alloc(0,sizeof(regex_t)))==0)  {
	return 0;
    }
    if (regcomp(reg,(gs_sp_t)(pattern->data), REG_NEWLINE|REG_EXTENDED|REG_NOSUB)!=0) {
	return 0;
    }
    return (gs_param_handle_t) reg;
}

gs_uint32_t str_regex_match(struct string* str, gs_param_handle_t pattern_handle) {
    regex_t * reg = (regex_t *) pattern_handle ;
    gs_sp_t source = (gs_sp_t)(str->data);
    int res;
    if (str->length==0) return 0;

    /* HACK ALERT: commented out regnexec invocations to unbreak the build */
    res = REG_NOMATCH;
    /* res = regnexec(reg, (gs_sp_t)(str->data),str->length, 0, NULL, 0); */
    return (res==REG_NOMATCH)?0:1;
}


gs_retval_t deregister_handle_for_str_regex_match_slot_1(gs_param_handle_t handle) {
    regex_t * x = (regex_t *) handle;
    regfree(x);
    if (x!=0) fta_free(0,(void *)x);
    return 0;
}

gs_param_handle_t register_handle_for_str_partial_regex_match_slot_1(struct FTA * f,
					struct string* pattern) {
    regex_t * reg;
    if ((reg=(regex_t *) fta_alloc(0,sizeof(regex_t)))==0)  {
	return 0;
    }
    if (regcomp(reg,(gs_sp_t)(pattern->data), REG_NEWLINE|REG_EXTENDED|REG_NOSUB)!=0) {
	return 0;
    }
    return (gs_param_handle_t) reg;
}

gs_uint32_t str_partial_regex_match(struct string* str,
			     gs_param_handle_t pattern_handle,
			     gs_uint32_t maxlen) {
    regex_t * reg = (regex_t *) pattern_handle ;
    gs_sp_t source = (gs_sp_t)(str->data);
    gs_int32_t res;
    gs_int32_t end;
    if (str->length==0) return 0;
    end=(maxlen>(str->length-1))?(str->length-1):maxlen;
    /* HACK ALERT: commented out regnexec invocations to unbreak the build */
    res = REG_NOMATCH;
    /* res = regnexec(reg, (gs_sp_t)(str->data),end, 0, NULL, 0); */
    return (res==REG_NOMATCH)?0:1;
}


gs_retval_t deregister_handle_for_str_partial_regex_match_slot_1(
				  gs_param_handle_t handle) {
    regex_t * x = (regex_t *) handle;
    regfree(x);
    if (x!=0) fta_free(0,(void *)x);
    return 0;
}



static gs_uint32_t nextint(struct string *str , gs_uint32_t * offset, gs_uint32_t *res) {
	gs_uint8_t * s = (gs_uint8_t *)(str->data);
	gs_int32_t v = 0;
	*res = 0;
	while(*offset<str->length) {
		if ((s[*offset]>='0') && (s[*offset]<='9')) {
			v=1;
			*res= (*res*10) + (gs_uint32_t) (s[*offset]-'0');
		} else {
			if (v!=0) { // got some valid result
				return 1;
			} // otherwise skip leading grabage
		}
		(*offset)++;
 	}
	return v;
}


gs_param_handle_t register_handle_for_strtoi_c_slot_0(struct FTA * f, struct string* istr) {
	gs_uint32_t offset,r;
	offset=0;
	if (nextint(istr,&offset,&r)!=0)
		return (gs_param_handle_t) r;
	return (gs_param_handle_t) 0;
}
gs_retval_t deregister_handle_for_strtoi_c_slot_0(gs_param_handle_t h) {
	return 0;
}

gs_param_handle_t register_handle_for_strtoip_c_slot_0(struct FTA * f, struct string* istr) {
        gs_uint32_t ip1,ip2,ip3,ip4,offset,r;
        offset=0;
        if (nextint(istr,&offset,&ip1)==0) return (gs_param_handle_t)0;
        if (nextint(istr,&offset,&ip2)==0) return (gs_param_handle_t)0;
        if (nextint(istr,&offset,&ip3)==0) return (gs_param_handle_t)0;
        if (nextint(istr,&offset,&ip4)==0) return (gs_param_handle_t)0;
        r=ip1<<24|ip2<<16|ip3<<8|ip4;
		return (gs_param_handle_t)r;
}
gs_retval_t deregister_handle_for_strtoip_c_slot_0(gs_param_handle_t h) {
	return 0;
}


///////////////////////////////////////////////////
//	ipv6 procedures



gs_retval_t ipv6_compare( struct ipv6_str i1, struct ipv6_str i2){
    if(i1.v[0] > i2.v[0])
	return 1;
    if(i1.v[0] < i2.v[0])
	return -1;
    if(i1.v[1] > i2.v[1])
	return 1;
    if(i1.v[1] < i2.v[1])
	return -1;
    if(i1.v[2] > i2.v[2])
	return 1;
    if(i1.v[2] < i2.v[2])
	return -1;
    if(i1.v[3] > i2.v[3])
	return 1;
    if(i1.v[3] < i2.v[3])
	return -1;

    return 0;
}

gs_retval_t Ipv6_Constructor(struct ipv6_str *s, gs_sp_t l){
	gs_uint32_t i0=0,i1=0,i2=0,i3=0,i4=0,i5=0,i6=0,i7=0;
	sscanf(l,"%x:%x:%x:%x:%x:%x:%x:%x",&i0,&i1,&i2,&i3,&i4,&i5,&i6,&i7);
	s->v[0] = ((i0 & 0xffff) << 16) | (i1 & 0xffff);
	s->v[1] = ((i2 & 0xffff) << 16) | (i3 & 0xffff);
	s->v[2] = ((i4 & 0xffff) << 16) | (i5 & 0xffff);
	s->v[3] = ((i6 & 0xffff) << 16) | (i7 & 0xffff);
    return(0);
}

struct ipv6_str And_Ipv6(const struct ipv6_str i1, const struct ipv6_str i2){
        struct ipv6_str ret;
        ret.v[0] = i1.v[0] & i2.v[0];
        ret.v[1] = i1.v[1] & i2.v[1];
        ret.v[2] = i1.v[2] & i2.v[2];
        ret.v[3] = i1.v[3] & i2.v[3];
        return ret;
}

struct ipv6_str Or_Ipv6(const struct ipv6_str i1, const struct ipv6_str i2){
        struct ipv6_str ret;
        ret.v[0] = i1.v[0] | i2.v[0];
        ret.v[1] = i1.v[1] | i2.v[1];
        ret.v[2] = i1.v[2] | i2.v[2];
        ret.v[3] = i1.v[3] | i2.v[3];
        return ret;
}
struct ipv6_str hton_ipv6(struct ipv6_str s){
        struct ipv6_str ret;
//        ret.v[0] = htonl(s.v[0]);
//        ret.v[1] = htonl(s.v[1]);
//        ret.v[2] = htonl(s.v[2]);
//        ret.v[3] = htonl(s.v[3]);
        ret.v[0] = s.v[0];
        ret.v[1] = s.v[1];
        ret.v[2] = s.v[2];
        ret.v[3] = s.v[3];
	return ret;
}

struct ipv6_str ntoh_ipv6(struct ipv6_str s){
        struct ipv6_str ret;
//        ret.v[0] = ntohl(s.v[0]);
//        ret.v[1] = ntohl(s.v[1]);
//        ret.v[2] = ntohl(s.v[2]);
//        ret.v[3] = ntohl(s.v[3]);
        ret.v[0] = s.v[0];
        ret.v[1] = s.v[1];
        ret.v[2] = s.v[2];
        ret.v[3] = s.v[3];
	return ret;
}


