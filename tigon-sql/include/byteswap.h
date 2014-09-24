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
#ifndef BYTESWAP_H
#define BYTESWAP_H

#ifdef __cplusplus
extern "C" {
#endif
    
#include "gsconfig.h"
#include "gstypes.h"
    
    
    gs_uint16_t gscpbswap16(gs_uint16_t);
    gs_uint32_t gscpbswap32(gs_uint32_t);
    gs_uint64_t gscpbswap64(gs_uint64_t);
    
#ifdef __cplusplus
}
#endif

#undef ntohl
#undef ntohs
#undef htons
#undef htonl
#undef htonll
#undef ntohll


#define	ntohs(x)	gscpbswap16((gs_uint16_t)(x))
#define	ntohl(x)	gscpbswap32((gs_uint32_t)(x))
#define ntohll(x)       gscpbswap64((gs_uint64_t)(x))
#define	htons(x)	gscpbswap16((gs_uint16_t)(x))
#define	htonl(x)	gscpbswap32((gs_uint32_t)(x))
#define htonll(x)       gscpbswap64((gs_uint64_t)(x))

/* do nothing fo floating point numbers */
#define htonf(x)        (x)
#define ntohf(x)        (x)


#endif
