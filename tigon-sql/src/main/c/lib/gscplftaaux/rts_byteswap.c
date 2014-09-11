#include "gsconfig.h"
#include "gstypes.h"

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


gs_int16_t gscpbswap16(gs_int16_t x){
        return ((x << 8) & 0xff00) | ((x >> 8) & 0x00ff);
}

gs_int32_t gscpbswap32(gs_int32_t x ) {
    return	((x << 24) & 0xff000000 ) |
	((x <<  8) & 0x00ff0000 ) |
	((x >>  8) & 0x0000ff00 ) |
	((x >> 24) & 0x000000ff );
}


gs_int64_t gscpbswap64(gs_int64_t x) {
    gs_uint64_t t1;
    gs_uint64_t t2;
    t1=x>>32;
    t1 = gscpbswap32(t1);
    t2=x & 0xffffffff;
    t2 = gscpbswap32(t2);
    x = t2<<32;
    x = x|t1;
    return x;
}
