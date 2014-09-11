#include "md_stdlib.h"
#include "gsconfig.h"
#include "gstypes.h"
#include "rts_external.h"

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


gs_retval_t has_ipv4_option( struct string * s, gs_uint32_t ipoption) {
    gs_int32_t x=0;
    gs_int32_t on;
    gs_int32_t len;

    if (s->length==0) return 0; /* no match */

    while(0==0) {
	if ((on=s->data[x]&0x1f)==ipoption) return 1; /* got a match */
	switch (on) {
	  case 0:
	    return 0; /* no match and end of list */
	  case 1:
	    x++; /* one byte options */
	    break;
	  case 2: /* 11 byte security option */
	    x+=11;
	    break;
	  case 8:
	    x+=4; /* stream id option */
	    break;
	  case 3:
	  case 9:
	  case 7:
	  case 4:
	    if (x+1>=s->length) return 0; /* run out of data no match */
	    len=s->data[x+1];
	    x+=len;
	    break;
	  default:
	    return 0; /* don't undestand the option */
	}
	if (x>=s->length) return 0; /* run out of data no match */
    }
		
    return 0; /* should never be reached */
}
