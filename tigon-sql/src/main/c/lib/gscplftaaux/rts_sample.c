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


//#define FILTERDEBUG 100000
#ifdef FILTERDEBUG
#include "stdio.h"
#include "stdlib.h"
#endif

#define OFFSET 17
#define PRIME 2997913


static gs_uint32_t rval;

#define MAX_TRIGGER 16


void set_seed(gs_uint32_t s){rval=s;}

gs_uint32_t lc_rand(){
		rval = (rval+OFFSET)*PRIME;
			return(rval & 0xffffffff);
}



