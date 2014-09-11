#include "rts_udaf.h"
#include "gsconfig.h"
#include "gstypes.h"
#include <stdio.h>
#include <limits.h>
#include <math.h>

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


#define max(a,b) ((a) > (b) ? (a) : (b))
#define MAX_BUFSIZE     128


/****************************************************************/
/* LFTA functions                                               */
/****************************************************************/


////////////////////////////////////////////////////////////////////////
////		avg_udaf

typedef struct avg_udaf_lfta_struct_t{
	gs_int64_t sum;
	gs_uint32_t cnt;
} avg_udaf_lfta_struct_t;

void avg_udaf_lfta_LFTA_AGGR_INIT_(gs_sp_t b){
	avg_udaf_lfta_struct_t *s = (avg_udaf_lfta_struct_t *)b;
	s->sum = 0;
	s->cnt = 0;
}

void avg_udaf_lfta_LFTA_AGGR_UPDATE_(gs_sp_t  b,gs_uint32_t v){
	avg_udaf_lfta_struct_t *s = (avg_udaf_lfta_struct_t *)b;
	s->sum += v;
	s->cnt++;
}

gs_retval_t avg_udaf_lfta_LFTA_AGGR_FLUSHME_(gs_sp_t b){
	return 0;
}

void avg_udaf_lfta_LFTA_AGGR_OUTPUT_(struct string *r,gs_sp_t b){
	r->length = 12;
	r->data = b;
}

void avg_udaf_lfta_LFTA_AGGR_DESTROY_(gs_sp_t b){
	return;
}

/////////////////////////////////////////////////////////
//		Moving sum

typedef struct moving_sum_lfta_struct{
	gs_uint32_t sum;
	gs_uint32_t N;
} moving_sum_lfta_struct;

void moving_sum_lfta_LFTA_AGGR_INIT_(gs_sp_t b){
	moving_sum_lfta_struct *s = (moving_sum_lfta_struct *)b;
	s->sum = 0;
	s->N = 0;
}

void moving_sum_lfta_LFTA_AGGR_UPDATE_(gs_sp_t b, gs_uint32_t v, gs_uint32_t N){
	moving_sum_lfta_struct *s = (moving_sum_lfta_struct *)b;
	s->sum += v;
	s->N = N;
}
	
gs_retval_t moving_sum_lfta_LFTA_AGGR_FLUSHME_(gs_sp_t b){
	return 0;
}

void moving_sum_lfta_LFTA_AGGR_OUTPUT_(gs_uint64_t *r, gs_sp_t b){
	moving_sum_lfta_struct *s = (moving_sum_lfta_struct *)b;
	*r = ((gs_uint64_t)(s->N) << 32) | (gs_uint64_t)(s->sum);
}

gs_retval_t moving_sum_lfta_LFTA_AGGR_DESTROY_(gs_sp_t b){
	return 0;
}

