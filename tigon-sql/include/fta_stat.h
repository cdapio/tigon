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

#ifndef FTA_STAT_H
#define FTA_STAT_H

/* statiss are collected by FTAs and regularly sent to cluster manager (currently every 1 sec)
 * Statistics refers to time interval since the last time statistics was reported */
typedef struct fta_stat {
   FTAID ftaid;
   gs_uint32_t in_tuple_cnt;		/* # of tuples received */
   gs_uint32_t out_tuple_cnt;		/* # of tuples produced */
   gs_uint32_t out_tuple_sz;		/* bytes in produced tuples */
   gs_uint32_t accepted_tuple_cnt;	/* # of tuples that passed initial unpacking and selection predicate */
   gs_uint64_t cycle_cnt;			/* # of cpu cycles spent in accept_tuple */
   gs_uint32_t collision_cnt;		/* # of collisions in an aggregation hash table */
   gs_uint32_t eviction_cnt;		/* # of collisions in an aggregation hash table */
   gs_float_t sampling_rate;			/* sampling rate used by fta */

  /* ...will add more fields in the future... */
} fta_stat;

#endif	/* FTA_STAT_H */
