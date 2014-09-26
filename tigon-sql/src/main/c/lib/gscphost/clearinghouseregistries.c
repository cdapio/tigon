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
#include <lapp.h>
#include <clearinghouseregistries.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "gshub.h"

extern const gs_sp_t fta_names[];


/* fta lookup registry in the clearinghouse */


struct ftalookup {
    gs_int32_t used;
    gs_sp_t name;
    FTAID ftaid;
    gs_uint32_t  reusable;
    gs_sp_t  schema;
};

struct ftalookup * flookup =0;
gs_int32_t llookup=0;


/* Adds a FTA to the lookup table */
gs_retval_t ftalookup_register_fta( FTAID subscriber, FTAID f,
                                   FTAname name, gs_uint32_t  reusable,
                                   gs_csp_t  schema)
{
    gs_int32_t x;
    static gs_int32_t registered_ftas=0;
    endpoint gshub;
    if (get_hub(&gshub)!=0) {
        gslog(LOG_EMERG,"ERROR:could not find gshub to announce fta/instance");
        return -1;
    }
    if (llookup == 0) {
        if ((flookup = malloc(sizeof(struct ftalookup)*STARTSZ))==0) {
            gslog(LOG_EMERG,"ERROR:Out of memory ftalookup\n");
            return -1;
        }
        memset(flookup,0,sizeof(struct ftalookup)*STARTSZ);
        llookup = STARTSZ;
    }
    for(x=0;(x<llookup)&&(flookup[x].used!=0);x++);
    if (x == llookup) {
        gs_int32_t y;
        llookup = 2*llookup;
        if ((flookup = realloc(flookup,llookup*sizeof(struct ftalookup)))==0) {
            gslog(LOG_EMERG,"ERROR:Out of memory ftacallback\n");
            return -1;
        }
        for (y=x;y<llookup;y++)
            flookup[y].used=0;
    }
    if (f.streamid==0) {
        gslog(LOG_INFO,"Basic FTA can not be reusable\n");
        reusable=0;
        if (registered_ftas>=0)
            registered_ftas++; // count them to know when everybody is in
    } else {
        // register none basic FTA's with GSHUB
        if (set_ftainstance(gshub,get_instance_name(),(gs_sp_t)name,&f)!=0) {
            gslog(LOG_EMERG,"ERROR:could not set_ftainstance");
            return -1;
        }
    }
    gslog(LOG_INFO,"Adding fta to registry %s reuseable %u\n",name,reusable);
    flookup[x].name=strdup(name);
    flookup[x].ftaid=f;
    flookup[x].reusable=reusable;
    flookup[x].schema=strdup(schema);
    flookup[x].used=1;

    if (registered_ftas>=0) {
        for(x=0; fta_names[x]!=0;x++ );
        if (x<=registered_ftas) {
            if (set_initinstance(gshub,get_instance_name())!=0) {
                gslog(LOG_EMERG,"hostlib::error::could not init instance");
                return -1;
            }
            registered_ftas=-1;
        }
    }
    return 0;
}

/* Removes the FTA from the lookup table */
gs_retval_t ftalookup_unregister_fta(FTAID subscriber,FTAID f)
{
    gs_int32_t x;
    for(x=0;x<llookup;x++) {
        if ((flookup[x].used==1)
            && (flookup[x].ftaid.streamid)
            && (flookup[x].ftaid.ip)
            && (flookup[x].ftaid.port)
            && (flookup[x].ftaid.index)) {
            flookup[x].used=0;
            free(flookup[x].name);
            free(flookup[x].schema);
        }
    }
    return 0;
}

/* Looks an FTA up by name */
gs_retval_t ftalookup_lookup_fta_index(FTAID caller,
                                       FTAname name, gs_uint32_t  reuse, FTAID * ftaid,
                                       gs_csp_t  * schema)
{
    gs_int32_t x;
#ifdef PRINTMSG
    fprintf(stderr,"Name %s reusable %u\n",name,reuse);
#endif
    if (reuse==1) {
        /* grep the firs reusable instance */
        for(x=0;x<llookup;x++) {
            if ((flookup[x].used==1)&&(strcmp(flookup[x].name,name)==0)
                && (flookup[x].reusable>=1)
                && (flookup[x].ftaid.streamid!=0)) {
                *ftaid=flookup[x].ftaid;
                *schema=flookup[x].schema;
#ifdef PRINTMSG
                fprintf(stderr,"\tREUSE FTA\n");
#endif
                return 0;
            }
        }
    }
    /* grep the first fta with the name not an instance */
    for(x=0;x<llookup;x++) {
        if ((flookup[x].used==1)&&(strcmp(flookup[x].name,name)==0)
            && (flookup[x].ftaid.streamid==0)) {
            *ftaid=flookup[x].ftaid;
            *schema=flookup[x].schema;
#ifdef PRINTMSG
            fprintf(stderr,"\tNEW FTA\n");
#endif

            gslog(LOG_DEBUG,"Lookup of FTA %s with FTAID {ip=%u,port=%u,index=%u,streamid=%u}\n",name,ftaid->ip,ftaid->port,ftaid->index,ftaid->streamid);
            return 0;
        }
    }
#ifdef PRINTMSG
    fprintf(stderr,"NO MATCH\n");
#endif
    return -1;
}

gs_retval_t ftalookup_producer_failure(FTAID caller,FTAID producer) {
    return 0;
}

gs_retval_t ftalookup_heartbeat(FTAID caller_id, gs_uint64_t trace_id,
                                gs_uint32_t  sz, fta_stat * trace){

	gs_uint32_t i = 0;
    endpoint gshub;
    if (get_hub(&gshub)!=0) {
        gslog(LOG_EMERG,"ERROR:could not find gshub to announce fta");
        return -1;
    }

    // to avoid sending redundant FTA instance stats to GSHUB we will only send statistics that have trace size of 1
	// for application heartbeats (streamid=0) we will only send last stat in their traces
    if ((sz == 1) || (trace[sz-1].ftaid.streamid == 0)) {
        if (set_instancestats(gshub,get_instance_name(),&trace[sz-1])!=0) {
	        gslog(LOG_EMERG,"ERROR:could not set instancestats");
	        return -1;
		}

    }

	gslog(LOG_DEBUG,"Heartbeat trace from FTA {ip=%u,port=%u,index=%u,streamid=%u}, trace_id=%llu ntrace=%d\n", caller_id.ip,caller_id.port,caller_id.index,caller_id.streamid, trace_id,sz);
	for (i = 0; i < sz; ++i) {
		gslog(LOG_DEBUG,"trace_id=%llu, trace[%u].ftaid={ip=%u,port=%u,index=%u,streamid=%u}, fta_stat={in_tuple_cnt=%u,out_tuple_cnt=%u,out_tuple_sz=%u,accepted_tuple_cnt=%u,cycle_cnt=%llu,collision_cnt=%u,eviction_cnt=%u,sampling_rate=%f}\n", trace_id, i,
              trace[i].ftaid.ip,trace[i].ftaid.port,trace[i].ftaid.index,trace[i].ftaid.streamid,
              trace[i].in_tuple_cnt, trace[i].out_tuple_cnt, trace[i].out_tuple_sz, trace[i].accepted_tuple_cnt, trace[i].cycle_cnt, trace[i].collision_cnt, trace[i].eviction_cnt, trace[i].sampling_rate);
	}
    return 0;
}
