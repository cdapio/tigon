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
#include "lapp.h"
#include <ipcencoding.h>
#include <callbackregistries.h>
#include "fta.h"
#include "stdio.h"
#include "stdlib.h"
#include "rts.h"

#define  POLLING

static struct ringbuf * ru=0;

gs_retval_t print_error(gs_sp_t c) {
    gslog(LOG_EMERG,"%s",c);
    return 0;
}

void *fta_alloc(struct FTA * owner, gs_int32_t size)
{
    gs_uint8_t * c;
    gs_uint32_t x;
    if ((c=(gs_uint8_t *)malloc(size))==0) return 0;
    /* touch all memory once to map/reserve it now */
    for(x=0;x<size;x=x+1024) {
        c[x]=0;
    }
    
    return (void *) c;
}

void fta_free(struct FTA * owner , void * mem) {
    free(mem);
}


void fta_free_all(struct FTA * owner) {
    gslog(LOG_ERR,"fta_free_all not available ");
}

/* It is assumed that there is no write activity on all ringbuffers between an alloccate_tuple and
 * a post_tuple. If there is any the result is unpredictable
 */

void * allocate_tuple(struct FTA * owner, gs_int32_t size)
{
    gs_int32_t state;
    if (ru!=0) {
        gslog(LOG_ALERT,"Can't allocate multiple tuples at the same time before posting them");
        return 0;
    }
    
    if (size>MAXTUPLESZ) {
        gslog(LOG_ALERT,"Maximum tuple size is %u",MAXTUPLESZ);
        ru=0;
        return 0;
    }
    
    if (ftacallback_start_streamid(owner->ftaid.streamid)<0) {
        gslog(LOG_ALERT,"Post for unkown streamid\n");
        ru=0;
        return 0;
    }
    
    /* we grep memory in the first none suspended ringbuffer. Note that if there is no such ringbuffer we might
     not find any memory*/
    while ((ru=ftacallback_next_streamid(&state))!=0) {
#ifdef PRINTMSG
        fprintf(stderr,"Allocating in ringpuffer %p [%p:%u]"
                "(%u %u %u) \n",ru,&ru->start,ru->end,ru->reader,ru->writer,
                ru->length);
        fprintf(stderr,"Pointer to current writer %p\n",CURWRITE(ru));
#endif
        if (state != LFTA_RINGBUF_SUSPEND) {
#ifdef BLOCKRINGBUFFER
            if (state == LFTA_RINGBUF_ATOMIC) {
                while (!SPACETOWRITE(ru)) {
                    usleep(100);
                }
            }
#endif
            if (SPACETOWRITE(ru)) {
                CURWRITE(ru)->f=owner->ftaid;
                CURWRITE(ru)->sz=size;
                return &(CURWRITE(ru)->data[0]);
            } else {
                shared_memory_full_warning++;
            }
        }
    }
    ru=0;
    outtupledrop++;
  	
    return 0;
}

void free_tuple(void * data) {
    ru=0;
}

gs_retval_t post_tuple(void * tuple) {
    struct ringbuf * r;
    FTAID * ftaidp;
    gs_uint32_t stream_id;
    struct wakeup_result a;
    gs_int32_t state;
    
    if (ru==0) {
        gslog(LOG_ALERT,"lfta post tuple posted tupple was never allocated\n");
        return -1;
    }
    
    
    if (tuple != ((void*) &(CURWRITE(ru)->data[0]))) {
        gslog(LOG_ALERT,"lfta post tuple posted tupple which was not allocated"
              "immediatly before\n");
        ru=0;
        return -1;
    }
    
    stream_id=CURWRITE(ru)->f.streamid;
    
    if (ftacallback_start_streamid(stream_id)<0) {
        gslog(LOG_ALERT,"ERROR:Post for unkown streamid\n");
        ru=0;
        return -1;
    }
    /* now make sure we have space to write in all atomic ringbuffer */
    while((r=ftacallback_next_streamid(&state))!=0) {
        if ((state == LFTA_RINGBUF_ATOMIC) && (r!=ru)) {
#ifdef BLOCKRINGBUFFER
            while (!SPACETOWRITE(r)) {
                usleep(10000);
            }
#endif
            if (! SPACETOWRITE(r)) {
                /* atomic ring buffer and no space so post nothing */
				outtupledrop++;
				ru=0;
				return -1;
            }
        }
    }
    
    if (ftacallback_start_streamid(stream_id)<0) {
        gslog(LOG_ALERT,"Post for unkown streamid\n");
        ru=0;
        return -1;
    }
    
    while((r=ftacallback_next_streamid(&state))!=0) {
#ifdef PRINTMSG
        fprintf(stderr,"Found additional ring buffer make a copy to rb%p\n",
                r);
#endif
        /* try to post in all none suspended ringbuffer for atomic once
         * we know we will succeed
         */
        if ((r!=ru)&&(state != LFTA_RINGBUF_SUSPEND)) {
            if (SPACETOWRITE(r)) {
                CURWRITE(r)->f=CURWRITE(ru)->f;
                CURWRITE(r)->sz=CURWRITE(ru)->sz;
                memcpy(&(CURWRITE(r)->data[0]),&(CURWRITE(ru)->data[0]),
                       CURWRITE(ru)->sz);
                outtuple++;
                outbytes=outbytes+CURWRITE(ru)->sz;
                ADVANCEWRITE(r);
#ifdef PRINTMSG
                fprintf(stderr,"Wrote in ringpuffer %p [%p:%u]"
                        "(%u %u %u) \n",r,&r->start,r->end,r->reader,r->writer,
                        r->length);
                fprintf(stderr,"\t%u %u %u\n",CURREAD(r)->next,
                        CURREAD(r)->f.streamid,CURREAD(r)->sz);
#endif
            } else {
                outtupledrop++;
            }
        }
        if (HOWFULL(r) > 500) {
            // buffer is at least half full
            shared_memory_full_warning++;
#ifdef PRINTMSG
            fprintf(stderr,"\t\t buffer full\n");
#endif
        }
    }
    
    if (HOWFULL(ru) > 500) {
        // buffer is at least half full
        shared_memory_full_warning++;
#ifdef PRINTMSG
        fprintf(stderr,"\t\t buffer full\n");
#endif
    }
    outtuple++;
    outbytes=outbytes+CURWRITE(ru)->sz;
    ADVANCEWRITE(ru);
    ru=0;
#ifndef POLLING
    if (ftacallback_start_wakeup(stream_id)<0) {
        gslog(LOG_ALERT,"Wakeup for unkown streamid\n");
        return -1;
    }
    a.h.callid=WAKEUP;
    a.h.size=sizeof(struct wakeup_result);
    while((ftaidp=ftacallback_next_wakeup())!=0) {
        if (send_wakeup(*ftaidp)<0) {
            gslog(LOG_ALERT,"Could not send wakeup\n");
            return -1;
        }
    }
#endif
    return 0;
}

gs_retval_t get_ringbuf_space(struct FTA * f, FTAID * r, gs_int32_t* space, gs_int32_t szr, gs_int32_t tuplesz)
{
    gs_int32_t x=0;
    gs_int32_t state;
    struct ringbuf * ru;
    if (ftacallback_start_streamid(f->ftaid.streamid)<0) {
        gslog(LOG_ALERT,"Space check for unkown streamid\n");
        return -1;
    }
    
    while ((ru=ftacallback_next_streamid(&state))!=0) {
        if (szr > x ) {
            r[x]=ru->destid;
            space[x]=TUPLEFIT(ru,tuplesz);
        }
        x++;
    }
    return x;
}

gs_retval_t set_ringbuf_type(struct FTA * f, FTAID process, gs_int32_t state)
{
    
    if (ftacallback_state_streamid(f->ftaid.streamid,process, state)<0) {
		gslog(LOG_ALERT,"state change for unkown streamid\n");
		return -1;
    }
    return 0;
}


gs_retval_t tpost_ready() {
    return 1;
}

