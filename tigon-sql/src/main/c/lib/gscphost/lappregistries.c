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

#include <lappregistries.h>
#include <stdio.h>
#include <stdlib.h>

/* datastructure to keep track of streamids used by the lapp */
struct streamregistry {
    gs_int32_t used;
    FTAID ftaid;
    struct ringbuf * r;
};

struct streamregistry * sreg =0;
gs_int32_t lsreg=0;

struct ringbuf ** rbr=0;
gs_int32_t lrbr=0;

FTAID ** rms=0;
gs_int32_t lrms=0;

/* adds the remote streamid with its associated msgid and ringbuf
 */
gs_retval_t streamregistry_add(FTAID ftaid,struct ringbuf * r)
{
    gs_int32_t x;
    if (lsreg == 0) {
        if ((sreg = malloc(sizeof(struct streamregistry)*STARTSZ))==0) {
            gslog(LOG_EMERG,"ERROR:Out of memory streanregistry\n");
            return -1;
        }
        memset(sreg,0,sizeof(struct streamregistry)*STARTSZ);
        lsreg = STARTSZ;
    }
    for(x=0;(x<lsreg)&&(sreg[x].used!=0);x++);
    if (x == lsreg) {
        gs_int32_t y;
        lsreg = 2*lsreg;
        if ((sreg = realloc(sreg,lsreg*sizeof(struct streamregistry)))==0) {
            gslog(LOG_EMERG,"ERROR:Out of memory streamregistry\n");
            return -1;
        }
        for (y=x;y<lsreg;y++)
            sreg[y].used=0;
    }
    sreg[x].ftaid=ftaid;
    sreg[x].r=r;
    sreg[x].used=1;
    return 0;
}

/* removes streamid from registry for specific msgid and ringbuf */
void streamregistry_remove(FTAID ftaid)
{
    gs_int32_t x;
    for(x=0;x<lsreg;x++) {
        if ((sreg[x].ftaid.ip==ftaid.ip)
            && (sreg[x].ftaid.port==ftaid.port)
            && (sreg[x].ftaid.index==ftaid.index)
            && (sreg[x].ftaid.streamid==ftaid.streamid)) {
            sreg[x].used=0;
        }
    }
    return;
}



/* the following two functions are used to cycle
 through all ringbuffers
 */
gs_retval_t streamregistry_getactiveringbuf_reset()
{
    gs_int32_t x,y;
    /* XXXOS this is not the most effective way of doing
     this needs improvment. */
    /* Build a list of all ringbufs make sure they
     are unique since multiple entrys could share
     a ringbuf
     */
    if (rbr!=0) (free(rbr));
    if ((rbr=malloc(sizeof(struct ringbuf *)*lsreg))==0) {
        gslog(LOG_EMERG,"Can't allocate memory in ftaregistry\n");
        return -1;
    }
    memset(rbr,0,sizeof(struct ringbuf *)*lsreg);
    lrbr=0;
    for(x=0;x<lsreg;x++) {
        if (sreg[x].used) {
            for(y=0;(y<lrbr)&&(rbr[y]!=sreg[x].r);y++);
            if (y>=lrbr) {
                rbr[y]=sreg[x].r;
                lrbr++;
            }
        }
    }
    return 0;
}

struct ringbuf * streamregistry_getactiveringbuf()
{
    if (lrbr>0) {
        lrbr --;
        return rbr[lrbr];
    }
    return 0;
}


gs_retval_t streamregistry_getactiveftaid_reset()
{
    gs_int32_t x,y;
    /* XXXOS this is not the most effective way of doing
     this needs improvment. */
    /* Build a list of at least one ftaid per process
     */
    if (rms!=0) (free(rms));
    if ((rms=malloc(sizeof(FTAID *)*lsreg))==0) {
        gslog(LOG_EMERG,"Can't allocate memory in ftaregistry\n");
        return -1;
    }
    memset(rms,0,sizeof(gs_int32_t *)*lsreg);
    lrms=0;
    for(x=0;x<lsreg;x++) {
        if (sreg[x].used) {
            for(y=0;(y<lrms)
                &&(rms[y]->ip!=sreg[x].ftaid.ip)
                &&(rms[y]->port!=sreg[x].ftaid.port);y++);
            if (y>=lrms) {
                rms[y]=&(sreg[x].ftaid);
                lrms++;
            }
        }
    }
    return 0;
}

FTAID * streamregistry_getactiveftaid()
{
    if (lrms>0) {
        lrms --;
        return rms[lrms];
    }
    return 0;    
}
