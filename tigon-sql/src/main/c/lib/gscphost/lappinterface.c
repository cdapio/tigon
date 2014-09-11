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
#include <gscpipc.h>
#include <ipcencoding.h>
#include <stdlib.h>
#include <stdio.h>
#include <lapp.h>
#include <lappregistries.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/mman.h>
#include "rdtsc.h"
// If POLLING is defined applications poll every 100 msec instead of blocking
#define POLLING

struct processtate curprocess = {0,0,0,255,0};
struct FTAID clearinghouseftaid = {0,0,0,0};

/*
 * sends the message passed in buf and waits for a result
 * if a message returned is not a result it is put in the
 * request queue. The resultsbuf has to be large enough
 * for the largest result
 */
gs_retval_t ipc_call_and_wait(FTAID f, gs_sp_t  msg, gs_sp_t  result)
{
    struct hostcall * h = (struct hostcall *) msg;
    gs_int8_t  buf[MAXMSGSZ];
    FTAID from;
    gs_int32_t length;
    gs_int32_t lowop;
#ifdef PRINTMSG
    fprintf(stderr, "HOST sending to %u.%u.%u.%u:%u of "
            "type %u with length %u\n",
            (f.ip>>24)&0xff,
            (f.ip>>16)&0xff,
            (f.ip>>8)&0xff,
            (f.ip)&0xff,
            f.port,h->callid,h->size);
#endif
    if (gscpipc_send(f,FTACALLBACK,msg,h->size,1)<0) {
        gslog(LOG_EMERG,"ERROR:Could not send on message queue\n");
        return -1;
    }
    h=(struct hostcall *) buf;
    while (gscpipc_read(&from,&lowop,buf,&length,1)>0) {
#ifdef PRINTMSG
        fprintf(stderr, "HOST response from %u.%u.%u.%u:%u"
                " of type %u with length %u\n",
                (from.ip>>24)&0xff,
                (from.ip>>16)&0xff,
                (from.ip>>8)&0xff,
                (from.ip)&0xff,
                from.port,
                h->callid,h->size);
#endif
        if ((lowop == FTACALLBACK) && (h->callid < RESULT_OPCODE_IGNORE)) {
            h=(struct hostcall *) buf;
            if (h->callid > RESULT_OPCODE_BASE) {
                memcpy(result,buf,length);
                return 0;
            }
            if (sidequeue_append(from,buf,length)<0) {
                gslog(LOG_EMERG,"ERROR:: Could not add to sidequeue\n");
                return -1;
            }
        }
    }
    gslog(LOG_EMERG, "ERROR::gscipc_read failed in ipc_call_and_wait\n");
    return -1;
}


gs_retval_t hostlib_init(gs_int32_t type, gs_int32_t buffersize, gs_int32_t deviceid, gs_int32_t mapcnt, gs_sp_t map[])
{
    FILE * f;
    
    if (curprocess.active != 0 ) {
        return -1;
    }
    
    switch (type) {
        case CLEARINGHOUSE:
            if (gscpipc_init(1) < 0) {
                gslog(LOG_EMERG,"ERROR:Could not initalize comm layer for "
                      "clearinghouse process\n");
                return -1;
            }
            break;
        case LFTA:
#ifdef __linux__
            mlockall(MCL_CURRENT|MCL_FUTURE);
#endif
        case APP:
        case HFTA:
            if (gscpipc_init(0) < 0) {
                gslog(LOG_EMERG,"ERROR:Could not initalize comm layer for "
                      "non clearinghouse process\n");
                return -1;
            }
            break;
        default:
            gslog(LOG_EMERG,"ERROR:Unknown process type\n");
            return -1;
    }
    
    // if the buffersize is zero then allocating shared memory
    // will fail. So only use it for the clearinghouse and LFTAs
    if ((buffersize<(4*MAXTUPLESZ)) && (buffersize!=0)) {
        gslog(LOG_EMERG,
              "ERROR:buffersize in hostlib_init has to "
              "be at least %u Bytes long\n",
              4*MAXTUPLESZ);
        return -1;
    }
    
    curprocess.type=type;
    curprocess.buffersize=buffersize;
    curprocess.active = 1;
    curprocess.deviceid=deviceid;
    curprocess.mapcnt=mapcnt;
    curprocess.map=map;
    return 0;
}

void hostlib_free()
{
    if (curprocess.active != 1 ) {
        return;
    }
    curprocess.active = 0;
    gscpipc_free();
}


gs_retval_t fta_find(FTAname name, gs_uint32_t  reuse, FTAID *ftaid,
                     gs_sp_t  schema, gs_int32_t schemasz)
{
    gs_int8_t  rb[MAXRES];
    struct fta_find_arg a;
    struct ftafind_result * sr = (struct ftafind_result *)rb;
    
    a.h.callid = FTA_LOOKUP;
    a.h.size = sizeof(struct fta_find_arg);
    a.reuse=reuse;
    if (strlen(name)>=(MAXFTANAME-1)) {
        gslog(LOG_EMERG,"ERROR:FTA name (%s) to large\n",name);
        return -1;
    }
    strcpy(a.name,name);
    ipc_call_and_wait(clearinghouseftaid,(gs_sp_t )&a,rb);
    if (sr->h.callid != FTAFIND_RESULT) {
        gslog(LOG_EMERG,"ERROR:Wrong result code received in fta_find\n");
        return -1;
    }
    if (sr->result >= 0) {
        if (schema !=0) {
            if (strlen(sr->schema) >= schemasz) {
                gslog(LOG_EMERG,"Could not fit schema into schema buffer fta_find\n");
                return -1;
            } else {
                strcpy(schema,sr->schema);
            }
        }
        *ftaid=sr->f;
    }
    return sr->result;
}

gs_retval_t fta_alloc_instance(FTAID subscriber,
                               FTAID * ftaid, FTAname name, gs_sp_t schema,
                               gs_uint32_t  reusable,
                               gs_int32_t command, gs_int32_t sz, void *  data)
{
    gs_int8_t  rb[MAXRES];
    struct fta_alloc_instance_arg * a;
    struct fta_result * fr = (struct fta_result *)rb;
    struct ringbuf *r;
    
    /* make sure we have the share memory required */
    if ((r=gscpipc_createshm(*ftaid,curprocess.buffersize))==0) {
        gslog(LOG_EMERG,"ERROR:could not allocate shared memory"
              "for FTA %s\n",name);
        return -1;
    }
    
    if (strlen(name)>=(MAXFTANAME-1)) {
        gslog(LOG_EMERG,"ERROR:FTA name (%s) to large\n",name);
        return -1;
    }
    
    if (strlen(schema)>=(MAXSCHEMASZ-1)) {
        gslog(LOG_EMERG,"ERROR:FTA name (%s) to large\n",name);
        return -1;
    }
    
    a = alloca(sizeof(struct fta_alloc_instance_arg) + sz);
    
    a->h.callid = FTA_ALLOC_INSTANCE;
    a->h.size = sizeof(struct fta_alloc_instance_arg)+ sz;
    a->f=*ftaid;
    a->subscriber=subscriber;
    a->reusable=reusable;
    a->command = command;
    a->sz = sz;
    memcpy(&a->data[0],data,sz);
    strcpy(a->name,name);
    strcpy(a->schema,schema);
    
    ipc_call_and_wait(*ftaid,(gs_sp_t )a,rb);
    
    if (fr->h.callid != FTA_RESULT) {
        gslog(LOG_EMERG,"ERROR:Wrong result code received\n");
        return -1;
    }
    
    *ftaid=fr->f;
    
    if (fr->result==0) {
        gslog(LOG_INFO,"Allocated fta instance %s with FTAID {ip=%u,port=%u,index=%u,streamid=%u}\n",name,ftaid->ip,ftaid->port,ftaid->index,ftaid->streamid);
        return streamregistry_add(*ftaid,r);
    }
    
    return fr->result;
}

gs_retval_t fta_alloc_print_instance(FTAID subscriber,
                                     FTAID * ftaid,
                                     FTAname name, gs_sp_t schema, gs_uint32_t  reusable,
                                     gs_int32_t command, gs_int32_t sz, void *  data,
                                     gs_sp_t  path,gs_sp_t  basename,
                                     gs_sp_t  temporal_field, gs_sp_t  split_field,
                                     gs_uint32_t  delta, gs_uint32_t  split)
{
    gs_int8_t  rb[MAXRES];
    struct fta_alloc_instance_arg * a;
    struct fta_result * fr = (struct fta_result *)rb;
    
    if ((strlen(path)>=MAXPRINTSTRING-1)
        || (strlen(basename)>=MAXPRINTSTRING-1)
        || (strlen(temporal_field)>=MAXPRINTSTRING-1)) {
        gslog(LOG_EMERG,"INTERNAL ERROR:fta_alloc_print_instance string"
              " arguments to long\n");
        return -1;
    }
    if (strlen(name)>=(MAXFTANAME-1)) {
        gslog(LOG_EMERG,"ERROR:FTA name (%s) to large\n",name);
        return -1;
    }
    
    if (strlen(schema)>=(MAXSCHEMASZ-1)) {
        gslog(LOG_EMERG,"ERROR:FTA name (%s) to large\n",name);
        return -1;
    }
    
    a = alloca(sizeof(struct fta_alloc_instance_arg) + sz);
    
    a->h.callid = FTA_ALLOC_PRINT_INSTANCE;
    a->h.size = sizeof(struct fta_alloc_instance_arg)+ sz;
    a->f=*ftaid;
    a->subscriber=subscriber;
    a->reusable=reusable;
    a->split=split;
    strcpy(a->name,name);
    strcpy(a->schema,schema);
    a->command = command;
    a->sz = sz;
    strcpy(a->path,path);
    strcpy(a->basename,basename);
    strcpy(a->temporal_field,temporal_field);
    strcpy(a->split_field,split_field);
    a->delta=delta;
    memcpy(&a->data[0],data,sz);
    
    ipc_call_and_wait(*ftaid,(gs_sp_t )a,rb);
    
    if (fr->h.callid != FTA_RESULT) {
        gslog(LOG_EMERG,"ERROR:Wrong result code received\n");
        return -1;
    }
    
    *ftaid=fr->f;
    
    return fr->result;
}

gs_retval_t fta_free_instance(FTAID subscriber, FTAID ftaid, gs_uint32_t  recursive)
{
    gs_int8_t  rb[MAXRES];
    struct fta_free_instance_arg a;
    struct standard_result * sr = (struct standard_result *)rb;
    struct ringbuf *r;
    
    a.h.callid = FTA_FREE_INSTANCE;
    a.h.size = sizeof(struct fta_free_instance_arg);
    a.subscriber=subscriber;
    a.f=ftaid;
    a.recursive=recursive;
    ipc_call_and_wait(ftaid,(gs_sp_t )&a,rb);
    if (sr->h.callid != STANDARD_RESULT) {
        gslog(LOG_EMERG,"ERROR:Wrong result code received\n");
        return -1;
    }
    
    /* make sure we remove the mapping*/
    streamregistry_remove(ftaid);
    
    return sr->result;
}

gs_retval_t fta_control(FTAID subscriber,
                        FTAID ftaid, gs_int32_t command, gs_int32_t sz, void * value)
{
    gs_int8_t  rb[MAXRES];
    struct fta_control_arg * a;
    struct standard_result * sr = (struct standard_result *)rb;
    
    a = alloca(sizeof(struct fta_control_arg) + sz);
    
    a->h.callid = FTA_CONTROL;
    a->h.size = sizeof(struct fta_control_arg)+ sz;
    a->subscriber=subscriber;
    a->f=ftaid;
    a->command = command;
    a->sz = sz;
    memcpy(&a->data[0],value,sz);
    
    ipc_call_and_wait(ftaid,(gs_sp_t )a,rb);
    
    if (sr->h.callid != STANDARD_RESULT) {
        gslog(LOG_EMERG,"ERROR:Wrong result code received\n");
        return -1;
    }
    
    return sr->result;
}

gs_retval_t fta_heartbeat(FTAID self,gs_uint64_t trace_id,
                          gs_uint32_t  sz, fta_stat * trace){
#ifdef CLEARINGHOUSE_HEARTBEAT
    struct fta_heartbeat_arg  * a;
    a = alloca(sizeof(struct fta_heartbeat_arg) + (sz*sizeof(fta_stat)));
    a->h.callid = FTA_HEARTBEAT;
    a->h.size = sizeof(struct fta_heartbeat_arg)+(sz*sizeof(fta_stat));
    a->sender=self;
    a->trace_id=trace_id;
    a->sz=sz;
    if (sz!=0) {
        memcpy(&a->data[0],trace,(sz*sizeof(fta_stat)));
    }
#ifdef PRINTMSG
    fprintf(stderr, "HOST sending heartbeat to %u.%u.%u.%u:%u of "
            "type %u with length %u\n",
            (clearinghouseftaid.ip>>24)&0xff,
            (clearinghouseftaid.ip>>16)&0xff,
            (clearinghouseftaid.ip>>8)&0xff,
            (clearinghouseftaid.ip)&0xff,
            clearinghouseftaid.port,a->h.callid,a->h.size);
#endif
    if (gscpipc_send(clearinghouseftaid,FTACALLBACK,(gs_sp_t)a,a->h.size,1)<0) {
        gslog(LOG_EMERG,"ERROR:Could not send on message queue\n");
        return -1;
    }
#endif
    return 0;
}

gs_retval_t fta_notify_producer_failure(FTAID self, FTAID producer){
    struct fta_notify_producer_failure_arg  a;
    a.h.callid = FTA_PRODUCER_FAILURE;
    a.h.size = sizeof(struct fta_notify_producer_failure_arg);
    a.sender=self;
    a.producer=producer;
#ifdef PRINTMSG
    fprintf(stderr, "HOST sending producer failure to %u.%u.%u.%u:%u of "
            "type %u with length %u\n",
            (clearinghouseftaid.ip>>24)&0xff,
            (clearinghouseftaid.ip>>16)&0xff,
            (clearinghouseftaid.ip>>8)&0xff,
            (clearinghouseftaid.ip)&0xff,
            clearinghouseftaid.port,a.h.callid,a.h.size);
#endif
    if (gscpipc_send(clearinghouseftaid,FTACALLBACK,(gs_sp_t)&a,a.h.size,1)<0) {
        gslog(LOG_EMERG,"ERROR:Could not send on message queue\n");
        return -1;
    }
    return 0;
}

gs_retval_t process_control(FTAID ftaid, gs_int32_t command, gs_int32_t sz, void * value)
{
    gs_int8_t  rb[MAXRES];
    struct process_control_arg * a;
    struct standard_result * sr = (struct standard_result *)rb;
    
    
    a = alloca(sizeof(struct process_control_arg) + sz);
    
    a->h.callid = PROCESS_CONTROL;
    a->h.size = sizeof(struct process_control_arg)+ sz;
    a->command = command;
    a->sz = sz;
    memcpy(&a->data[0],value,sz);
    
    ipc_call_and_wait(ftaid,(gs_sp_t )a,rb);
    
    if (sr->h.callid != STANDARD_RESULT) {
        gslog(LOG_EMERG,"ERROR:Wrong result code received\n");
        return -1;
    }
    
    return sr->result;
}


static void timeouthandler ()
{
    struct timeout_result a;
    
    a.h.callid=TIMEOUT;
    a.h.size=sizeof(struct timeout_result);
    if (gscpipc_send(gscpipc_getftaid(), FTACALLBACK, (gs_sp_t )&a,a.h.size,1)<0) {
        gslog(LOG_EMERG,"ERROR:Could not send on message queue\n");
    }
}

gs_retval_t gscp_get_buffer(FTAID * ftaid, gs_int32_t * size, void *tbuffer,
                            gs_int32_t tbuf_size, gs_int32_t timeout)
{
    struct ringbuf * r;
    FTAID from;
    gs_int32_t length;
    gs_int8_t  buf[MAXMSGSZ];
    gs_int32_t lopp;
    FTAID * f;
    static	gs_uint64_t s1=0;
    static	gs_uint64_t s2;
    if (s1==0) {
        s1=rdtsc();
    }
    s2=rdtsc();
    cycles+=(s2-s1);
start:
#ifdef PRINTMSG
    fprintf(stderr,"CHECK RINGBUFS\n");
#endif
#ifndef POLLING
    /* use chance to cleanout message queue no reason
     to keep anything else */
    while (gscpipc_read(&from,&lopp,buf,&length,0)>0);
#endif
    
    streamregistry_getactiveringbuf_reset();
    while ((r=streamregistry_getactiveringbuf())>0) {
#ifdef PRINTMSG
	    fprintf(stderr,"Reading from ringpuffer %p [%p:%u]"
                "(%u %u %u) \n",r,&r->start,r->end,r->reader,r->writer,
                r->length);
	    if (UNREAD(r)) {
            fprintf(stderr,"\t%u %u.%u.%u.%u:%u-%p %u\n",CURREAD(r)->next,
                    (CURREAD(r)->f.ip>>24)&0xff,
                    (CURREAD(r)->f.ip>>16)&0xff,
                    (CURREAD(r)->f.ip>>8)&0xff,
                    (CURREAD(r)->f.ip)&0xff,
                    CURREAD(r)->f.port,
                    CURREAD(r)->f.streamid,
                    CURREAD(r)->sz);
	    }
        
#endif
        if (UNREAD(r)) {
            *ftaid=(CURREAD(r)->f);
            *size=CURREAD(r)->sz;
            memcpy(tbuffer,&(CURREAD(r)->data[0]),(tbuf_size>(*size))?(*size):tbuf_size);
            intuple++;
            inbytes+=CURREAD(r)->sz;
            ADVANCEREAD(r);
            s1=rdtsc();
            return 0;
        }
    }
    if (timeout == -1) {
        *size=0;
        s1=rdtsc();
        return 1;
    }
    if (timeout !=0) {
        signal(SIGALRM, timeouthandler);
        alarm(timeout);
    }
    
#ifndef POLLING
#ifdef PRINTMSG
    fprintf(stderr,"START BLOCKCALLS\n");
#endif
    streamregistry_getactiveftaid_reset();
    while ((f=streamregistry_getactiveftaid())!=0) {
        struct gscp_get_buffer_arg a;
        a.h.callid = GSCP_GET_BUFFER;
        a.h.size = sizeof(struct gscp_get_buffer_arg);
        a.timeout = timeout;
#ifdef PRINTMSG
        fprintf(stderr,"Waiting for  %u.%u.%u.%u:%u\n",
                (f->ip>>24)&0xff,
                (f->ip>>16)&0xff,
                (f->ip>>8)&0xff,
                (f->ip)&0xff,
                f->port
                );
#endif
        if (gscpipc_send(*f,FTACALLBACK,(gs_sp_t )&a,a.h.size,1)<0) {
            s1=rdtsc();
            return -1;
        }
    }
#ifdef PRINTMSG
    fprintf(stderr,"BLOCK\n");
#endif
    while (gscpipc_read(&from,&lopp,buf,&length,1)>0) {
#else  // If we poll we return after 100 msec
    sleepagain:
        while (gscpipc_read(&from,&lopp,buf,&length,2)>0) {
#endif
            struct standard_result * sr = (struct standard_result *) buf;
#ifdef PRINTMSG
            fprintf(stderr,"Got return code %u\n",sr->h.callid);
#endif
            if (lopp==FTACALLBACK) {
                if (timeout != 0) {
                    signal(SIGALRM, SIG_IGN);
                }
                if (sr->h.callid == WAKEUP) {
                    /* use chance to cleanout message queue no reason
                     to keep anything else */
                    while (gscpipc_read(&from,&lopp,buf,&length,0)>0);
                    goto start;
                }
                if (sr->h.callid == TIMEOUT) {
                    /* use chance to cleanout message queue no reason
                     to keep anything else */
                    while (gscpipc_read(&from,&lopp,buf,&length,0)>0);
                    *size=0;
                    s1=rdtsc();
                    return 1;
                }
                if (sidequeue_append(from,buf,length)<0) {
                    gslog(LOG_EMERG,"ERROR:: Could not add to sidequeue\n");
                    s1=rdtsc();
                    return -1;
                }
            }
        }
#ifdef POLLING
        streamregistry_getactiveringbuf_reset();
        while ((r=streamregistry_getactiveringbuf())>0) {
#ifdef PRINTMSG
            fprintf(stderr,"Reading from ringpuffer %p [%p:%u]"
                    "(%u %u %u) \n",r,&r->start,r->end,r->reader,r->writer,
                    r->length);
            if (UNREAD(r)) {
                fprintf(stderr,"\t%u %u.%u.%u.%u:%u-%p %u\n",CURREAD(r)->next,
                        (CURREAD(r)->f.ip>>24)&0xff,
                        (CURREAD(r)->f.ip>>16)&0xff,
                        (CURREAD(r)->f.ip>>8)&0xff,
                        (CURREAD(r)->f.ip)&0xff,
                        CURREAD(r)->f.port,
                        CURREAD(r)->f.streamid,
                        CURREAD(r)->sz);
            }
            
#endif
            if (UNREAD(r)) {
                *ftaid=(CURREAD(r)->f);
                *size=CURREAD(r)->sz;
                memcpy(tbuffer,&(CURREAD(r)->data[0]),(tbuf_size>(*size))?(*size):tbuf_size);
                intuple++;
                inbytes+=CURREAD(r)->sz;
                ADVANCEREAD(r);
                if (timeout != 0) {
                    signal(SIGALRM, SIG_IGN);
                }
                s1=rdtsc();
                return 0;
            }
        }
        goto sleepagain; // Try again
#endif
        gslog(LOG_EMERG,"Unexpected code reached in: gscp_get_buffer \n");
        /* we should never get here */
        s1=rdtsc();
        return -1;
    }
    
    
    
