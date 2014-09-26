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

#include "callbackregistries.h"
#include "lapp.h"
#include "gscpipc.h"
#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include <unistd.h>
#include <signal.h>
#include <sys/mman.h>
#include "schemaparser.h"
#include "errno.h"

#include "gsconfig.h"
#include "gstypes.h"

struct ftacallbackalloc {
    gs_int32_t used;
    gs_sp_t name;
    alloc_fta fta_alloc_functionptr;
    gs_uint64_t prefilter;
};

static struct ftacallbackalloc * falloc =0;
static gs_int32_t lalloc=0;

struct ftacallbackstreamid {
    gs_int32_t refcnt;
    gs_uint32_t streamid;
    gs_uint32_t state;
    struct ringbuf * r;
};

static struct ftacallbackstreamid * fstreamid =0;
static gs_int32_t lstreamid=0;
static gs_uint32_t cstreamid;
static gs_int32_t nstreamid;

struct ftacallbackwakeup {
    gs_int32_t used;
    FTAID ftaid;
    struct ringbuf * r;
};

static struct ftacallbackwakeup * fwakeup =0;
static gs_int32_t lwakeup=0;
static gs_uint32_t swakeup;
static gs_int32_t nwakeup;

/* XXX memory of fta list has no owner struct */

struct fta_list {
    struct fta_list * prev;
    struct fta_list * next;
    struct FTA * fta;
};

static struct fta_list * process=0;
static struct fta_list * created=0;

static struct fta_list * itteration=0;

// Side queue datastructures

struct sq {
    FTAID from;
    gs_int8_t buf[MAXMSGSZ];
    gs_int32_t length;
    struct sq * next;
};

static struct sq * sqtop=0;
static struct sq * sqtail=0;


/* HFTA internal print function*/

gs_retval_t add_printfunction_to_stream( struct FTA  * ftaid, gs_sp_t schema, gs_sp_t path, gs_sp_t basename,
                                        gs_sp_t temporal_field, gs_sp_t split_field, gs_uint32_t delta, gs_uint32_t split) {
	gs_uint32_t parserversion;
	gs_int32_t schemaid;
	gs_uint32_t x;
	gs_int8_t temp[50000];
	if (ftaid->printfunc.in_use==1) {
        gslog(LOG_EMERG,"ERROR:Printfunction::only allow one print function per HFTA instance\n");
	    return -1;
	}
	ftaid->printfunc.path=strdup(path);
	ftaid->printfunc.basename=strdup(basename);
	ftaid->printfunc.nexttime=0;
	ftaid->printfunc.split=(split%1000);
	ftaid->printfunc.itt=(split/1000)%1000;
	ftaid->printfunc.base=(split/1000000)%1000;
	ftaid->printfunc.delta=delta;
	ftaid->printfunc.in_use=1;
	if (ftaid->printfunc.split > MAXPRINTFILES) {
        gslog(LOG_EMERG,"ERROR:Printfunction SPLIT to large\n");
	    return -1;
	}
	for (x=0;x<ftaid->printfunc.split;x++) ftaid->printfunc.fa[x]=0;
    
	if ((ftaid->printfunc.schemaid=ftaschema_parse_string(schema))<0) {
		gslog(LOG_EMERG,"ERROR:could not parse schema in HFTA print function");
		return -1;
	}
	if ((ftaid->printfunc.temporal_field=ftaschema_get_field_offset_by_name(
                                                                            ftaid->printfunc.schemaid,temporal_field))<0) {
		gslog(LOG_EMERG,"ERROR:could not get "
              "offset for timefield %s in HFTA print function\n",
              temporal_field);
		return -1;
	}
    
	if (ftaschema_get_field_type_by_name(
                                         ftaid->printfunc.schemaid,temporal_field)!=UINT_TYPE) {
	    gslog(LOG_EMERG,"ERROR: illegal type for timefield "
              "%s in HFTA print function UINT expected\n",
              temporal_field);
	    return -1;
	}
	if ((ftaid->printfunc.split_field=ftaschema_get_field_offset_by_name(
                                                                         ftaid->printfunc.schemaid,split_field))<0) {
		gslog(LOG_EMERG,"ERROR:could not get "
              "offset for splitfield %s in HFTA print function\n",
              split_field);
		return -1;
	}
    
	if (ftaschema_get_field_type_by_name(
                                         ftaid->printfunc.schemaid,split_field)!=UINT_TYPE) {
	    gslog(LOG_EMERG,"ERROR: illegal type for splitfield"
              "%s in HFTA print function UINT expected\n",
              split_field);
	    return -1;
	}
	parserversion=get_schemaparser_version();
	sprintf(temp,"GDAT\nVERSION:%u\nSCHEMALENGTH:%lu\n%s",parserversion,strlen(schema)+1,schema);
	ftaid->printfunc.header=strdup(temp);
 	gslog(LOG_INFO,"Established print function for %s",basename);
	return 0;
}

gs_retval_t print_stream(struct FTA * self, gs_int32_t sz, void *tuple)
{
	gs_int32_t problem;
	gs_uint32_t timeval;
	gs_uint32_t splitval;
	gs_uint32_t x;
	gs_uint32_t nsz;
	timeval=fta_unpack_uint(tuple,sz,self->printfunc.temporal_field,&problem);
	if (timeval==0) return 0; // ignore heartbeats till we see a real timestamp
	if (timeval>= self->printfunc.nexttime) {
		gs_int8_t oldname[1024];
		gs_int8_t newname[1024];
		if (self->printfunc.split==0) {
			if (self->printfunc.fa[0] != 0) {
				sprintf(oldname,"%s/%u%s.tmp",self->printfunc.path,self->printfunc.nexttime-self->printfunc.delta,self->printfunc.basename);
				sprintf(newname,"%s/%u%s",self->printfunc.path,self->printfunc.nexttime-self->printfunc.delta,self->printfunc.basename);
				fclose(self->printfunc.fa[0]);
				rename(oldname,newname);
			}
			if (self->printfunc.nexttime==0) {
				self->printfunc.nexttime=(timeval/self->printfunc.delta)*self->printfunc.delta;
			}
			sprintf(oldname,"%s/%u%s.tmp",self->printfunc.path,self->printfunc.nexttime,self->printfunc.basename);
			if ((self->printfunc.fa[0]=fopen(oldname,"w"))==0) {
				gslog(LOG_EMERG,"ERROR:Could not open file in HFTA print function\n");
				return -1;
			}
			if (setvbuf(self->printfunc.fa[0],0,_IOFBF,16000000)!=0) {
				gslog(LOG_EMERG,"ERROR:Could not setvbuf\n");
			}
			if (fwrite(self->printfunc.header,strlen(self->printfunc.header)+1,1,self->printfunc.fa[0])!=1) {
				gslog(LOG_EMERG,"ERROR:fwrite:xfgh1:%s:%u",self->printfunc.basename,errno);
			}
			gslog(LOG_INFO,"Opened file %s",oldname);
		} else {
			for(x=self->printfunc.base;x<self->printfunc.split;x=x+self->printfunc.itt) {
				if (self->printfunc.fa[x] != 0) {
					sprintf(oldname,"%s/%u_s%u%s.tmp",self->printfunc.path,self->printfunc.nexttime-self->printfunc.delta,x+1,self->printfunc.basename);
					sprintf(newname,"%s/%u_s%u%s",self->printfunc.path,self->printfunc.nexttime-self->printfunc.delta,x+1,self->printfunc.basename);
					fclose(self->printfunc.fa[x]);
					rename(oldname,newname);
				}
				if (self->printfunc.nexttime==0) {
					self->printfunc.nexttime=(timeval/self->printfunc.delta)*self->printfunc.delta;
				}
				sprintf(oldname,"%s/%u_s%u%s.tmp",self->printfunc.path,self->printfunc.nexttime,x+1,self->printfunc.basename);
				if ((self->printfunc.fa[x]=fopen(oldname,"w"))==0) {
					gslog(LOG_EMERG,"ERROR:Could not open file in HFTA print function\n");
					return -1;
				}
                if (setvbuf(self->printfunc.fa[x],0,_IOFBF,16000000)!=0) {
                    gslog(LOG_EMERG,"ERROR:Could not setvbuf\n");
                }
				if (fwrite(self->printfunc.header,strlen(self->printfunc.header)+1,1,self->printfunc.fa[x])!=1)  {
                    gslog(LOG_EMERG,"ERROR:fwrite:xfgh2:%s:%u",self->printfunc.basename,errno);
                }
                gslog(LOG_INFO,"Opened file %s",oldname);
			}
		}
		self->printfunc.nexttime=self->printfunc.nexttime+self->printfunc.delta;
	}
    // don't write temporal tuples to file but use them to advance file name.
	if (ftaschema_is_temporal_tuple(self->printfunc.schemaid, tuple)) return 0;
	if (self->printfunc.split!=0) {
		splitval=fta_unpack_uint(tuple,sz,self->printfunc.split_field,&problem)%(self->printfunc.split);
		if (self->printfunc.fa[splitval]==0) {
			gslog(LOG_EMERG,"Inconsistent rangehash in print %u\n", splitval);
			exit(0);
		}
	} else {
		splitval=0;
	}
	nsz=htonl(sz);
	if (fwrite(&nsz,sizeof(gs_uint32_t),1,self->printfunc.fa[splitval])!=1) {
		gslog(LOG_EMERG,"Could not write to output in HFTA print\"%s\":%u.. EXITING\n",
              self->printfunc.basename,errno);
		exit(0);
	}
	if (fwrite(tuple,sz,1,self->printfunc.fa[splitval])!=1) {
		gslog(LOG_EMERG,"Could not write to output in HFTA print\"%s\"%u.. EXITING\n",
              self->printfunc.basename,errno);
		exit(0);
	}
	return 0;
}

/* registers an alloc function of an FTA and returns a unique index */
gs_retval_t ftacallback_add_alloc(FTAname name, alloc_fta fta_alloc_functionptr,gs_uint64_t prefilter)
{
    gs_int32_t x;
    gslog(LOG_INFO,"Register prefilter %llu for %s\n",prefilter,name);
    if (lalloc == 0) {
        if ((falloc = malloc(sizeof(struct ftacallbackalloc)*STARTSZ))==0) {
            gslog(LOG_EMERG,"ERROR:Out of memory ftacallback\n");
            return -1;
        }
        memset(falloc,0,sizeof(struct ftacallbackalloc)*STARTSZ);
        lalloc = STARTSZ;
    }
    for(x=0;(x<lalloc)&&(falloc[x].used!=0);x++);
    if (x == lalloc) {
        gs_int32_t y;
        lalloc = 2*lalloc;
        if ((falloc = realloc(falloc,lalloc*sizeof(struct ftacallbackalloc)))==0) {
            gslog(LOG_EMERG,"ERROR:Out of memory ftacallback\n");
            return -1;
        }
        for (y=x;y<lalloc;y++)
            falloc[y].used=0;
    }
    falloc[x].name=strdup(name);
    falloc[x].fta_alloc_functionptr=fta_alloc_functionptr;
    falloc[x].prefilter=prefilter;
    falloc[x].used=1;
    return x;
}

/* unregisters an alloc function of an FTA and makes the index available
 * for reuse
 */

gs_retval_t ftacallback_rm_alloc(gs_uint32_t index)
{
    falloc[index].used=0;
    free(falloc[index].name);
    return 0;
}
/* returns the prefilter for a given
 * index
 */

gs_uint64_t ftacallback_get_prefilter(gs_int32_t index)
{
    if ((index<lalloc) && (falloc[index].used!=0)) {
        return falloc[index].prefilter;
    }
    return 0;
}

/* returns the function pointer of the callback function for a given
 * index
 */

alloc_fta ftacallback_get_alloc(gs_int32_t index)
{
    if ((index<lalloc) && (falloc[index].used!=0)) {
        return falloc[index].fta_alloc_functionptr;
    }
    return 0;
}





/* associate ringbuffer with streamid (using refcounting) */
gs_retval_t ftacallback_add_streamid(struct ringbuf * r, gs_uint32_t streamid) {
    gs_int32_t x;
    if (lstreamid == 0) {
        if ((fstreamid = malloc(sizeof(struct ftacallbackstreamid)*STARTSZ))==0) {
            gslog(LOG_EMERG,"ERROR:Out of memory ftacallback\n");
            return -1;
        }
        memset(fstreamid,0,sizeof(struct ftacallbackstreamid)*STARTSZ);
        lstreamid = STARTSZ;
    }
    /* first try to increment refcnt */
    for(x=0;(x<lstreamid)&&(
                            (fstreamid[x].streamid!=streamid)
                            ||(fstreamid[x].r!=r)
                            ||(fstreamid[x].refcnt<=0)) ;x++);
    if (x>=lstreamid) {
        /* now try to find empty slot */
        for(x=0;(x<lstreamid)&&(fstreamid[x].refcnt!=0);x++);
        if (x >= lstreamid) {
            gs_int32_t y;
            lstreamid = 2*lstreamid;
            if ((fstreamid =
                 realloc(fstreamid,sizeof(struct ftacallbackstreamid)*lstreamid))==0) {
                gslog(LOG_EMERG,"ERROR:Out of memory ftacallback\n");
                return -1;
            }
            for (y=x;y<lstreamid;y++) {
                fstreamid[y].refcnt=0;
                fstreamid[y].streamid=0;
            }
        }
        fstreamid[x].state=HFTA_RINGBUF_ATOMIC;
    	fstreamid[x].streamid=streamid;
    	fstreamid[x].r=r;
    }
    fstreamid[x].refcnt+=1;
    return 0;
}


/* unassosciate a ringbuffer from a streamid */

gs_retval_t ftacallback_rm_streamid(struct ringbuf * r, gs_uint32_t streamid)
{
    gs_int32_t x;
    for(x=0;x<lstreamid;x++) {
        if ((fstreamid[x].streamid == streamid)
            && (fstreamid[x].r == r)
            && (fstreamid[x].refcnt > 0))
            fstreamid[x].refcnt--;
    }
    return 0;
}

/* set the state for a given streamid and destination process */
gs_retval_t ftacallback_state_streamid(gs_int32_t streamid,FTAID process, gs_int32_t state)
{
    gs_int32_t x;
    for(x=0;x<lstreamid;x++) {
        if ((fstreamid[x].streamid == streamid)
            && (fstreamid[x].r->destid.ip == process.ip )
            && (fstreamid[x].r->destid.port == process.port )
            && (fstreamid[x].refcnt > 0))
            fstreamid[x].state=state;
        return 0;
    }
    return -1;
}

/* starts an itteration through all ringbuffers for a particular streamid */

gs_retval_t ftacallback_start_streamid(gs_int32_t streamid)
{
    cstreamid=streamid;
    nstreamid=0;
    return 0;
}

/* returns all the ringbuffer associated with the streamid passed in
 * ftacallback_start_streamid
 */
struct ringbuf * ftacallback_next_streamid(gs_int32_t* state)
{
    for(;(nstreamid<lstreamid)
	    &&(fstreamid[nstreamid].streamid != cstreamid);
        nstreamid++);
    if (nstreamid<lstreamid) {
        nstreamid++;
        *state=fstreamid[nstreamid-1].state;
        return fstreamid[nstreamid-1].r;
    }
    return 0;
}


/* associate msgid with ringbuf  */
gs_retval_t ftacallback_add_wakeup(FTAID ftaid, struct ringbuf * r)
{
    gs_int32_t x;
    if (lwakeup == 0) {
        if ((fwakeup = malloc(sizeof(struct ftacallbackwakeup)*STARTSZ))==0) {
            gslog(LOG_EMERG,"ERROR:Out of memory ftacallback\n");
            return -1;
        }
        memset(fwakeup,0,sizeof(struct ftacallbackwakeup)*STARTSZ);
        lwakeup = STARTSZ;
    }
    /* first try to find one for the same process */
    for(x=0;(x<lwakeup)&&(
                          ((fwakeup[x].ftaid.ip!=ftaid.ip)
                           || (fwakeup[x].ftaid.port!=ftaid.port))
                          ||(fwakeup[x].used==0)) ;x++);
    if (x==lwakeup) {
        /* now try to find empty slot */
        for(x=0;(x<lwakeup)&&(fwakeup[x].used!=0);x++);
        if (x == lwakeup) {
            gs_int32_t y;
            lwakeup = 2*lwakeup;
            if ((fwakeup =
                 realloc(fwakeup,sizeof(struct ftacallbackwakeup)*lwakeup))==0) {
                gslog(LOG_EMERG,"ERROR:Out of memory ftacallback\n");
                return -1;
            }
            for (y=x;y<lwakeup;y++)
                fwakeup[y].used=0;
        }
    }
    fwakeup[x].used=1;
    fwakeup[x].ftaid=ftaid;
    fwakeup[x].r=r;
    return x;
}

/* starts an itteration through all msgids associated with
 a streamid. This also uses data kept in the fstreamid
 registry
 */
gs_retval_t ftacallback_start_wakeup(gs_uint32_t streamid)
{
    swakeup=streamid;
    nwakeup=0;
    return 0;
}

/* returns all the msgid blocked on the streamid passed in
 * ftacallback_start_streamid and removes the msgid from
 * the wakeup list
 */
FTAID * ftacallback_next_wakeup()
{
    for(;(nwakeup<lstreamid)
	    &&(fstreamid[nwakeup].streamid != swakeup);
        nwakeup++);
    if (nwakeup<lstreamid) {
        gs_int32_t x;
        for(x=0;x<lwakeup;x++)
            if ((fwakeup[x].r==fstreamid[nwakeup].r) &&
                (fwakeup[x].used==1)) {
                fwakeup[x].used=0;
                nwakeup++;
                return & fwakeup[x].ftaid;
            }
    }
    nwakeup ++;
    return (FTAID *) 0;
}



static gs_retval_t fta_list_add(struct fta_list ** root, struct FTA * after,
                                struct FTA * fta) {
    struct fta_list * new;
    struct fta_list * tmp;
    
    if ((new=(struct fta_list *)malloc(sizeof(struct fta_list)))==0) {
        gslog(LOG_EMERG,"fta_list_add:: can't allocate memory\n");
        return -1;
    }
    
    new->fta=fta;
    
    
    if (after==0) {
        new->next=*root;
        new->prev=0;
        *root=new;
        if (new->next) {
            new->next->prev=new;
        }
    } else {
        tmp=*root;
        while((tmp)&&(tmp->fta!=after)) {
            tmp=tmp->next;
        }
        if (tmp==0) {
            gslog(LOG_EMERG,"fta_list_add:: can't find after fta\n");
            return -1;
        }
        new->next=tmp->next;
        new->prev=tmp;
        tmp->next=new;
        if (new->next) {
            new->next->prev=new;
        }
    }
    return 0;
}

static gs_retval_t fta_list_rm(struct fta_list ** root, struct FTA * fta) {
    struct fta_list * tmp;
    tmp=*root;
    while((tmp)&&(tmp->fta!=fta)) {
        tmp=tmp->next;
    }
    if (tmp==0) {
        gslog(LOG_EMERG,"fta_list_rm:: can't find fta\n");
        return -1;
    }
    if (tmp == (*root)) {
        *root=tmp->next;
        if (tmp->next) {
            tmp->next->prev=0;
        }
    } else {
        tmp->prev->next=tmp->next;
        if (tmp->next) {
            tmp->next->prev=tmp->prev;
        }
    }
    
    free(tmp);
    return 0;
}


static gs_retval_t fta_list_check(struct fta_list ** root, struct FTA * fta) {
    struct fta_list * tmp;
    tmp=*root;
    while((tmp)&&(tmp->fta!=fta)) {
        tmp=tmp->next;
    }
    if (tmp==0) {
        return -1;
    }
    return 0;
}

gs_retval_t ftaexec_insert(struct FTA * after, struct FTA * new)
{
    
    if ((after!=0) && (fta_list_check(&process,after)<0)) {
        gslog(LOG_EMERG,"fta_insert:: ilegal adapter for after\n");
        return -1;
    }
    
    if (fta_list_check(&created,new)<0) {
        gslog(LOG_EMERG,"fta_insert:: ilegal adapter for new\n");
        return -1;
    }
    
    if (fta_list_check(&process,new)==0) {
		new->runrefcnt++;
        gslog(LOG_INFO,"fta_insert:: new already in process list reusing entry with streamid %d\n",new->ftaid.streamid);
        return 0;
    }
    
    if (fta_list_add(&process,after,new)<0) {
        gslog(LOG_EMERG,"fta_insert:: new can not be added to process list\n");
        return -1;
    }
    
    new->runrefcnt++;
    
    return 0;
    
}

gs_retval_t ftaexec_remove(struct FTA * id){
    
    if (fta_list_check(&process,id)<0) {
        gslog(LOG_EMERG,"fta_remove:: id not in process list\n");
        return -1;
    }
    id->runrefcnt--;
    if (id->runrefcnt<=0) {
        if (fta_list_rm(&process,id)<0) {
            gslog(LOG_EMERG,"fta_remove:: id could not be removed from process list\n");
            return -1;
        }
    }
    return 0;
}


struct FTA * ftaexec_alloc_instance(gs_uint32_t index, struct FTA * reuse,
                                    gs_uint32_t reusable,
                                    gs_int32_t command, gs_int32_t sz, void *  data){
    struct FTA * f;
    FTAID ftaid;
    ftaid=gscpipc_getftaid();
    ftaid.index=index;
    ftaid.streamid=0;
    if (fta_list_check(&created,reuse)==0) {
        reuse->refcnt++;
        return reuse;
    }
    if (ftacallback_get_alloc(index)==0) return 0;
    f=ftacallback_get_alloc(index)(ftaid,reusable,command, sz, data);
    f->prefilter=ftacallback_get_prefilter(index);
    gslog(LOG_INFO,"Using prefilter %llu for fta %x\n",f->prefilter,f);
    if (fta_list_add(&created,0,f)<0) {
        gslog(LOG_EMERG,"fta_alloc_instance:: new fta can not be added to created list\n");
        return 0;
    }
    f->refcnt=1;
    return f;
}



gs_retval_t ftaexec_free_instance(struct FTA * id, gs_uint32_t recursive){
    id->refcnt --;
    if (id->refcnt==0) {
        if (fta_list_rm(&created,id)<0) {
            gslog(LOG_EMERG,"fta_free_instance:: fta could not be removed from created list\n");
            return -1;
        }
        /* just to make sure remove it form process list too */
        if (fta_list_check(&process,id)>=0) {
            fta_list_rm(&process,id);
        }
        
        id->free_fta(id,recursive);
    }
    return 0;
}

gs_retval_t ftaexec_control(struct FTA * id, gs_int32_t command, gs_int32_t sz, void * value){
    if (fta_list_check(&created,id)<0) {
        gslog(LOG_EMERG,"fta_control:: id not found in adapter's created list\n");
        return -1;
    }
    return id->control_fta(id,command,sz,value);
}

gs_retval_t ftaexec_process_control(gs_int32_t command, gs_int32_t sz, void * value){
    struct FTA * f;
    ftaexec_start();
    while((f=ftaexec_next())!=0) {
        f->control_fta(f,command,sz,value);
    }
    return 1;
}


/* Start itteration through list of active FTA */

gs_retval_t ftaexec_start()
{
    itteration=process;
    return 0;
}

/* get one FTA at a time */

struct FTA * ftaexec_next()
{
    struct FTA * fta=0;
    if (itteration) {
        fta=itteration->fta;
        itteration=itteration->next;
    }
    return fta;
}


/* adds a buffer to the end of the sidequeue*/
gs_retval_t sidequeue_append(FTAID from, gs_sp_t buf, gs_int32_t length)
{
    struct sq * s;
    if ((s=malloc(sizeof(struct sq)))==0) {
        gslog(LOG_EMERG,"Could not allocate memory for sidequeue");
        return -1;
    }
    s->from=from;
    memcpy(&s->buf[0],buf,MAXMSGSZ);
    s->length=length;
    s->next=0;
    if (sqtail) {
        sqtail->next=s;
        sqtail=s;
    } else {
        sqtop = s;
        sqtail = s;
    }
    return 0;
}

/* removes a buffer from the top of the sidequeue*/
gs_retval_t sidequeue_pop(FTAID * from, gs_sp_t buf, gs_int32_t* length)
{
    struct sq * s;
    
    if (sqtop) {
        *from=sqtop->from;
        memcpy(buf,&sqtop->buf[0],MAXMSGSZ);
        *length=sqtop->length;
        s=sqtop;
        sqtop=sqtop->next;
        if (sqtop==0) sqtail=0;
        free(s);
        return 0;
    }
    return -1;
}
