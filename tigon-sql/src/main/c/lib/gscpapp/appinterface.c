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
#include "app.h"
#include "fta.h"
#include "lapp.h"
#include "string.h"
#include "stdio.h"
#include "stdlib.h"
#include "schemaparser.h"
#include "gshub.h"


// Defined here to avoid link errors as this array is auto generated for the lfta and referenced in the clearinghouse library which gets linked against the hfta
gs_sp_t fta_names[]={0};


/* HIGH LEVEL APPLICATION INTERFACE */
/* ================================ */

struct fta_instance {
    gs_sp_t name;
    FTAID ftaid;
    gs_int32_t used;
    gs_schemahandle_t schema;
};


struct fta_instance * instance_array=0;
gs_int32_t instance_array_sz=0;

static gs_retval_t
add_fta(gs_sp_t name, FTAID ftaid,
        gs_schemahandle_t schema)
{
    gs_int32_t x;
    if ( instance_array_sz == 0) {
        if ((instance_array = malloc(sizeof(struct fta_instance)*STARTSZ))==0) {
            gslog(LOG_EMERG,"ERROR:Out of memory ftacallback\n");
            return -1;
        }
        memset(instance_array,0,sizeof(struct fta_instance)*STARTSZ);
        instance_array_sz = STARTSZ;
    }
    for(x=0;(x<instance_array_sz)&&(instance_array[x].used!=0);x++);
    if (x == instance_array_sz) {
        gs_int32_t y;
        instance_array_sz = 2*instance_array_sz;
        if ((instance_array =
             realloc(instance_array,instance_array_sz*
                     sizeof(struct fta_instance)))==0) {
                 gslog(LOG_EMERG,"ERROR:Out of memory ftacallback\n");
                 return -1;
             }
        for (y=x;y<instance_array_sz;x++)
            instance_array[y].used=0;
    }
    instance_array[x].name=strdup(name);
    instance_array[x].ftaid=ftaid;
    instance_array[x].schema=schema;
    instance_array[x].used=1;
    return 0;
}

static gs_retval_t
rm_fta(FTAID ftaid)
{
    gs_int32_t x;
    for (x=0;x<instance_array_sz;x++) {
        if ( (instance_array[x].ftaid.ip=ftaid.ip)
            && (instance_array[x].ftaid.port==ftaid.port)
            && (instance_array[x].ftaid.index==ftaid.index)
            && (instance_array[x].ftaid.streamid==ftaid.streamid)){
            instance_array[x].used=0;
        }
    }
    return 0;
}


static struct fta_instance *
get_fta(FTAID ftaid)
{
    gs_int32_t x;
    for (x=0;x<instance_array_sz;x++) {
        if (( instance_array[x].used!=0 )
            && (instance_array[x].ftaid.ip==ftaid.ip)
            && (instance_array[x].ftaid.port==ftaid.port)
            && (instance_array[x].ftaid.index==ftaid.index)
            && (instance_array[x].ftaid.streamid==ftaid.streamid))
        {
            return &instance_array[x];
        }
    }
    return 0;
}



gs_retval_t
ftaapp_init(gs_uint32_t bufsz)
{
    
    endpoint gshub;
    FTAID myftaid;
    gs_sp_t name = "app\0";
    if (hostlib_init(APP,bufsz,DEFAULTDEV,0,0)!=0) {
        gslog(LOG_EMERG,"ftaap_init::error:could not initialize hostlib\n");
        return -1;
    }
    if (get_hub(&gshub)!=0) {
         gslog(LOG_EMERG,"ERROR:could not find gshub in appinterface init");
         return -1;
    }
    myftaid=gscpipc_getftaid();
    if (set_ftainstance(gshub,get_instance_name(),(gs_sp_t)name,&myftaid)!=0) {
        gslog(LOG_EMERG,"ERROR:could not set_ftainstance");
        return -1;
    }
    return 0;
}

/* this should be used before exiting to make sure everything gets
 cleaned up
 */
gs_retval_t
ftaapp_exit()
{
    gs_int32_t x;
    for (x=0;x<instance_array_sz;x++) {
        if (instance_array[x].used!=0) {
            ftaapp_remove_fta(instance_array[x].ftaid,1);
        }
    }
    hostlib_free();
    return 0;
}

/* adds an FTA by key returns unique streamid which can be used to reference FTA*/


FTAID
ftaapp_add_fta(FTAname name, gs_uint32_t reuse, gs_uint32_t reusable,
               gs_int32_t command, gs_int32_t sz, void *  data)
{
    gs_int8_t schemabuf[MAXSCHEMASZ];
    gs_schemahandle_t schema;
    FTAID f;
    FTAID ferr;
    ferr.ip=0;
    ferr.port=0;
    ferr.index=0;
    ferr.streamid=0;
    
    
    if (fta_find(name,reuse,&f,schemabuf,MAXSCHEMASZ)!=0) {
        gslog(LOG_EMERG,"ftaapp_add_fta::error:could not find FTA\n");
        return ferr;
    }
    
    
    if ((schema=ftaschema_parse_string(schemabuf))<0) {
        gslog(LOG_EMERG,"ftaapp_add_fta::error:could not parse schema\n");
        fprintf(stderr,"/n%s\n",schemabuf);
        return ferr;
    }
    
    if ((fta_alloc_instance(gscpipc_getftaid(),&f,name,schemabuf,reusable,
                            command,sz,data))!=0) {
        gslog(LOG_EMERG,"ftaapp_add_fta::error:could instantiate a FTA\n");
        ftaschema_free(schema);
        return ferr;
    }
    
    if (f.streamid==0) {
        gslog(LOG_EMERG,"ftaapp_add_fta::error:could instantiate a FTA\n");
        ftaschema_free(schema);
        return ferr;
    }
    
    //gslog(LOG_EMERG,"apptrace adding fta %u %u %u %u\n",f.ip,f.port,f.index,f.streamid);
    if (add_fta((gs_sp_t)name,f,schema)<0) {
        gslog(LOG_EMERG,"ftaapp_add_fta::error:could not add fta to internal db\n");
        fta_free_instance(gscpipc_getftaid(),f,1);
        ftaschema_free(schema);
        return ferr;
    }
    
    return f;
}

FTAID ftaapp_add_fta_print(FTAname name, gs_uint32_t reuse, gs_uint32_t reusable,
                           gs_int32_t command, gs_int32_t sz,
                           void *  data,gs_sp_t path,
                           gs_sp_t basename, gs_sp_t temporal_field, gs_sp_t split_field,
                           gs_uint32_t delta, gs_uint32_t split) {
    gs_int8_t schemabuf[MAXSCHEMASZ];
    gs_schemahandle_t schema;
    FTAID f;
    FTAID ferr;
    ferr.ip=0;
    ferr.port=0;
    ferr.index=0;
    ferr.streamid=0;
    
    
    if (fta_find(name,reuse,&f,schemabuf,MAXSCHEMASZ)!=0) {
        gslog(LOG_EMERG,"ftaapp_add_fta::error:could not find FTA\n");
        return ferr;
    }
    
    if ((schema=ftaschema_parse_string(schemabuf))<0) {
        gslog(LOG_EMERG,"ftaapp_add_fta::error:could not parse schema\n");
        return ferr;
    }
    
    if ((fta_alloc_print_instance(gscpipc_getftaid(),
                                  &f,name,schemabuf,reusable,
                                  command,sz,data,path,
                                  basename,temporal_field,split_field,delta,split))!=0) {
        gslog(LOG_EMERG,"ftaapp_add_fta::error:could instantiate a FTA\n");
        ftaschema_free(schema);
        return ferr;
    }
    
    if (f.streamid==0) {
        gslog(LOG_EMERG,"ftaapp_add_fta::error:could instantiate a FTA\n");
        ftaschema_free(schema);
        return ferr;
    }
    
    if (add_fta((gs_sp_t)name,f,schema)<0) {
        gslog(LOG_EMERG,"ftaapp_add_fta::error:could not add fta to internal db\n");
        fta_free_instance(gscpipc_getftaid(),f,1);
        ftaschema_free(schema);
        return ferr;
    }
    
    return f;
}

/* get the schema definition of an FTA */
gs_schemahandle_t
ftaapp_get_fta_schema(FTAID ftaid)
{
    struct fta_instance * fi;
    
    //gslog(LOG_EMERG,"apptrace checking fta %u %u %u %u\n",ftaid.ip,ftaid.port,ftaid.index,ftaid.streamid);
    
    if ((fi=get_fta(ftaid))==0) {
        gslog(LOG_EMERG,"ftaapp_get_fta_schema::error:unknown streamid\n");
        return -1;
    }
    return fi->schema;
}

/* get the asci schema definition for the FTA associated with the FTA name */
gs_schemahandle_t ftaapp_get_fta_schema_by_name(gs_sp_t name)
{
    FTAID f;
    gs_int8_t schemabuf[MAXSCHEMASZ];
    gs_schemahandle_t schema;
    if (fta_find(name,0,&f,schemabuf,MAXSCHEMASZ)!=0) {
        gslog(LOG_EMERG,"ftaapp_get_fta_schema_by_name::error:could not find FTA\n");
        return -1;
    }
    if ((schema=ftaschema_parse_string(schemabuf))<0) {
        gslog(LOG_EMERG,"ftaapp_get_fta_schema_by_name::error:could not parse schema\n");
        return -1;
    }
    return schema;
}

/* get the asci schema definition for the FTA associated with the FTA name */
gs_sp_t  ftaapp_get_fta_ascii_schema_by_name(gs_sp_t name)
{
    FTAID f;
    static gs_int8_t schemabuf[MAXSCHEMASZ];
    if (fta_find(name,0,&f,schemabuf,MAXSCHEMASZ)!=0) {
        gslog(LOG_EMERG,"ftaapp_get_fta_schema_by_name::error:could not find FTA\n");
        return 0;
    }
    return schemabuf;
}

/* control operations keyed of one to one mapping of stream id */
gs_retval_t
ftaapp_control(FTAID f, gs_int32_t command, gs_int32_t sz, void *  data)
{
    struct fta_instance * fi;
    if ((fi=get_fta(f))<0) {
        gslog(LOG_EMERG,"ftaapp_control::error:unknown streamid\n");
        return -1;
    }
    return fta_control(gscpipc_getftaid(),fi->ftaid,command,sz,data);
}

/* remove FTA keyed of stream id */
gs_retval_t
ftaapp_remove_fta(FTAID f, gs_uint32_t recursive)
{
    struct fta_instance * fi;
    if ((fi=get_fta(f))<0) {
        gslog(LOG_EMERG,"ftaapp_control::error:unknown streamid\n");
        return -1;
    }
    
    fta_free_instance(gscpipc_getftaid(),fi->ftaid,recursive);
    ftaschema_free(fi->schema);
    rm_fta(f);
    
    return 0;
}

/* same as sgroup_get_buffer just repeated to have a complet ftapp interface and remove
 the heartbeat tuples*/
gs_retval_t
ftaapp_get_tuple(FTAID * ftaid, gs_uint32_t * size, void *tbuffer,
                 gs_int32_t tbuf_size, gs_int32_t timeout)
{
    gs_uint64_t trace_id;
    gs_uint32_t sz;
    fta_stat * trace;
    gs_sp_t trace_buffer;
    gs_retval_t res;
    
get_tuple_again:
    res=gscp_get_buffer(ftaid,(gs_int32_t *)size,tbuffer,tbuf_size,timeout);
    
    if ((res==0) && (ftaschema_is_temporal_tuple(get_fta(*ftaid)->schema, tbuffer))) {
        FTAID myftaid;
        myftaid=gscpipc_getftaid();
        /* extract trace */
        if (ftaschema_get_trace(get_fta(*ftaid)->schema,
                                tbuffer, *size, &trace_id, &sz, &trace))
        {
            gslog(LOG_EMERG, "ftaapp_get_tuple:Error: temporal tuple with no trace\n");
            goto get_tuple_again;
        }
        
		if ((trace_buffer=(gs_sp_t)malloc((sz+1)*sizeof(fta_stat)))==0) {
            gslog(LOG_EMERG,"ftaapp_get_tuple::Error: allocation for trace tuple failed\n");
            goto get_tuple_again;
        }
        
        /* generate a heartbeat */
        memcpy(trace_buffer, trace, sz * sizeof(fta_stat));
        /* append producers fta_stat to the trace */
        /* for now we will just fill the FTAID part with 0 of fta_stat, the rest will be cleared */
        memset(trace_buffer + (sz * sizeof(fta_stat)), 0, sizeof(fta_stat));

        memcpy(trace_buffer + (sz * sizeof(fta_stat)), &myftaid, sizeof(FTAID));

        fta_heartbeat(gscpipc_getftaid(), trace_id, sz+1, (fta_stat *)trace_buffer);
		free(trace_buffer);
        res=2; //indicate that it is a temporal tuple
    }
    return res;
}
