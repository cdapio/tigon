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
#include <app.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "errno.h"


#include "gsconfig.h"
#include "gstypes.h"
#include "gshub.h"
#include <schemaparser.h>



#define MAXSTRLEN 256
#define MAXNUMFIELDS 64
#define BUFSIZE 16*1024*1024


gs_sp_t me;    	/* saved copy argv[0] */

gs_uint32_t socket_desc;
gs_uint32_t verbose=0;
gs_uint32_t parserversion;
gs_uint32_t withtrace=0;

struct FTA_state {
    FTAID fta_id;
    gs_schemahandle_t schema;
    gs_sp_t asciischema;
    gs_int32_t numfields;
};

struct FTA_state fs;

void hand(int iv) {
    ftaapp_exit();
    gslog(LOG_NOTICE, "exiting via signal handler %d...\n", iv);
    exit(0);
}

void timeouthand(int iv) {
    ftaapp_exit();
    //  if (s->verbose!=0) fprintf(stderr, "exiting because of timeout...\n");
    exit(0);
}


static void gs_write(gs_sp_t buffer, gs_uint32_t len)
{
    if (send(socket_desc,buffer,len,0) != len) {
        gslog(LOG_EMERG,"could not write on stream socket");
        exit(0);
    }
}

static void print_usage_exit(gs_sp_t reason) {
    fprintf(stderr,
            "%s::error: %s\n"
            "%s::usage: %s -v -t -h <gshub-hostname>:<gshub-port> <gsinstance_name>  <query_name>  <data_sink_name>\n"
            , me, reason, me, me);
    exit(1);
}




static void init(gs_int32_t argc, gs_sp_t argv[]) {
    void *pblk;
    gs_int32_t x, y, schema, pblklen, lcv;
    gs_sp_t c;
    gs_int8_t name[1024];
    gs_sp_t instance_name;
    gs_sp_t data_sink_name;
    gs_sp_t query_name;
    endpoint gshub;
    endpoint data_sink;
    endpoint dummyep;
    gs_uint32_t tip1,tip2,tip3,tip4;
    struct sockaddr_in server;
    
    if( (argc!=4) ) {
        print_usage_exit("Wrong number of paramters");
    }
    sprintf(name,"gsexit: %s %s %s %s ",argv[0],argv[1],argv[2],argv[3]);
    
    gsopenlog(name);
    
    
    if (sscanf(argv[0],"%u.%u.%u.%u:%hu",&tip1,&tip2,&tip3,&tip4,&(gshub.port))!= 5 ) {
        gslog(LOG_EMERG,"HUB IP NOT DEFINED");
        exit(1);
    }
    
    gshub.ip=htonl(tip1<<24|tip2<<16|tip3<<8|tip4);
    gshub.port=htons(gshub.port);
    instance_name=strdup(argv[1]);
    query_name=strdup(argv[2]);
    data_sink_name=strdup(argv[3]);
    
    if (set_hub(gshub)!=0) {
        gslog(LOG_EMERG,"Could not set hub");
        exit(1);
    }
    if (set_instance_name(instance_name)!=0) {
        gslog(LOG_EMERG,"Could not set instance name");
        exit(1);
    }
    
    if (get_initinstance(gshub,instance_name,&dummyep,1)!=0) {
        gslog(LOG_EMERG,"Did not receive signal that GS is initiated\n");
    }
    
    if (get_streamsink(gshub,data_sink_name,&data_sink,1) !=0 ) {
        gslog(LOG_EMERG,"Could not find data sink");
        exit(0);
    }
    
    if (ftaapp_init(BUFSIZE)!=0) {
        gslog(LOG_EMERG,"%s::error:could not initialize gscp\n", me);
        exit(1);
    }
    
    signal(SIGTERM, hand);
    signal(SIGINT, hand);
    signal(SIGPIPE, hand);
    
    if (verbose!=0) gslog(LOG_DEBUG,"Initializing FTAs\n");
    
    pblk = 0;
    pblklen = 0;
    
    fs.fta_id=ftaapp_add_fta(query_name,pblk?0:1,pblk?0:1,0,pblklen,pblk);
    if (fs.fta_id.streamid==0){
        gslog(LOG_EMERG,"%s::error:could not initialize fta %s\n",
              me,query_name);
        exit(1);
    }
    
    if ((c=ftaapp_get_fta_ascii_schema_by_name(query_name))==0){
        gslog(LOG_EMERG,"%s::error:could not get ascii schema for %s\n",
              me,query_name);
        exit(1);
    }
    
    //ftaapp_get_fta_ascii_schema_by_name uses static buffer so make a copy
    fs.asciischema=strdup(c);
    
    
    // Set parser version here
    parserversion=get_schemaparser_version();
    
    if ((fs.schema=ftaapp_get_fta_schema(fs.fta_id))<0) {
        gslog(LOG_EMERG,"%s::error:could not get schema for query\n",
              me,query_name);
        exit(1);
    }
    
    // Use all available fields
    if ((fs.numfields=ftaschema_tuple_len(fs.schema))<0) {
        gslog(LOG_EMERG,"%s::error:could not get number of fields for query %s\n",
              me,query_name);
        exit(1);
    }
    
    // Important that we only open the socket to the data sink AFTER we have subscribed to the output query as it uses it as a signal
    
    socket_desc = socket(AF_INET , SOCK_STREAM , 0);
    if (socket_desc == -1)
    {
        gslog(LOG_EMERG,"ERROR:could not create socket for data stream");
        exit(0);
    }
    server.sin_addr.s_addr = data_sink.ip;
    server.sin_family = AF_INET;
    server.sin_port = data_sink.port;
    
    if (connect(socket_desc , (struct sockaddr *)&server , sizeof(server)) < 0)
    {
        gslog(LOG_EMERG,"ERROR: could not open connection to data source");
        exit(0);
    }
    if (set_streamsubscription(gshub,instance_name,data_sink_name) !=0 ) {
        gslog(LOG_EMERG,"Could not announce streamsubscription for exit process");
        exit(0);
    }
  
    
}


static void process_data()
{
    gs_uint32_t nsz;
    FTAID rfta_id;
    gs_uint32_t rsize;
    gs_int32_t code;
    gs_int8_t rbuf[2*MAXTUPLESZ];
 	gs_int8_t topb[1024];
    
    
    if (verbose!=0) gslog(LOG_INFO,"Getting Data");
    
	sprintf(&topb[0],"GDAT\nVERSION:%u\nSCHEMALENGTH:%u\n",
			parserversion,(unsigned int)strlen(fs.asciischema)+1);
    gs_write(&topb[0],strlen(topb));
    gs_write(fs.asciischema,strlen(fs.asciischema)+1);
    
    
    
    while((code=ftaapp_get_tuple(&rfta_id,&rsize,rbuf,2*MAXTUPLESZ,0))>=0) {
        nsz=htonl(rsize);
        if ((withtrace==0)&&(code==2)) continue;
        if (verbose) {
            if (ftaschema_is_eof_tuple(fs.schema, rbuf)) {
                /* initiate shutdown or something of that nature */
                gslog(LOG_INFO,"gsexit::All data proccessed\n");
            }
        }
        gs_write((gs_sp_t)&nsz,sizeof(gs_uint32_t));
        gs_write(rbuf,rsize);
	}
}



int main(int argc, char** argv) {
    gs_int32_t ch;
    me = argv[0];
    
    while ((ch = getopt(argc, argv, "hvt")) != -1) {
        switch(ch) {
            case 'h':
                print_usage_exit("help");
                break;
            case 'v':
                verbose=1;
                break;
            case 't':
                withtrace=1;
                break;
            default:
                break;
        }
    }
    
    argc -= optind;
    argv += optind;
    
    /* initialize host library and the sgroup  */
    
    if (argc<=1) {
        print_usage_exit("Not enough arguments");
    }
    
    init(argc, argv);
    
    process_data();
    
    gslog(LOG_EMERG,"%s::internal error reached unexpected end", me);
}

