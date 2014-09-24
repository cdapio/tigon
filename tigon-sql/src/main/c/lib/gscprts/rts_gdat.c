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
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "errno.h"

#include "gsconfig.h"
#include "gshub.h"
#include "gstypes.h"
#include "lapp.h"
#include "fta.h"
#include "stdio.h"
#include "stdlib.h"
#include "packet.h"
#include "schemaparser.h"
#include "lfta/rts.h"


void rts_fta_process_packet(struct packet * p);
void rts_fta_done();
void fta_init();

#define CSVMAXLINE 1000000

static FILE *pd;
static struct packet cur_packet;
static gs_sp_t name;
static gs_uint32_t verbose=0;
static gs_uint32_t startupdelay=0;
static gs_uint32_t gshub=0;
static int socket_desc=0;

static void gdat_replay_check_messages() {
    if (fta_start_service(0)<0) {
        print_error("Error:in processing the msg queue for a replay file");
        exit(9);
    }
}

static gs_retval_t gs_read(gs_sp_t buffer, gs_uint32_t length){
	gs_uint32_t used=0;
	gs_uint32_t cur;
    fd_set socket_rset;
    fd_set socket_eset;
    struct timeval socket_timeout;
    int retval;
    
    FD_ZERO(&socket_rset);
    FD_SET(socket_desc,&socket_rset);
    FD_ZERO(&socket_eset);
    FD_SET(socket_desc,&socket_eset);
    // timeout in one millisecon
    socket_timeout.tv_sec=0;
    socket_timeout.tv_usec=1000;
    
    if ((retval=select(socket_desc+1,&socket_rset,0,&socket_eset,&socket_timeout))<=0) {
        if (retval==0) {
            // caught a timeout
            return -1;
        }
        print_error("ERROR:select error in reading data from socket");
        exit(0);
    }
    
	while(used < length) {
		if ((cur=read(socket_desc,&(buffer[used]),length-used))<=0) {
            if (errno==115) return -2; // error code we get if the server closes the connection on us
			print_error("ERROR:could not read data from gdat stream");
			exit(0);
		}
		used+=cur;
	}
	return 1;
}

static gs_uint32_t gs_read_line(gs_sp_t buffer, gs_uint32_t length){
    gs_uint32_t used=0;
    gs_uint32_t cur;
    
    while((used < (length-1)) && ((used==0)|| (buffer[used-1]!='\n'))) {
        if ((cur=read(socket_desc,&(buffer[used]),1))<=0) {
            print_error("ERROR:could not read data from gdat stream");
            exit(0);
        }
        used+=cur;
    }
	buffer[used]=0;
	return 1;
}


static void init_socket() {
	endpoint gshub;
	endpoint srcinfo;
 	struct sockaddr_in server;
    gs_int32_t parserversion;
    gs_uint32_t schemalen;
    static char * asciischema=0;
	gs_int8_t buf[1024];
	
	if (get_hub(&gshub)!=0) {
		print_error("ERROR:could not find gshub for data source");
		exit(0);
	}
    
	if (get_streamsource(gshub,name,&srcinfo,1) !=0) {
		print_error("ERROR:could not find data source for stream\n");
		exit(0);
	}
    
	socket_desc = socket(AF_INET , SOCK_STREAM , 0);
    if (socket_desc == -1)
    {
        print_error("ERROR:could not create socket for data stream");
		exit(0);
    }
	server.sin_addr.s_addr = srcinfo.ip;
    server.sin_family = AF_INET;
    server.sin_port = srcinfo.port;
    
	if (connect(socket_desc , (struct sockaddr *)&server , sizeof(server)) < 0)
    {
		print_error("ERROR: could not open connection to data source");
		exit(0);
	}
    
    
	gs_read_line(buf,1024);
	if (strncmp(buf,"GDAT",4)!=0) {
 		print_error("ERROR: not a GDAT stream\n");
        exit(0);
    }
	gs_read_line(buf,1024);
	if (sscanf(buf,"VERSION:%u\n",&parserversion)!=1) {
        print_error("ERROR: no GDAT VERSION given\n");
        exit(0);
    }
	gs_read_line(buf,1024);
    if (sscanf(buf,"SCHEMALENGTH:%u\n",&schemalen)!=1) {
        print_error("ERROR: no GDAT SCHEMALENGTH given\n");
        exit(0);
    }
    if (schemaparser_accepts_version(parserversion)!=1) {
        print_error("gdatinput::wrong gdat version\n");
        exit(0);
    }
    if ((asciischema=malloc(schemalen))==0) {
        print_error("gdatinput::could not allocate memory for schema\n");
        exit(0);
    }
    if (gs_read(asciischema,schemalen)!=1) {
        print_error("gdatinput::could not read schema from file\n");
        exit(0);
    }
    if ((cur_packet.record.gdat.schema=ftaschema_parse_string(asciischema))<0) {
        print_error("gdatinput::could not parse schema\n");
        exit(0);
    }
    cur_packet.record.gdat.numfields=ftaschema_tuple_len(cur_packet.record.gdat.schema);
}


static void next_file() {
    struct stat s;
    gs_int32_t parserversion;
    gs_uint32_t schemalen;
	static char * asciischema=0;
    
    if (verbose) {
        fprintf(stderr,"Opening %s\n",name);
    }
    while (lstat(name,&s)!=0) {
        if (errno!=ENOENT) {
            print_error("gdat::lstat unexpected return value");
            exit(10);
        }
        gdat_replay_check_messages();
        usleep(10000);
    }
    
    
    if (pd!=0) {
        fclose(pd);
        if (asciischema!=0) free(asciischema);
        if (cur_packet.record.gdat.schema>=0) {
            ftaschema_free(cur_packet.record.gdat.schema);
        }
    }
    
    if ((pd=fopen(name,"r"))==0) return;
	unlink(name);
    
	if (fscanf(pd,"GDAT\nVERSION:%u\nSCHEMALENGTH:%u\n",&parserversion,&schemalen)!=2) {
		if (verbose) fprintf(stderr,"gdatinput::could not parse GDAT header\n");
		fclose(pd);
		pd=0;
		return;
    }
    if (schemaparser_accepts_version(parserversion)!=1) {
        if (verbose) fprintf(stderr,"gdatinput::wrong gdat version\n");
        fclose(pd);
        pd=0;
		return;
	}
	if ((asciischema=malloc(schemalen))==0) {
        if (verbose) fprintf(stderr,"gdatinput::could not allocate memory for schema\n");
 		fclose(pd);
		pd=0;
        return;
    }
	if (fread(asciischema,schemalen,1,pd)!=1) {
        if (verbose) fprintf(stderr,"gdatinput::could not read schema from file\n");
		fclose(pd);
		pd=0;
        return;
    }
	if ((cur_packet.record.gdat.schema=ftaschema_parse_string(asciischema))<0) {
        if (verbose) fprintf(stderr,"gdatinput::could not parse schema\n");
		fclose(pd);
		pd=0;
		return;
	}
	cur_packet.record.gdat.numfields=ftaschema_tuple_len(cur_packet.record.gdat.schema);
}

static gs_retval_t gdat_replay_init(gs_sp_t device)
{
    gs_sp_t  verbosetmp;
    gs_sp_t  delaytmp;
    gs_sp_t  gshubtmp;
    if ((name=get_iface_properties(device,"filename"))==0) {
		print_error("csv_init::No GDAT \"Filename\" defined");
		exit(0);
	}
    
    if ((verbosetmp=get_iface_properties(device,"verbose"))!=0) {
        if (strncmp(verbosetmp,"TRUE",4)==0) {
            verbose=1;
            fprintf(stderr,"VERBOSE ENABLED\n");
        } else {
            fprintf(stderr,"VERBOSE DISABLED\n");
        }
    }
    if ((delaytmp=get_iface_properties(device,"startupdelay"))!=0) {
        if (verbose) {
            fprintf(stderr,"Startup delay of %u seconds\n",atoi(get_iface_properties(device,"startupdelay")));
        }
        startupdelay=atoi(get_iface_properties(device,"startupdelay"));
    }
    if ((gshubtmp=get_iface_properties(device,"gshub"))!=0) {
        if (verbose) {
            fprintf(stderr,"GDAT format using gshub\n");
        }
        gshub=1;
    }
    
    cur_packet.ptype=PTYPE_GDAT;
    return 0;
}

static gs_retval_t gdat_read_socket(){
    gs_uint32_t nsz,sz;
    gs_retval_t retval;
    if ((retval=gs_read((gs_sp_t)&nsz,sizeof(gs_uint32_t)))<0) {
        return retval;
    }
    sz=ntohl(nsz);
    if (sz>MAXTUPLESZ) {
        if (verbose) {
            fprintf(stderr,"INTERNAL ERROR tuple to long for fixed buffer. Tuple sz %u\n",
                    (sz));
        }
        print_error("Error::Illegal tuple received");
        exit(0);
    }
    
    cur_packet.record.gdat.datasz=sz;
    
    if (gs_read((gs_sp_t)cur_packet.record.gdat.data,(sz))!=1) {
        if (verbose){
            fprintf(stderr,"UNEXPECTED END OF FILE. Tryed to read tuple of size %u\n",
                    (sz));
        }
        print_error("Error::Illegal tuple received");
        exit(0);
    }
    cur_packet.systemTime=time(0);
    return 0;
}

static gs_retval_t  gdat_read_tuple(){
    gs_uint32_t nsz,sz;
again:
    while((pd==0) ||  (fread(&nsz,sizeof(gs_uint32_t),1,pd)!=1)) {
        next_file();
    }
    sz=ntohl(nsz);
    if (sz>MAXTUPLESZ) {
        if (verbose) {
        	fprintf(stderr,"INTERNAL ERROR tuple to long for fixed buffer. Tuple sz %u\n",
                    (sz));
        }
        fclose(pd);
        pd=0;
        goto again;
    }
    
    cur_packet.record.gdat.datasz=sz;
	
    if (fread(cur_packet.record.gdat.data,(sz),1,pd)!=1) {
    	if (verbose){
            fprintf(stderr,"UNEXPECTED END OF FILE. Tryed to read tuple of size %u\n",
                    (sz));
        }
        fclose(pd);
        pd=0;
        goto again;
    }
    cur_packet.systemTime=time(0);
    return 0;
}

static gs_retval_t gdat_process_file()
{
	unsigned cnt=0;
	static unsigned totalcnt=0;
   	for(cnt=0;cnt<50000;cnt++) {
		if (gshub!=0) {
            gs_retval_t retval;
            retval=gdat_read_socket();
			if (retval==-1) return 0; // got a timeout so service message queue
            if ((retval==-2) || (ftaschema_is_eof_tuple(cur_packet.record.gdat.schema,(void *)cur_packet.record.gdat.data))) {
                // we signal that everything is done if we either see an EOF tuple OR the socket is closed by the peer
                if (verbose)
                    fprintf(stderr,"Done processing waiting for things to shut down\n");
                rts_fta_done();
                // now just service message queue until we get killed or loose connectivity
                while (0==0) {
                    fta_start_service(0); // service all waiting messages
                    usleep(1000); // sleep a millisecond
                }
            }
		} else {
			gdat_read_tuple();
		}
		rts_fta_process_packet(&cur_packet);
		cnt++;
	}
	totalcnt=totalcnt+cnt;
	if (verbose) {
		fprintf(stderr,"Processesd %u tuple\n",totalcnt);
	}
	return 0;
}

gs_retval_t main_gdat(gs_int32_t devicenum, gs_sp_t device, gs_int32_t mapcnt, gs_sp_t map[]) {
    gs_uint32_t cont;
    endpoint mygshub;
    
    gdat_replay_init(device);
    
    /* initalize host_lib */
    if (verbose) {
        fprintf(stderr,"Init LFTAs for %s\n",device);
    }
    
    if (hostlib_init(LFTA,0,devicenum,mapcnt,map)<0) {
        fprintf(stderr,"%s::error:could not initiate host lib for clearinghouse\n",
                device);
        exit(7);
    }
    
    fta_init();
    
    cont=startupdelay+time(0);
    
    if (verbose) { fprintf(stderr,"Start startup delay"); }
    
    while (cont>time(NULL)) {
        if (fta_start_service(0)<0) {
            fprintf(stderr,"%s::error:in processing the msg queue\n",
                    device);
            exit(9);
        }
        usleep(1); /* sleep for one millisecond */
    }
    
    if (verbose) { fprintf(stderr,"... Done\n"); }
    
    // open the connection to the data source
    
    if (gshub!=0) { init_socket();}
    
    // wait to process till we get the signal from GSHUB
	if (get_hub(&mygshub)!=0) {
		print_error("ERROR:could not find gshub for data source");
		exit(0);
	}
    while(get_startprocessing(mygshub,get_instance_name(),0)!=0) {
        usleep(100);
        if (fta_start_service(0)<0) {
            fprintf(stderr,"%s::error:in processing the msg queue\n",
                    device);
            exit(9);
        }
    }

    /* now we enter an endless loop to process data */
    if (verbose) {
    	fprintf(stderr,"Start processing %s\n",device);
    }
    
    while (1==1) {
        /* proess packets data stream*/
        if (gdat_process_file()<0) {
            fprintf(stderr,"%s::error:in processing packets\n",
                    device);
            exit(8);
        }
        /* process all messages on the message queue*/
        if (fta_start_service(0)<0) {
            fprintf(stderr,"%s::error:in processing the msg queue\n",
                    device);
            exit(9);
        }
    }
    return 0;
}



