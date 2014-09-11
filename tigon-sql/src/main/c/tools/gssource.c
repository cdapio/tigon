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
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "gsconfig.h"
#include "gstypes.h"
#include "gshub.h"




gs_sp_t me = 0;
gs_sp_t schematext = 0;
gs_int32_t schematextlen = 0;
gs_sp_t schematmp = 0;
gs_int32_t verbose=0;
gs_uint32_t tcpport=0;
int listensockfd=0;
int fd=0;
endpoint hub;
endpoint ds;
gs_sp_t source_name;

static void gs_write(gs_sp_t buffer, gs_uint32_t len)
{
    if (send(fd,buffer,len,0) != len) {
        fprintf(stderr,"could not write on stream socket");
        exit(0);
    }
}

static void wait_for_feed() {
    struct sockaddr_in serv_addr,cli_addr;
    struct sockaddr_in sin;
    socklen_t clilen;
    socklen_t sin_sz;
    if (listensockfd==0) {
		gs_int32_t on = 1;
        
        if (verbose) {
            fprintf(stderr,"Create listen socket for port %u\n",tcpport);
        }
		listensockfd=socket(AF_INET, SOCK_STREAM, 0);
        if (listensockfd < 0) {
			fprintf(stderr,"Error:Could not create socket for tcp data stream");
			exit(1);
		}
		bzero((char *) &serv_addr, sizeof(serv_addr));
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_addr.s_addr = INADDR_ANY;
		serv_addr.sin_port = htons(tcpport);
#ifndef __linux__
        /* make sure we can reuse the common port rapidly */
        if (setsockopt(listensockfd, SOL_SOCKET, SO_REUSEPORT,
                       (gs_sp_t )&on, sizeof(on)) != 0) {
            fprintf(stderr,"Error::could not set socket option\n");
            exit(1);
        }
#endif
        if (setsockopt(listensockfd, SOL_SOCKET, SO_REUSEADDR,
                       (gs_sp_t )&on, sizeof(on)) != 0) {
            fprintf(stderr,"Error::could not set socket option\n");
            exit(1);
		}
        
		if (bind(listensockfd, (struct sockaddr *) &serv_addr,
                 sizeof(serv_addr)) < 0) {
			fprintf(stderr,"Error:Could not bind socket for tcp data stream");
            exit(1);
        }
	}
    
    if (verbose) {
        fprintf(stderr,"Socket created waiting for data producer\n");
    }
    if (listen(listensockfd,5)< 0) {
        fprintf(stderr,"Error::could not listen to socket for port %u \n",ntohs(serv_addr.sin_port));
        close(listensockfd);
        exit(1);
    }
    sin_sz=sizeof(sin);
    if (getsockname(listensockfd, (struct sockaddr *) &sin, &sin_sz) < 0) {
        fprintf(stderr,"Error::could not get local port number of listen socket\n");
        exit(1);
    }
    ds.ip=htonl(127<<24|1);
    ds.port=sin.sin_port;
    if (set_streamsource(hub,source_name,ds)!=0) {
        fprintf(stderr,"Error::could not set source in GSHUB for %s source name\n",source_name);
        exit(1);
    }
    
	do {
		clilen = sizeof(cli_addr);
		fd=accept(listensockfd, (struct sockaddr *) &cli_addr, &clilen);
		if (fd<0) {
            fprintf(stderr,"Error:Could not accept connection on tcp socket\n");
		}
	} while (fd==0);
    if (verbose) {
        fprintf(stderr,"Sink found ready to rock!\n");
    }
    
}


static void do_file(gs_sp_t filename, gs_int32_t fnlen);

int main(int argc, char** argv) {
    gs_int32_t x;
    gs_int32_t s=0;
    gs_int32_t ch;
    gs_int32_t endless=0; // repeats files forever
    gs_uint32_t tip1,tip2,tip3,tip4;
    while ((ch = getopt(argc, argv, "hxep:")) != -1) {
        switch(ch) {
            case 'h':
                fprintf(stderr,"%s::usage: %s -x -v -e -p <portnumber> <IP>:<port> <source_name> <gdatfiles...>\n",argv[0],argv[0]);
                exit(0);
                break;
            case 'p':
                tcpport=atoi(optarg);;
                break;
            case 'v':
                verbose=1;
                break;
            case 'x':
                verbose=2;
                break;
            case 'e':
                endless=1;
                break;
            default:
                break;
        }
    }
    s+=optind;
    if (s+2>argc) {
        fprintf(stderr,"Could not find hub and stream source name on command line\n");
        fprintf(stderr,"%s::usage: %s -x -v -e -p <portnumber> <IP>:<port> <source_name> <gdatfiles...>\n",argv[0],argv[0]);
        exit(1);
    }
    if (sscanf(argv[s],"%u.%u.%u.%u:%hu",&tip1,&tip2,&tip3,&tip4,&(hub.port))!= 5 ) {
        fprintf(stderr,"Could not parse hub endpoint\n");
        fprintf(stderr,"%s::usage: %s -x -v -e -p <portnumber> <IP>:<port> <source_name> <gdatfiles...>\n",argv[0],argv[0]);
        exit(1);
    }
    hub.ip=htonl(tip1<<24|tip2<<16|tip3<<8|tip4);
    hub.port=htons(hub.port);
    source_name=strdup(argv[s+1]);
    s+=2;
    wait_for_feed();
    do {
        for(x=s;x<argc;x++) {
            if (verbose) {
                fprintf(stderr,"%s\n",argv[x]);
            }
            do_file(argv[x], strlen(argv[x]));
        }
    } while (endless !=0); // will run forever if endless option is set
    close(fd); // make sure we wait till buffers are empty
    return 0;
}


/*
 * do_file: dump the file out
 */

static void do_file(gs_sp_t filename, gs_int32_t fnlen) {
    gs_int32_t pipe, parserversion, schemalen;
    FILE *input;
    gs_int8_t cmd2[4096 + 128];
    static gs_int8_t *dbuf;
    size_t sz;
    
    if (fnlen > 3 && filename[fnlen - 3] == '.' &&
        filename[fnlen - 2] == 'g' &&
        filename[fnlen - 1] == 'z') {
        pipe = 1;
        snprintf(cmd2, sizeof(cmd2), "gzcat %s", filename);
        input = popen(cmd2, "r");
    } else {
        if (fnlen > 3 && filename[fnlen - 3] == 'b' &&
            filename[fnlen - 2] == 'z' &&
            filename[fnlen - 1] == '2') {
            pipe = 1;
            snprintf(cmd2, sizeof(cmd2), "bzcat %s", filename);
            input = popen(cmd2, "r");
        } else {
            pipe=0;
            input = fopen(filename, "r");
        }
    }
    
    if (!input) {
        perror("stream open");
        fprintf(stderr, "%s: cannot open %s\n", me, filename);
        return;
    }
    
    if (fscanf(input, "GDAT\nVERSION:%u\nSCHEMALENGTH:%u\n",
               &parserversion,&schemalen) != 2) {
        fprintf(stderr,"%s: cannot parse GDAT file header in '%s'\n",
                me, filename);
        exit(1);
    }
    
    /* first time ? */
    if (schematext == 0) {
        gs_uint8_t buf[1024];
        schematextlen = schemalen;
        schematext = malloc(schemalen);
        dbuf = malloc(CATBLOCKSZ);
        if (!schematext  || !dbuf) {
            fprintf(stderr,"%s: malloc error reading GDAT file header in '%s'\n",
                    me, filename);
            exit(1);
        }
        if (fread(schematext, schemalen, 1, input) != 1) {
            fprintf(stderr,"%s: cannot parse-read GDAT file header in '%s'\n",
                    me, filename);
            exit(1);
        }
        sprintf((char *)buf,"GDAT\nVERSION:%u\nSCHEMALENGTH:%u\n", parserversion, schemalen);
        gs_write((gs_sp_t)buf,strlen((const char*)buf));
        gs_write(schematext, schemalen);
    } else {
        schematmp = malloc(schemalen);
        if (!schematmp ) {
            fprintf(stderr,"%s: malloc error reading GDAT file header in '%s'\n",
                    me, filename);
            exit(1);
        }
        if (fread(schematmp, schemalen, 1, input) != 1) {
            fprintf(stderr,"%s: cannot parse-read GDAT file header in '%s'\n",
                    me, filename);
            exit(1);
        }
        free(schematmp);
        //   if (memcmp(schematext, schematmp, schematextlen)) {
        //     fprintf(stderr,"%s: GDAT schema mis-match in file '%s'\n",
        //             me, filename);
        //     exit(1);
        //   }
    }
    
    while ((sz = fread(dbuf, 1, CATBLOCKSZ, input)) > 0) {
        gs_write(dbuf,sz);
    }
    
    if (pipe) {
        pclose(input);
    } else {
        fclose(input);
    }
    
}
