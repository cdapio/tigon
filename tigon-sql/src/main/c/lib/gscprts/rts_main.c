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
#include "gshub.h"

#include "lapp.h"
#include "fta.h"
#include "lfta/rts.h"

#include "stdio.h"
#include "stdlib.h"
#include <sys/types.h>
#include <unistd.h>

#include <sys/ipc.h>
#include <sys/shm.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>

gs_retval_t main_csv(gs_int32_t devicenum, gs_sp_t device, gs_int32_t mapcnt, gs_sp_t map[]);
gs_retval_t main_gdat(gs_int32_t devicenum, gs_sp_t device, gs_int32_t mapcnt, gs_sp_t map[]);

int main (int argc, char* argv[]) {
    gs_int32_t pid;
    gs_int32_t x;
    gs_int32_t y;
    gs_sp_t* device;
    gs_int32_t devcnt=0;
    gs_sp_t* mappings;
    gs_int32_t mapcnt=0;
    gs_sp_t* lmap;
    gs_int32_t lmapcnt=0;
    FILE * cfg_file;
    gs_int32_t tip1,tip2,tip3,tip4;
    endpoint gshub;
    gs_sp_t instance_name;
    
    
	gsopenlog(argv[0]);
    
    if (setpgid(0,0)<0) {
        gslog(LOG_EMERG,"Could not set process group id of rts");
        exit(1);
    }
	
    if (argc<4) {
		gslog(LOG_EMERG,"Wrong arguments at startup");
        exit(1);
    }
    
    /* allocate more than enough for each array */
    if ((device=malloc(sizeof(gs_sp_t*) * argc))==0) {
        gslog(LOG_EMERG,"malloc error");
        exit(1);
    }
    if ((mappings=malloc(sizeof(gs_sp_t*) * argc))==0) {
        gslog(LOG_EMERG,"malloc error");
        exit(1);
    }
    if ((lmap=malloc(sizeof(gs_sp_t*) * argc))==0) {
        gslog(LOG_EMERG,"malloc error");
        exit(1);
    }
    
    /* parse the arguments */
    
    if ((sscanf(argv[1],"%u.%u.%u.%u:%hu",&tip1,&tip2,&tip3,&tip4,&(gshub.port))!=5)) {
        gslog(LOG_EMERG,"HUB IP NOT DEFINED");
        exit(1);
    }
    gshub.ip=htonl(tip1<<24|tip2<<16|tip3<<8|tip4);
    gshub.port=htons(gshub.port);
    instance_name=strdup(argv[2]);
    if (set_hub(gshub)!=0) {
        gslog(LOG_EMERG,"Could not set hub");
        exit(1);
    }
    if (set_instance_name(instance_name)!=0) {
        gslog(LOG_EMERG,"Could not set instance name");
        exit(1);
    }
    
    for(x=3;x<argc;x++) {
        if (strncmp("-D",argv[x],2)==0) {
            /* macro definition */
            y=2;
            while((y<(strlen(argv[x])-1))&&(argv[x][y]!='=')) y++;
            
            
            if (y<(strlen(argv[x])-1)) {
                /* if that is not true the define is empty and
                 we ignore it otherwise we set the = to 0 to
                 make two strings out of it*/
                argv[x][y]=0;
                mappings[mapcnt]=&(argv[x][2]);
                mapcnt++;
            }
        } else {
            /* device definition */
            device[devcnt]=argv[x];
            devcnt++;
        }
    }
    
    if (devcnt==0) {
        gslog(LOG_EMERG,"at least one device has to be specified");
        exit(1);
    }
	
    
    /* now startup all the device dependend processes. */
    
    for (x=0;x<devcnt;x++) {
        if ((pid=fork())==-1) {
            gslog(LOG_EMERG,"fork error");
            exit(1);
        }
        
        if (pid==0) {
            gs_sp_t interfacetype;
            /* wait for clearinghouse to finish startup */
            sleep(2);
            for(y=0;y<mapcnt;y++) {
                if (strcmp(device[x],&mappings[y][strlen(mappings[y])+1])==0) {
                    /* point to the second string */
                    lmap[lmapcnt]=mappings[y];
                    lmapcnt++;
                }
            }
            /* the devicename always matches */
            lmap[lmapcnt]=device[x];
            lmapcnt++;
            
            interfacetype=get_iface_properties(device[x],"interfacetype");
            
            if (strncmp(interfacetype,"CSV",3)==0) {
                main_csv(x,device[x],lmapcnt,lmap);
            } else {
                
           		if (strncmp(interfacetype,"GDAT",4)==0) {
                    main_gdat(x,device[x],lmapcnt,lmap);
                } else {
                    
                    gslog(LOG_ERR,"UNKNOWN InterfaceType\n");
                    exit(0);
                }
            }
            
            /* should never return */
            gslog(LOG_EMERG,"lfta init returned");
            exit(1);	    
        }
    }
    
    /* initalize host_lib */
    
    gslog(LOG_INFO,"Init host lib in clearinghouse");
    
    if (hostlib_init(CLEARINGHOUSE,0,DEFAULTDEV,0,0)<0) {
        gslog(LOG_EMERG,"%s::error:could not initiate host lib for clearinghouse\n",
              argv[0]);
        exit(7);
    }
    
    
    
    /* start processing messages should never return*/
    if (fta_start_service(-1)<0) {
        gslog(LOG_EMERG,"error in processing the msg queue");
        exit(9);
    }
    gslog(LOG_EMERG,"%s::error:start service returned");
    return 0;
}

