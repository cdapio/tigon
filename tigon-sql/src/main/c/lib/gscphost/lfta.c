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
#include <gsconfig.h>
#include <fta.h>
#include <callbackregistries.h>
#include <ipcencoding.h>
#include <stdio.h>
#include "rdtsc.h"
#include "packet.h"
#include "gscpipc.h"

gs_uint64_t lfta_prefilter(struct packet * p);

struct pr_stats {
	gs_uint64_t process;
	gs_uint64_t packets;
};

void rts_fta_process_packet(struct packet * p)
{
    struct FTA * f;
	gs_uint64_t prefilter;
	static gs_uint32_t t=0;
	static struct pr_stats s = {0};
	gs_uint64_t s1;
	gs_uint64_t s2;
    prefilter=lfta_prefilter(p);
    
    if (ftaexec_start()<0) {
        gslog(LOG_EMERG,"GSCPRTS::error::could not init "
              "FTA list itteration\n");
        return;
    }
    s1=rdtsc();
    while ((f=ftaexec_next())!=0) {
        //		fprintf(stderr,"%llu %llu %llu\n",f->prefilter,prefilter,~( (prefilter&(f->prefilter)) | (~(f->prefilter)) ));
		if (~( (prefilter&(f->prefilter)) | (~(f->prefilter)) ) == 0)
			f->accept_packet(f,0,p,0);
    }
    s2=rdtsc();
    s.process+=(s2-s1);
    s.packets++;
    intuple++;
    cycles+=(s2-s1);
}

void rts_fta_done()
{
    struct FTA * f;
    if (ftaexec_start()<0) {
        gslog(LOG_EMERG,"GSCPRTS::error::could not init "
              "FTA list itteration\n");
        return;
    }
    while ((f=ftaexec_next())!=0) {
        f->control_fta(f,FTA_COMMAND_FILE_DONE,0,0);
    }
}
