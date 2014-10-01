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

/*
 * gslog.h: syslog wrappers for Tigon-SQL
 */
#ifndef GSLOG_H
#define GSLOG_H


#include "unistd.h"
#include "sys/syslog.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "stdarg.h"
#include "time.h"
#include "gsconfig.h"
#include "gstypes.h"

#ifdef LOGSTDERR
static inline void gslog(int loglevel, const char * format, ...) {
    va_list args;
    va_start(args, format);
    if (loglevel >= LOG_WARNING) {
        vfprintf(stdout, format, args);
        // Not generally syslog messages do not contain a \n so we need to add one for stdout/stderr
        fprintf(stdout, "\n");
        fflush(stdout);
    } else {
        //STDERR is not-buffered by default.
        vfprintf(stderr, format, args);
        fprintf(stderr, "\n");
    }
    va_end(args);
}

static inline void gsopenlog(gs_sp_t p) {
    return;
}

#else

#define gslog syslog

#ifndef LOG_EMERG
#define LOG_EMERG 7
#endif

// some state used for reporting

extern gs_uint64_t intupledrop;
extern gs_uint64_t outtupledrop;
extern gs_uint64_t intuple;
extern gs_uint64_t outtuple;
extern gs_uint64_t inbytes;
extern gs_uint64_t outbytes;
extern gs_uint64_t cycles;


static inline void gsopenlog(gs_sp_t p) {
	gs_int8_t c[HOST_NAME_MAX+1];
	gs_int8_t t[HOST_NAME_MAX+1+1000];
	gs_sp_t t2;
	if (gethostname(&c[0],HOST_NAME_MAX+1)!=0) {
		fprintf(stderr,"GSCPV1::ERROR:could not get hostname\n");
		exit(1);
	}
	c[HOST_NAME_MAX]=0;
	sprintf(t,"GSCPv2:%s:%s:",c,p);
	t2=strdup(t);
	openlog(t2,LOG_NOWAIT|LOG_PID|LOG_CONS,LOG_LOCAL5);
	gslog(LOG_INFO,"Started Logging");
	intupledrop=0;
	outtupledrop=0;
	intuple=0;
	outtuple=0;
	inbytes=0;
	outbytes=0;
	cycles=0;
}
#endif

static inline void gsstats() {
	gs_uint32_t t;
	//t=time(0);
	//gslog(LOG_NOTICE,"STATS|%u|%llu|%llu|%llu|%llu|%llu|%llu|%llu",t,intuple,inbytes,intupledrop,outtuple,outbytes,outtupledrop,cycles);
}

#endif
