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
#include <string.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "gsconfig.h"
#include "gstypes.h"
#include <schemaparser.h>
#include <gshub.h>

#define MAXSTRLEN 256
#define MAXNUMFIELDS 64

//#define GSBZIP2

#ifdef GSBZIP2
#include "bzlib.h"
#endif

#ifdef ZLIB
#include "zlib.h"
#endif

gs_sp_t me;    	/* saved copy argv[0] */

struct FTA_state {
    FTAID fta_id;
    gs_schemahandle_t schema;
    gs_sp_t asciischema;
    gs_int32_t numfields;
    gs_int32_t timefieldoffset;
};

struct gsasciiprint_state {
    // configuration state
    gs_uint32_t bufsz;
    gs_int32_t stream;
    gs_int32_t flush;
    gs_int32_t parserversion;
    gs_int32_t compressed;
    gs_int32_t verbose;
    gs_int32_t notemp;
    gs_sp_t timefield;
    gs_int32_t  interval;
    gs_int32_t  quitcnt;
    gs_int32_t  quittime;
    gs_int32_t  numfields;
    gs_sp_t extension;
    gs_sp_t query;
    gs_int32_t remote_print;
    
    
    // runtime state
    struct FTA_state fs;
};


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


static void print_usage_exit(gs_sp_t reason) {
    fprintf(stderr,
            "%s::error: %s\n"
            "%s::usage: %s -r <int> -v -f -s -z -c <int> -q <int> -b <field> -t <int> -e <string> <gshub-hostname>:<gshub-port> <gsinstance_name>   <query_name> <parameters>\n"
            "\t-v makes output verbose\n"
            "\t-f flushes each tuple individually\n"
            "\t-h <int> print within the hfta int identifies how many output streams based on the SubInterface field\n"
            "\t-r sets the ringbuffer size default is 8MB\n"
            "\t-s uses %s in streaming mode cannot be used with -v -z -b -t -e\n"
            "\t-c <int> indicates that %s should terminate after <int> tuples.\n"
            "\t-q <int> indicates that %s should terminate after <int> seconds.\n"
            "\t-z the output is compressed with gzip \n"
            "\t-b <field> identifies the field which is increasing and is used to\n"
            "\t\tbin output files. Field has to be of type uint.\n"
            "\t\tThe default is the UNIX system time\n"
            "\t-t <int> specifies the number the <field> specified with -b\n"
            "\t\thas to increase before a new output file is used\n"
            "\t\tthe default is 60 for 1 minute if the default UNIX time is used\n"
            "\t-e <string> identifies the file string extension used for the \n"
            "\t\toutput file. The output file always starts with the current value\n"
            "\t\tof the b field (or UNIX time) the default extension is .txt \n"
            "\t\twithout the -z option and .txt.gz with the -z option\n"
            "\t<query_name> specifies the query which will be\n"
            "\t\t instanciated.\n"
            "\t<parameters> sets the parameters for the query\n"
            , me, reason, me, me, me,me,me);
    exit(1);
}




static void init(gs_int32_t argc, gs_sp_t argv[], struct gsasciiprint_state *s) {
    void *pblk;
    gs_int32_t x, y, schema, pblklen, lcv;
    gs_sp_t c;
    gs_int8_t name[1024];
    
    sprintf(name,"gsgdatprint:%s",argv[0]);
    
    gsopenlog(name);
    
    
    if (s->verbose!=0) gslog(LOG_DEBUG,"Initializing gscp\n");
    if (ftaapp_init(s->bufsz)!=0) {
        gslog(LOG_EMERG,"%s::error:could not initialize gscp\n", me);
        exit(1);
    }
    
    signal(SIGTERM, hand);
    signal(SIGINT, hand);
    signal(SIGPIPE, hand);
    
    if (s->verbose!=0) gslog(LOG_DEBUG,"Initializing FTAs\n");
    
    schema = ftaapp_get_fta_schema_by_name(argv[0]);
    if (schema < 0) {
        gslog(LOG_EMERG,"%s::error:can't get fta '%s' schema\n", me ,argv[0]);
        exit(1);
    }
    if (!ftaschema_parameter_len(schema)) {
        pblk = 0;
        pblklen = 0;
        if (s->verbose) gslog(LOG_DEBUG,"[query does not have any params]\n");
    } else {
        /* parse the params */
        for (lcv = 1 ; lcv < argc ; lcv++) {
            char *k, *e;
            int rv;
            k = argv[lcv];
            e = k;
            while (*e && *e != '=') e++;
            if (*e == 0) {
                gslog(LOG_EMERG,"param parse error '%s' (fmt 'key=val')\n",
                      argv[lcv]);
                exit(1);
            }
            *e = 0;
            rv = ftaschema_setparam_by_name(schema, k, e+1, strlen(e+1));
            *e = '=';
            if (rv < 0) {
                gslog(LOG_EMERG,"param setparam error '%s' (fmt 'key=val')\n",
                      argv[lcv]);
                exit(1);
            }
        }
        if (ftaschema_create_param_block(schema, &pblk, &pblklen) < 0) {
            gslog(LOG_EMERG, "ftaschema_create_param_block failed!\n");
            exit(1);
        }
    }
    ftaschema_free(schema); /* XXXCDC */
    
    
	if (s->remote_print>=0) {
		s->fs.fta_id=ftaapp_add_fta_print(s->query,pblk?0:1,pblk?0:1,0,pblklen,pblk,"./data",s->extension,s->timefield,"SubInterface",s->interval,s->remote_print);
		if (s->fs.fta_id.streamid==0){
            gslog(LOG_EMERG,"%s::error:could not initialize fta_print %s\n",
                  me,s->query);
            exit(1);
		}
    again:
        // wait forever
        sleep(60);
        goto again;
	}
    
    
    s->fs.fta_id=ftaapp_add_fta(s->query,pblk?0:1,pblk?0:1,0,pblklen,pblk);
    if (s->fs.fta_id.streamid==0){
        gslog(LOG_EMERG,"%s::error:could not initialize fta %s\n",
              me,s->query);
        exit(1);
    }
    /* XXXCDC: pblk is malloc'd, should we free it? */
    
    if ((c=ftaapp_get_fta_ascii_schema_by_name(s->query))==0){
        gslog(LOG_EMERG,"%s::error:could not get ascii schema for %s\n",
              me,s->query);
        exit(1);
    }
    
    //ftaapp_get_fta_ascii_schema_by_name uses static buffer so make a copy
    s->fs.asciischema=strdup(c);
    
    
    // Set parser version here
    s->parserversion=get_schemaparser_version();
    
    if ((s->fs.schema=ftaapp_get_fta_schema(s->fs.fta_id))<0) {
        gslog(LOG_EMERG,"%s::error:could not get schema for query\n",
              me,s->query);
        exit(1);
    }
    
    // Use all available fields
    if ((s->fs.numfields=ftaschema_tuple_len(s->fs.schema))<0) {
        gslog(LOG_EMERG,"%s::error:could not get number of fields for query %s\n",
              me,s->query);
        exit(1);
    }
    if (s->timefield!=0) {
        if ((s->fs.timefieldoffset=ftaschema_get_field_offset_by_name(
                                                                      s->fs.schema,s->timefield))<0) {
            gslog(LOG_EMERG,"%s::error:could not get "
                  "offset for timefield %s in query %s\n",
                  me,s->timefield,s->query);
            exit(1);
        }
        
        if (ftaschema_get_field_type_by_name(
                                             s->fs.schema,s->timefield)!=UINT_TYPE) {
            gslog(LOG_EMERG,"%s::error: illegal type for timefield "
                  "%s in query %s UINT expected\n",
                  me,s->timefield,s->query);
            exit(1);
        }
    }
}


static void process_data(struct gsasciiprint_state *s)
{
	gs_int32_t x;
    gs_int32_t y;
    gs_int32_t z;
    gs_int32_t tb;
    gs_uint32_t nsz;
    FTAID rfta_id;
    gs_uint32_t rsize;
    gs_int8_t rbuf[2*MAXTUPLESZ];
    gs_int32_t problem;
    
    gs_int32_t ctb=0;
    gs_int8_t fname[1024];
    gs_int8_t tmpname[1024];
    gs_int8_t command[1024];
	gs_int8_t topb[1024];
    gs_int32_t rcnt=0;
    gs_retval_t code;
#ifdef GSBZIP2
	int bzerror;
	BZFILE * b;
	int abandon=0;
	unsigned int bytes_in;
	unsigned int bytes_out;
#endif
#ifdef ZLIB
	gzFile of=0;
#else
	FILE * of=0;
#endif
    
    
    tmpname[0]=0;
    
    if (s->verbose!=0) gslog(LOG_INFO,"Getting Data for %s",s->query);
    
	sprintf(&topb[0],"GDAT\nVERSION:%u\nSCHEMALENGTH:%lu\n",
			s->parserversion,strlen(s->fs.asciischema)+1);
    if (s->stream!=0) {
		of=stdout;
		// need to get ASCII version of schema
		fwrite(&topb[0],strlen(topb),1,of);
		fwrite(s->fs.asciischema,strlen(s->fs.asciischema)+1,1,of);
    }
    
    if (s->quittime !=0 ) {
		signal(SIGALRM,timeouthand);
		alarm(s->quittime);
    }
    
    
    while((code=ftaapp_get_tuple(&rfta_id,&rsize,rbuf,2*MAXTUPLESZ,0))>=0) {
		rcnt++;
		if ((s->notemp==1) && (code==2)) continue;
		if (((s->quitcnt>0) && (rcnt>s->quitcnt))||ftaschema_is_eof_tuple(s->fs.schema, rbuf)) {
			if (s->verbose!=0)
				gslog(LOG_EMERG, "exiting reached tuple limit or all data has been proccessed\n");
			if (of!=0) {
				fclose(of);
                
				if (s->compressed) {
					system(command);
				}
				rename(tmpname,fname);
			}
			exit(0);
		}
		if (((code==0) || (code==2))&&(s->fs.fta_id.streamid==rfta_id.streamid)) {
			if (s->stream==0) {
				if (s->timefield!=0) {
					tb=fta_unpack_uint(rbuf,
                                       rsize,
                                       s->fs.timefieldoffset,
                                       &problem);
				} else {
					tb=time(0);
				}
				if ((ctb+s->interval)<=tb) {
					gsstats();
					if (of!=0) {
#ifdef GSBZIP2
						BZ2_bzWriteClose(&bzerror, b, abandon, &bytes_in, &bytes_out );
						if (bzerror!=BZ_OK) {
							gslog(LOG_EMERG,"Could not bz close file .. EXITING\n");
							exit(0);
						}
#endif
#ifdef ZLIB
						gzclose(of);
#else
						fclose(of);
#endif
#ifndef GSBZIP2
						if (s->compressed) {
							system(command);
						}
#endif
						rename(tmpname,fname);
					}
					while((ctb+s->interval)<=tb) {
						if (ctb==0) {
							ctb=(tb/s->interval)*s->interval;
						} else {
							ctb=ctb+s->interval;
						}
					}
#ifdef ZLIB
					sprintf(tmpname,"%u%s.gz.tmp",ctb,s->extension);
					sprintf(fname,"%u%s.gz",ctb,s->extension);
#else
                    sprintf(tmpname,"%u%s.tmp",ctb,s->extension);
                    sprintf(fname,"%u%s",ctb,s->extension);
#endif
#ifndef GSBZIP2
					if (s->compressed) {
						sprintf(command,"gzip -S .tmpgz %s ; mv %s.tmpgz %s",tmpname,tmpname,tmpname);
					}
#endif
#ifdef ZLIB
					if ((of=gzopen(tmpname,"wb"))==0) {
						gslog(LOG_EMERG,"Could not open file \"%s\".. EXITING\n",
                              tmpname);
						exit(0);
					}
#else
					if ((of=fopen(tmpname,"w"))==0) {
						gslog(LOG_EMERG,"Could not open file \"%s\".. EXITING\n",
                              tmpname);
						exit(0);
					}
#endif
#ifdef GSBZIP2
					if (s->compressed) {
						b=BZ2_bzWriteOpen(&bzerror,of,5,0,30);
						if (bzerror!=BZ_OK) {
							gslog(LOG_EMERG,"Could not bz open file \"%s\".. EXITING\n",
                                  tmpname);
							exit(0);
						}
					}
#endif
                    
					if (s->compressed) {
#ifdef GSBZIP2
						BZ2_bzWrite ( &bzerror, b, &topb[0],strlen(topb));
						BZ2_bzWrite ( &bzerror, b, s->fs.asciischema,strlen(s->fs.asciischema)+1);
#endif
					} else {
						// need to get ASCII version of schema
#ifdef ZLIB
						gzwrite(of,&topb[0],strlen(topb));
						gzwrite(of,s->fs.asciischema,strlen(s->fs.asciischema)+1);
#else
						fwrite(&topb[0],strlen(topb),1,of);
						fwrite(s->fs.asciischema,strlen(s->fs.asciischema)+1,1,of);
#endif
					}
				}
			}
			if (code==0) {
				nsz=htonl(rsize);
				if (s->compressed) {
#ifdef GSBZIP2
					BZ2_bzWrite ( &bzerror, b,&nsz,sizeof(gs_uint32_t) );
					BZ2_bzWrite ( &bzerror, b,rbuf,rsize);
#endif
				} else {
#ifdef ZLIB
					gzwrite(of,&nsz,sizeof(gs_uint32_t));
					gzwrite(of,rbuf,rsize);
				}
#else
                if (fwrite(&nsz,sizeof(gs_uint32_t),1,of)!=1) {
                    ftaapp_exit();
                    if (s->verbose!=0)
                        gslog(LOG_EMERG,"Could not write to output\"%s\".. EXITING\n",
                              tmpname);
                    exit(0);
                }
                
                if (fwrite(rbuf,rsize,1,of)!=1) {
                    ftaapp_exit();
                    if (s->verbose!=0)
                        gslog(LOG_EMERG,"Could not write to output\"%s\".. EXITING\n",
                              tmpname);
                    exit(0);
                }
                if ((s->stream!=0) || (s->flush!=0)) {
                    fflush(of);
                }
            }
#endif
        }
    }
}
}



int main(int argc, char** argv) {
    
    struct gsasciiprint_state s;
    gs_retval_t ch;
    endpoint gshub;
    endpoint dummyep;
    gs_uint32_t tip1,tip2,tip3,tip4;
    gs_sp_t instance_name;
    
    me = argv[0];
    
    /* initialize host library and the sgroup  */
    
    if (argc<=1) {
        print_usage_exit("Not enough arguments");
    }
    
    /* parse args */
    bzero(&s, sizeof(s));
    s.interval = 60;	/* default */
    s.quittime=0;
    s.quitcnt=-1;
    s.remote_print=-1;
    s.bufsz=8*1024*1024;
    
    while ((ch = getopt(argc, argv, "nr:b:e:t:c:q:sfvzmh:")) != -1) {
        switch(ch) {
            case 'r':
                s.bufsz=atoi(optarg);
                break;
            case 'c':
                s.quitcnt = atoi(optarg);
                break;
            case 'q':
                s.quittime = atoi(optarg);
                break;
            case 'b':
                s.timefield = optarg;
                break;
            case 'e':
                s.extension = optarg;
                break;
            case 'n':
                s.notemp=1;
                break;
            case 't':
                s.interval = atoi(optarg);
                if (s.interval < 1) {
                    goto usage;
                }
                break;
            case 's':
                s.stream++;
                break;
            case 'f':
                s.flush++;
                break;
            case 'v':
                s.verbose++;
                break;
            case 'z':
                s.compressed++;
                break;
            case 'h':
                s.remote_print=atoi(optarg);
                break;
            default:
            usage:
                print_usage_exit("invalid args");
        }
    }
    
    if ((s.stream!=0) & (s.compressed!=0 | s.verbose!=0 |
                         s.timefield!=0 | s.extension!=0)) {
        print_usage_exit("illegal argument combination with -s");
    }
    
    if (!s.extension) {
        s.extension = (s.compressed) ? ".txt.gz" : ".txt";
    }
    
    argc -= optind;
    argv += optind;
    if (argc<3) print_usage_exit("must specify hub info and query");
    
    if (sscanf(argv[0],"%u.%u.%u.%u:%hu",&tip1,&tip2,&tip3,&tip4,&(gshub.port))!= 5 ) {
        gslog(LOG_EMERG,"HUB IP NOT DEFINED");
        exit(1);
    }
    gshub.ip=htonl(tip1<<24|tip2<<16|tip3<<8|tip4);
    gshub.port=htons(gshub.port);
    instance_name=strdup(argv[1]);
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
    
    
    argc -=2;
    argv +=2;
    
    s.query = argv[0];
    
    init(argc, argv, &s);
    
    process_data(&s);
    
    gslog(LOG_EMERG,"%s::internal error reached unexpected end", me);
}

