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
#include <schemaparser.h>
#include "gsconfig.h"
#include "gstypes.h"

FILE * ifd;

gs_retval_t read_tuple(gs_uint32_t * sz, gs_sp_t data, gs_int32_t maxlen) {
    gs_uint32_t nsz;
    static gs_uint32_t read=0;
again:
    if (fread(&nsz,sizeof(gs_uint32_t),1,ifd)!=1) {
        
        exit(0);
    }
    read=read+sizeof(gs_uint32_t);
    *sz=ntohl(nsz);
    if ((*sz)>maxlen) {
        fprintf(stderr,"INTERNAL ERROR tuple to long for fixed buffer. Tuple sz %u\n",
                (*sz));
        *sz=0;
        return 0;
    }
    if (*sz==0) goto again;
    if (fread(data,(*sz),1,ifd)!=1) {
        fprintf(stderr,"UNEXPECTED END OF FILE. Tryed to read tuple of size %u\n",
                (*sz));
        exit(0);
    }
    read=read+*sz;
    return 0;
}

int main(int argc, char** argv) {
    gs_uint32_t streamid;
    gs_schemahandle_t schema;
    
    gs_uint32_t rstreamid;
    gs_uint32_t rsize;
    gs_int8_t rbuf[2*MAXTUPLESZ];
    
    gs_int32_t numberoffields;
    gs_int32_t verbose=0;
    gs_int32_t truncate=-1;
    gs_int32_t y;
    gs_int32_t parserversion;
    gs_uint32_t schemalen;
    gs_sp_t asciischema;
    gs_sp_t me;
    gs_int32_t ch;
    gs_uint32_t scramble =0;
    gs_uint32_t schema_only =0;
    
    /* initialize host library and the sgroup  */
    
    me=argv[0];
    
    if (argc<2) {
        fprintf(stderr,
                "%s::usage: %s -v -x -t <int> -s <int> <uncompressed_file_name>\n",
                me,me);
        exit(1);
    }
    
    while ((ch = getopt(argc, argv, "ps:t:vx")) != -1) {
        switch(ch) {
            case 'p':
                schema_only=1;
                break;
            case 'v':
                verbose=1;
                break;
            case 'x':
                verbose=2;
                break;
            case 't':
                truncate=atoi(optarg);
                break;
            case 's':
                sscanf(optarg,"%u",&scramble);
        }
    }
    argc -= optind;
    if (argc < 1) {
        fprintf(stderr,"%s::usage: %s -v -x -t <int> <uncompressed_file_name>\n",
                me,me);
        exit(1);
    }
    argv += optind;
    
    if ((strcmp(argv[0],"-")!=0)&&(strcmp(argv[0],"stdin")!=0)) {
        if ((ifd=fopen(argv[0],"r"))==0) {
            fprintf(stderr,"%s::error:could not open input file %s\n",
                    me,argv[0]);
            exit(1);
        }
    } else {
        ifd = stdin;
    }
    
    if (fscanf(ifd,"GDAT\nVERSION:%u\nSCHEMALENGTH:%u\n",&parserversion,&schemalen)!=2) {
        fprintf(stderr,"%s::error:unknown file type for file %s\n",
                me,argv[0]);
        exit(1);
    }
    
    if (schemaparser_accepts_version(parserversion)!=1) {
        fprintf(stderr,"%s::error: wrong parser version %u for file %s\n",
                me,parserversion,argv[0]);
        exit(1);
    }
    
    if ((asciischema=malloc(schemalen))==0) {
        fprintf(stderr,"%s::error: could not allocate schema buffer of sz %u "
                "for file %s\n",
                me,schemalen,argv[0]);
        exit(1);
    }
    
    if (fread(asciischema,schemalen,1,ifd)!=1) {
        fprintf(stderr,"%s::error: could not read schema buffer of sz %u "
                "for file %s\n",
                me,schemalen,argv[0]);
        exit(1);
    }
    if (schema_only==1) {
        fprintf(stdout,"%s\n",asciischema);
    }
    if (verbose==2) {
        fprintf(stderr,"%s\n",asciischema);
    }
    if ((schema=ftaschema_parse_string(asciischema))<0) {
        fprintf(stderr,"%s::error: could not parse schema  "
                "for file %s\n",
                me,argv[0]);
        exit(1);
    }
    
    if ((numberoffields=ftaschema_tuple_len(schema))<0) {
        fprintf(stderr,"%s::error:could not get number of fields in schema\n",
                me);
        exit(1);
    }
    if (verbose==1) {
        for(y=0; y<numberoffields;y++) {
            printf("%s",ftaschema_tuple_name(schema,y));
            if (y<numberoffields-1) printf("|");
        }
        printf("\n");
    }
    while(read_tuple(&rsize,rbuf,2*MAXTUPLESZ)==0) {
        if ((!rsize) || (schema_only==1))
            continue;
        for(y=0; y<numberoffields;y++) {
            struct access_result ar;
            if (verbose==2)
                printf("%s->",ftaschema_tuple_name(schema,y));
            ar=ftaschema_get_field_by_index(schema,y,rbuf,rsize);
            switch (ar.field_data_type) {
                case INT_TYPE:
                    printf("%d",ar.r.i);
                    break;
                case UINT_TYPE:
                    printf("%u",ar.r.ui);
                    break;
                case IP_TYPE:
                    if (scramble!=0) {
                        ar.r.ui=gscpbswap32(ar.r.ui^scramble);
                    }
                    printf("%u.%u.%u.%u",ar.r.ui>>24&0xff,
                           ar.r.ui>>16&0xff,
                           ar.r.ui>>8&0xff,
                           ar.r.ui&0xff);
                    break;
                case IPV6_TYPE:
                {
                    unsigned x;
                    unsigned zc=0;
                    for(x=0;x<4;x++) { if (ar.r.ip6.v[x]==0) zc++;}
                    if (zc!=4) {
                        for(x=0;x<8;x++) {
                            unsigned char * a = (unsigned char *)  &(ar.r.ip6.v[0]);
                            unsigned y;
                            y=((unsigned)a[2*x])<<8|((unsigned) a[2*x+1]);
                            printf("%04x",y);
                            if (x<7) printf(":");
                        }
                    } else {
                        printf("::");
                    }
                }
                    break;
                case USHORT_TYPE:
                    printf("%u",ar.r.ui);
                    break;
                case BOOL_TYPE:
                    if (ar.r.ui==0) {
                        printf("FALSE");
                    } else {
                        printf("TRUE");
                    }
                    break;
                case ULLONG_TYPE:
                {
                    gs_uint64_t ul;
                    gs_uint64_t t1;
                    gs_uint64_t t2;
                    ul=ar.r.ul;
                    printf("%llu",ul);
                }
                    break;
                case LLONG_TYPE:
                    printf("%lld",ar.r.l);
                    break;
                case FLOAT_TYPE:
                    printf("%f",ar.r.f);
                    break;
                case TIMEVAL_TYPE:
                {
                    gs_float_t t;
                    t= ar.r.t.tv_usec;
                    t=t/1000000;
                    t=t+ar.r.t.tv_sec;
                    printf("%lf sec",t);
                }
                    break;
                case VSTR_TYPE:
                {
                    int x;
                    int c;
                    char * src;
                    src=(gs_sp_t)ar.r.vs.offset;
                    if ((truncate>=0) && (ar.r.vs.length>truncate)) {
                        ar.r.vs.length=truncate;
                    }
                    if ((ar.r.vs.length>0) && (src[ar.r.vs.length-1]==0)) {
                        ar.r.vs.length = ar.r.vs.length-1;
                    }
                    for(x=0;x<ar.r.vs.length;x++) {
                        c=src[x];
                        if (((c<='~') && (c>=' '))&&(c!='|')) {
                            printf("%c",c);
                        } else {
                            printf("(0x%x)",(gs_uint8_t)c);
                        }
                    }
                }
                    break;
                default:
                    break;
            }
            if (y<numberoffields-1) printf("|");
        }
        printf("\n");
        if (verbose!=0) fflush(stdout);
    }
    exit(0);
}

