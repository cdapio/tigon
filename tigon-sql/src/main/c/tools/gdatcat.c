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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>



gs_sp_t me = 0;
gs_sp_t schematext = 0;
gs_int32_t schematextlen = 0;
gs_sp_t schematmp = 0;

static void do_file(gs_sp_t filename, gs_int32_t fnlen);

int main(int argc, char** argv) {
    gs_int32_t x;
    gs_int32_t s=1;
    gs_int32_t debug=0;
    if (strcmp(argv[s],"-d")==0) {
        debug=1;
        s++;
    }
    for(x=s;x<argc;x++) {
        if (debug) {
            fprintf(stderr,"%s\n",argv[x]);
        }
        do_file(argv[x], strlen(argv[x]));
    }
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
        printf("GDAT\nVERSION:%u\nSCHEMALENGTH:%u\n", parserversion, schemalen);
        fwrite(schematext, schemalen, 1, stdout);
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
        fwrite(dbuf, 1, sz, stdout);
    }
    
    if (pipe) {
        pclose(input);
    } else {
        fclose(input);
    }
    
}
