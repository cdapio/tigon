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

#include <string>

#include <stdlib.h>
#include <stdio.h>

#include "schemaparser.h"
#include "schemaparser_impl.h"

//      Interface to FTA definition lexer and parser ...


//double htonf(double f){return f;}
//long long int htonll(long long int l){return l;}


using namespace std;
extern int errno;

// ./test_interfacelib test_param1.q_0.sch  test_param4.q_1.sch



int main(int argc, char **argv){
	int i;
	char tmpstr[1000];
	void * pblock;
	int block_size;
	
	if(argc < 2){
		fprintf(stderr,"Usage: %s schema_file [schema_file ... ]\n",argv[0]);
		exit(1);
	}
	
	int nfiles = argc-1;
	int f;
	for(f=0;f<nfiles;++f){

		FILE *infl = fopen(argv[f+1],"r");
		if(infl == NULL){
			fprintf(stderr,"ERROR can't open schema file %s\n",argv[1]);
			exit(1);
		}

		int sch_handle;
		if(f%2==0){
			sch_handle = ftaschema_parse_file(infl);
		}else{
			string fstr;
			while(fgets(tmpstr, 1000, infl)){
				fstr += tmpstr;
				fstr += "\n";
			}
			sch_handle = ftaschema_parse_string( (char *)(fstr.c_str()) );
		}

		if(sch_handle < 0){
			fprintf(stderr,"ERROR parsing schema file.\n");
			exit(1);
		}

		ftaschema_debugdump(sch_handle);
		
		fclose(infl);
	}
	
	printf("Testing access library.\n");
	
//			test_param1.q_0.sch

	printf("Query %s has %d fields and %d parameters.\n",
		ftaschema_name(0), ftaschema_tuple_len(0), ftaschema_parameter_len(0));
	printf("The parameters are named:\n");
	for(i=0;i<ftaschema_parameter_len(0);++i){
		printf("%s  ",ftaschema_parameter_name(0,i));
	}
	printf("\n");
	printf("Setting parameters by name.\n");
	sprintf(tmpstr,"%d",1111); printf("min_hdr_length : %s\n",tmpstr);
	ftaschema_setparam_by_name(0,"min_hdr_length",tmpstr,strlen(tmpstr));
	sprintf(tmpstr,"%d",2222); printf("unused_param : %s\n",tmpstr);
	ftaschema_setparam_by_name(0,"unused_param",tmpstr,strlen(tmpstr));
	printf("Creating param block.\n");
	ftaschema_create_param_block(0,&pblock, &block_size);
	printf("Block created, size is %d. Accessing by name.\n",block_size);
	printf("\t%s=%d (type is %d) %s=%lu (type is %d)\n",
		"time",
		(ftaschema_get_field_by_name(0,"time",pblock, block_size)).r.i,
		(ftaschema_get_field_by_name(0,"time",pblock, block_size)).field_data_type,
		"hdr_length",
		(ftaschema_get_field_by_name(0,"hdr_length",pblock, block_size)).r.ul,
		(ftaschema_get_field_by_name(0,"hdr_length",pblock, block_size)).field_data_type
	);
	
	printf("Query %s has %d fields and %d parameters.\n",
		ftaschema_name(1), ftaschema_tuple_len(1), ftaschema_parameter_len(1));
	printf("The parameters are named:\n");
	for(i=0;i<ftaschema_parameter_len(1);++i){
		printf("%s  ",ftaschema_parameter_name(1,i));
	}

	printf("\n");
	printf("Setting parameters by index.\n");
	sprintf(tmpstr,"%d",3333); printf("an_ullong_parameter : %s\n",tmpstr);
	ftaschema_setparam_by_index(1,0,tmpstr,strlen(tmpstr));
	sprintf(tmpstr,"%d",2222); printf("ullong_parameter2 : %s\n",tmpstr);
	ftaschema_setparam_by_index(1,1,tmpstr,strlen(tmpstr));
	sprintf(tmpstr,"%f",4444.0); printf("min_prec : %s\n",tmpstr);
	ftaschema_setparam_by_index(1,2,tmpstr,strlen(tmpstr));	
	sprintf(tmpstr,"%s","Foo"); printf("min_str : %s\n",tmpstr);
	ftaschema_setparam_by_index(1,3,tmpstr,strlen(tmpstr)+1);	
	sprintf(tmpstr,"%s","Bar"); printf("min_str : %s\n",tmpstr);
	ftaschema_setparam_by_index(1,3,tmpstr,strlen(tmpstr)+1);	
	ftaschema_create_param_block(1,&pblock, &block_size);
	printf("Block created, size is %d. Accessing by index.\n",block_size);
	printf("\t%s=%ld, type is %d\n",
		ftaschema_tuple_name(1,0),
		(ftaschema_get_field_by_index(1,0,pblock, block_size)).r.ul,
		(ftaschema_get_field_by_index(1,0,pblock, block_size)).field_data_type
	);	
	printf("\t%s=%ld, type is %d\n",
		ftaschema_tuple_name(1,1),
		(ftaschema_get_field_by_index(1,1,pblock, block_size)).r.ul,
		(ftaschema_get_field_by_index(1,1,pblock, block_size)).field_data_type
	);	
	printf("\t%s=%lf, type is %d\n",
		ftaschema_tuple_name(1,2),
		(ftaschema_get_field_by_index(1,2,pblock, block_size)).r.f,
		(ftaschema_get_field_by_index(1,2,pblock, block_size)).field_data_type
	);	
	printf("\t%s=%s (length=%d), type is %d\n",
		ftaschema_tuple_name(1,3),
		(ftaschema_get_field_by_index(1,3,pblock, block_size)).r.vs.offset,
		(ftaschema_get_field_by_index(1,3,pblock, block_size)).r.vs.length,
		(ftaschema_get_field_by_index(1,3,pblock, block_size)).field_data_type
	);	
	
	
	
}
