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

#include "parse_fta.h"
#include "parse_schema.h"
#include"generate_utils.h"

#include <string>
#include<set>

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include<errno.h>

#define TMPSTRLEN 1000

#ifndef PATH_DELIM
  #define PATH_DELIM '/'
#endif

char tmp_schema_str[10000];

//		Interface to FTA definition lexer and parser ...

extern int FtaParserparse(void);
extern FILE *FtaParserin;
extern int FtaParserdebug;

fta_parse_t *fta_parse_result;
var_defs_t *fta_parse_defines;



using namespace std;




int main(int argc, char **argv){
	int s,t,f;
  char tmpstr[TMPSTRLEN];



//				set these to 1 to debug the parser
  FtaParserdebug = 0;

  FILE *table_schemas_in;		// source tables definition file

  // -------------------------------
  // Handling of Input Arguments
  // -------------------------------
    const char *optstr = "C:";
	const char *usage_str = "Usage: %s [-C <config directory>] schema_file [operator name ...]\n"
        "\t[-C] : use <config directory> for definition files\n";


//		parameters gathered from command line processing
 	string config_dir_path;
	set<string> operator_names;
	string schema_file_name;

   char chopt;
   while((chopt = getopt(argc,argv,optstr)) != -1){
		switch(chopt){
 		case 'C':
				if(optarg != NULL)
 		        	 config_dir_path = string(optarg) + string("/");
 			break;
		case '?':
			fprintf(stderr,"Error, argument %c not recognized.\n",optopt);
			fprintf(stderr,"%s\n", usage_str);
			exit(1);
		default:
			fprintf(stderr,"Invalid arguments\n");
			fprintf(stderr,"%s\n", usage_str);
			exit(1);
		}
	}
	argc -= optind;
	argv += optind;
	for (int i = 0; i < argc; ++i) {
		if(schema_file_name == ""){
			schema_file_name = argv[i];
		}else{
			operator_names.insert(argv[i]);
		}
	}

	if(schema_file_name == ""){
		fprintf(stderr,"%s\n", usage_str);
		exit(1);
	}


//			Open globally used file names.

 	// prepend config directory to schema file
 	schema_file_name = config_dir_path + schema_file_name;

	if((table_schemas_in = fopen(schema_file_name.c_str(), "r")) == NULL) {
		fprintf(stderr,"Can't open schema file %s\n%s\n",schema_file_name.c_str(),strerror(errno));
		exit(1);
	}




//			Get an initial Schema
	table_list *Schema;
//			Parse the table schema definitions.
	fta_parse_result = new fta_parse_t();
	FtaParser_setfileinput(table_schemas_in);
	if(FtaParserparse()){
		fprintf(stderr,"Table schema parse failed.\n");
		exit(1);
	}
	if(fta_parse_result->parse_type != TABLE_PARSE){
	  	fprintf(stderr,"ERROR, file %s is not a table definition file.\n",schema_file_name.c_str());
	  	exit(1);
	}
	Schema = fta_parse_result->tables;

//			Process schema field inheritance
	string err_str;
	int retval;
	retval = Schema->unroll_tables(err_str);
	if(retval){
		fprintf(stderr,"Error processing schema filed inheritance:\n %s\n", err_str.c_str() );
		exit(1);
	}

//			Open header,  code files

	FILE *hdr_fl = fopen("operator_tuple_pack.h", "w");
	if(hdr_fl == NULL){
		fprintf(stderr,"Can't open output header file operator_tuple_pack.h\n%s\n",strerror(errno));
		exit(1);
	}
	FILE *code_fl = fopen("operator_tuple_pack.cc", "w");
	if(code_fl == NULL){
		fprintf(stderr,"Can't open output code file operator_tuple_pack.c\n%s\n",strerror(errno));
		exit(1);
	}





//			Generate code for the accessing the operator tuples.

	string header =
"#ifndef __OPERATOR_TUPLE_PACK__\n"
"#define __OPERATOR_TUPLE_PACK__\n"
"\n"
"#include \"host_tuple.h\"\n"
"#include \"vstring.h\"\n"
"#include \"byteswap.h\""
"\n"
"void *unpack_operator_params(void *buf, int buflen, char **param_list, int plist_len, int *params_found);\n"
"\n"
"\n";

	string code =
"#include \"operator_tuple_pack.h\"\n"
"#include <sys/param.h>\n"
"#include \"fta.h\"\n"
"#include \"lapp.h\"\n"
"#include \"hfta_runtime_library.h\"\n"
"\n"
"void *unpack_operator_params(void *buf, int buflen, char **param_list, int plist_len, int *params_found){\n"
"	char *c=(char *)buf;\n"
"    int pno=0, pos=0;\n"
"	*params_found = 0;\n"
"\n"
"	for(pos=0;pos<buflen;++pos){\n"
"		if(c[pos]=='\\n' || c[pos]=='\\0') return buf;\n"
"		if(c[pos]==':') break;\n"
"	}\n"
"	if(pos == buflen) return buf;\n"
"\n"
"	pos++; \n"
"	param_list[pno]=c+pos;\n"
"	pno++;\n"
"	\n"
"	for(;pos<buflen;++pos){\n"
"		if(c[pos]==','){\n"
"			c[pos]='\\0';\n"
"			pos++;\n"
"			param_list[pno]=c+pos;\n"
"			pno++;\n"
"		}\n"
"		if(c[pos]=='\\n'){\n"
"			c[pos]='\\0';\n"
"			pos++;\n"
"			*params_found = pno;\n"
"			return c+pos;\n"
"		}\n"
"	}\n"
"	return buf;\n"
"}\n"
"\n"
"\n";

	set<string> ufcns;
	vector<string> tbl_names = Schema->get_table_names();
	for(t=0;t<tbl_names.size();++t){
		if(operator_names.size()==0 || operator_names.count(tbl_names[t])>0){
			int tid = Schema->find_tbl(tbl_names[t]);
			if(Schema->get_schema_type(tid) == OPERATOR_VIEW_SCHEMA){
				table_def *optbl = Schema->get_table(tid);
				header += generate_host_name_tuple_struct(optbl);
				header += "int pack_"+tbl_names[t]+"_tuple(host_tuple *tup, char *buf, int len, struct "+generate_tuple_name(tbl_names[t])+" *s, unsigned char tuple_type);\n\n";
				code += generate_host_tuple_pack(optbl);

				header += "extern char *"+tbl_names[t]+"_Schema_def_;\n\n";
				code += "\tchar *"+tbl_names[t]+"_Schema_def_ = \n";
				string schema_str = "FTA{\n\n";
				schema_str += optbl->to_stream_string();
				schema_str +=
"DEFINE{\n"
"\tquery_name '"+tbl_names[t]+"';\n"
"}\n"
"\n"
"Select";
				vector<field_entry *> flds = optbl->get_fields();
				for(f=0;f<flds.size();++f){
					if(f>0) schema_str+=",";
					schema_str += " " + flds[f]->get_name();
				}
				schema_str += "\nFrom "+tbl_names[t]+"\n}\n\n";
				code += make_C_embedded_string(schema_str)+";\n\n";


				vector<subquery_spec *> subq = Schema->get_subqueryspecs(tid);
				for(s=0;s<subq.size();++s){
					subquery_spec *sqs = subq[s];
					field_entry_list fel;
					for(f=0;f<sqs->types.size();++f){
						fel.append_field(new field_entry(sqs->types[f].c_str(), sqs->names[f].c_str(), "", sqs->modifiers[f],ufcns));
					}
					string subq_name = tbl_names[t]+"_subq_"+sqs->name;
					table_def std(subq_name.c_str(),NULL,NULL,&fel, STREAM_SCHEMA);
					header += generate_host_name_tuple_struct(&std);
					header += "struct "+subq_name+"_tuple *unpack_"+subq_name+"_tuple(host_tuple *tup);\n\n";
					code += generate_host_tuple_unpack(&std);
				}
			}else{
				if(operator_names.size()>0)
					fprintf(stderr,"WARNING: %s is not an operator, skipping.\n",tbl_names[t].c_str());
			}
		}
	}

	header += "\n#endif\n";


	fprintf(hdr_fl,"%s\n",header.c_str());
	fprintf(code_fl,"%s\n",code.c_str());

///////////////////////////////////////////////////////////////////////

	return(0);
}

