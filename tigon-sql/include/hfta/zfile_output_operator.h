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

#ifndef ZFILE_OUTPUT_OPERATOR_H
#define ZFILE_OUTPUT_OPERATOR_H

#include <app.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>

#include "gsconfig.h"
#include "gstypes.h"
#include <schemaparser.h>

#include "host_tuple.h"
#include "base_operator.h"
#include <list>
#include<string>
#include<vector>
#include "gsconfig.h"
#include "gstypes.h"
#include <schemaparser.h>

#include <zlib.h>

using namespace std;

#ifndef FILESTREAM_OPERATOR_UTILS
#define FILESTREAM_OPERATOR_UTILS
string fs_int_to_string(int i){
    string ret;
    char tmpstr[100];
    sprintf(tmpstr,"%d",i);
    ret=tmpstr;
    return(ret);
}
#endif

template <class file_output_functor> class zfile_output_operator :
		public base_operator
{
private :
	file_output_functor func;

	gzFile *output_file;
	vector<string> xform_command;
	vector<string> temp_fname;
	vector<string> fname;
	string qname;
	string schema;
	string file_header;
	gs_uint32_t n_file_streams;

int total_written;

public:

zfile_output_operator(int schema_handle, gs_csp_t name, gs_csp_t sch) : base_operator(name), func(schema_handle) {
	char tmpstr[1000];
	int i;

	n_file_streams = func.num_file_streams();
	output_file = (gzFile *)(malloc(sizeof(gzFile )*n_file_streams));
	for(i=0;i<n_file_streams;++i){
		output_file[i] = NULL;
		xform_command.push_back("");
		temp_fname.push_back("");
		fname.push_back("");
	}
	qname = name;
	schema = sch;
	sprintf(tmpstr,"GDAT\nVERSION:%u\nSCHEMALENGTH:%lu\n",get_schemaparser_version(),strlen(sch)+1);
	file_header = tmpstr;

total_written = 0;
}



virtual int accept_tuple(host_tuple& tup, list<host_tuple>& result) {
string fname_base;
host_tuple res;
gs_uint32_t i;

	if(! func.temp_status_received(tup)){  // this operator doesn't write
//			Check for EOF conditions.
		if(func.is_eof_tuple()){
			if(output_file[0] != NULL){
				for(i=0;i<n_file_streams;++i){
					gzclose(output_file[i]);
					output_file[i] = NULL;
					rename(temp_fname[i].c_str(), fname[i].c_str());
				}
			}
			if(func.propagate_tuple()){
				tup.channel = output_channel;
				result.push_back(tup);
			}
			return 0;
		}

		if(func.new_epoch()){
			if(output_file[0] != 0){
total_written = 0;
				for(i=0;i<n_file_streams;++i){
					gzclose(output_file[i]);
					rename(temp_fname[i].c_str(), fname[i].c_str());
				}
			}

			fname_base = func.get_filename_base();
			for(i=0;i<n_file_streams;++i){
				if(n_file_streams > 1){
					temp_fname[i] = fname_base + "_stream"+fs_int_to_string(i)+ ".txt.gz.tmp";
					fname[i] = fname_base + "_stream"+fs_int_to_string(i) + ".txt.gz";
				}else{
					temp_fname[i] = fname_base + "_stream"+fs_int_to_string(i)+ ".txt.gz.tmp";
					fname[i] = fname_base + "_stream"+fs_int_to_string(i) + ".txt.gz";
				}
//			else{
//				xform_command = "mv "+temp_fname+" "+fname;
//			}
				if ((output_file[i]=gzopen(temp_fname[i].c_str(),"wb"))==0) {
					gslog(LOG_EMERG,"Could not open file \"%s\".. EXITING\n", temp_fname[i].c_str());
					exit(1);
				}
				gzwrite(output_file[i],file_header.c_str(),strlen(file_header.c_str()));
				gzwrite(output_file[i],schema.c_str(),strlen(schema.c_str())+1);
			}
		}

		gs_uint32_t sz = tup.tuple_size;
		gs_uint32_t nsz = htonl(tup.tuple_size);
		gs_uint32_t file_hash = func.output_hash();
total_written += sizeof(gs_uint32_t) + sz;
		if (gzwrite(output_file[file_hash],&nsz,sizeof(gs_uint32_t))==0) {
			gslog(LOG_EMERG,"zfile_output_operator: Could not write %d bytes to output\"%s\".. EXITING\n", sizeof(gs_uint32_t), temp_fname[file_hash].c_str());
			exit(1);
		}

		if (gzwrite(output_file[file_hash],tup.data,sz)==0) {
			gslog(LOG_EMERG,"zfile_output_operator: Could not write %d bytes to output\"%s\".. EXITING\n", sz, temp_fname[file_hash].c_str());
			exit(1);
		}
		if(func.propagate_tuple()){
			tup.channel = output_channel;
			result.push_back(tup);
		}
		return 0;
	}else{
/*
		if (!func.create_temp_status_tuple(res)) {
			res.channel = output_channel;
			result.push_back(res);
		}
*/

//fprintf(stderr,"\tis temp tuple\n");
	}

	if(func.propagate_tuple()){
		tup.channel = output_channel;
		result.push_back(tup);
	}
	return 0;
}


	virtual int flush(list<host_tuple>& result) {
		return 0;
	}

	virtual int set_param_block(int sz, void * value) {
//		func.set_param_block(sz, value);
		return 0;
	}

	virtual int get_temp_status(host_tuple& result) {
		result.channel = output_channel;
		return func.create_temp_status_tuple(result);
	}

	virtual int get_blocked_status () {
		return -1;		// selection operators are not blocked on any input
	}
};


#endif	// ZFILE_OUTPUT_OPERATOR_H

