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
#ifndef __INTERFACE_IMPL_INCLUDED__
#define __INTERFACE_IMPL_INCLUDED__

#include"schemaparser.h"


 #include"type_indicators.h"
 #include"type_objects.h"
 #include"parse_fta.h"
 #include "gstypes.h"




//	access_result (access_fcn)(char *tuple, int offset);
class tuple_access_info{
public:
	std::string field_name;
	int offset;
	data_type *pdt;


	tuple_access_info(){
		pdt = NULL;
		offset = 0;
	};

	int init(field_entry *fe){
		field_name = fe->get_name();
		pdt = new data_type(fe->get_type());
		return(pdt->type_indicator());
	};
};

class param_block_info{
public:
	std::string param_name;
	access_result value;
	data_type *pdt;
	unsigned int offset;
	bool value_set;

	param_block_info(){
		pdt = NULL;
		offset = 0;
		value_set = false;
	};

	int init(var_pair_t *vp){
		value_set = false;
		param_name = vp->name;
		pdt = new data_type(vp->val);
		switch(pdt->get_type()){
			case int_t:
				value.r.i=0; break;
			case u_int_t:
			case u_short_t:
			case bool_t:
				value.r.ui=0; break;
			case floating_t:
				value.r.f = 0.0; break;
			case u_llong_t:
				value.r.ul = 0; break;
			case llong_t:
				value.r.l=0; break;
			case timeval_t:
				value.r.t.tv_sec=0; value.r.t.tv_usec=0; break;
			case v_str_t:
				value.r.vs.length = 0; value.r.vs.offset = 0; value.r.vs.reserved=0; break;
			case fstring_t:
				value.r.fs.data = NULL;
				value.r.fs.size = 0;
				break;
			default:
				break;
		}
		return(pdt->type_indicator());
	};
};



class query_rep{
public:
	std::string name;

	std::vector<tuple_access_info> field_info;
	int min_tuple_size;

	std::vector<param_block_info> param_info;
	int min_param_size;

	query_rep(std::string nm, int ntuples, int nparams) :
		name(nm), field_info(ntuples), param_info(nparams) {
			min_tuple_size = 0;
			min_param_size = 0;
	};

	int set_field_info(int f, field_entry *fe){
		if(f<0 || f>=field_info.size()) return(-1);
		return(field_info[f].init(fe));
	};

	int finalize_field_info(){
		int f;
		int curr_pos = 0;
		int fld_undefined = 0;
		for(f=0;f<field_info.size();++f){
			field_info[f].offset = curr_pos;
			int sz = field_info[f].pdt->type_size();
			if(sz==0) fld_undefined = 1;
			curr_pos += sz;
		}
		min_tuple_size = curr_pos;
		if(fld_undefined) return(-1);
		else return(0);
	};

	int set_param_info(int p, var_pair_t *vp){
		if(p<0 || p>=param_info.size()) return(-1);
		return(param_info[p].init(vp));
	};

	int finalize_param_info(){
		int p;
		int curr_pos = 0;
		int fld_undefined = 0;
		for(p=0;p<param_info.size();++p){
			param_info[p].offset = curr_pos;
			int sz = param_info[p].pdt->type_size();
			if(sz==0) fld_undefined = 1;
			curr_pos += sz;
		}
		min_param_size = curr_pos;
		if(fld_undefined) return(-1);
		else return(0);
	};


};

//			Diagnostic tool.
void ftaschema_debugdump(int sch_handle);

#endif

