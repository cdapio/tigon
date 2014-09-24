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

#include"schemaparser_impl.h"
#include"schemaparser.h"
#include <string>
#include "parse_fta.h"
#include "parse_schema.h"
#include"generate_utils.h"

#include"host_tuple.h"
#include"lapp.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>


//		Interface to FTA definition lexer and parser ...

extern int FtaParserparse(void);
extern FILE *FtaParserin;
extern int FtaParserdebug;

//			This will need to be moved to a parse_fta.cc file.
fta_parse_t *fta_parse_result;
var_defs_t *fta_parse_defines;



using namespace std;
extern int errno;

static gs_int8_t tmpstr[20000];			// for returning const char* values.

vector<query_rep *> schema_list;	//	The schemas parsed thus far.

/////////////////////////////////////////////////////////////
///					Version functions

static gs_int32_t curr_version = 4;
static gs_int32_t accepted_versions[] = { 4, 3, 2, 1, 0 };		// must end with zero.

gs_int32_t get_schemaparser_version(){ return curr_version; }

gs_int32_t *get_schemaparser_accepted_versions(){return accepted_versions;}

gs_int32_t schemaparser_accepts_version(gs_int32_t v){
	int i;
	for(i=0;accepted_versions[i]>0;++i){
		if(accepted_versions[i] == v) return 1;
	}
	return 0;
}


/////////////////////////////////////////////////////////////////////
////////////		Utility Functions

/*
int fta_field_size(int dt, int *is_udef){
	switch(dt){
	case INT_TYPE:
		return(sizeof(int));
	case UINT_TYPE:
	case USHORT_TYPE:
	case BOOL_TYPE:
		return(sizeof(unsigned int));
	case ULLONG_TYPE:
		return(sizeof(unsigned long long int));
	case LLONG_TYPE:
		return(sizeof(long long int));
	case FLOAT_TYPE:
		return(sizeof(double));
	case TIMEVAL_TYPE:
		return(sizeof(timeval));
	case VSTR_TYPE:
		return(sizeof(vstring));
	default:
		*is_udef = 1;
		return(0);
	}
	return(0);
};
*/


/////////////////////////////////////////////////////////////
///			Interface fcns


////////////		Schema management

static gs_schemahandle_t ftaschema_parse();

gs_schemahandle_t ftaschema_parse_string(gs_csp_t f){

	  fta_parse_result = new fta_parse_t();
	  gs_sp_t schema = strdup(f);

//	  FtaParserin = f;
	  FtaParser_setstringinput(schema);

	  if(FtaParserparse()){
		fprintf(stderr,"FTA parse failed.\n");
		free (schema);
		return(-1);
	  }
	  free (schema);

	  return ftaschema_parse();
}

gs_schemahandle_t ftaschema_parse_file(FILE *f){

	  fta_parse_result = new fta_parse_t();

//	  FtaParserin = f;
	  FtaParser_setfileinput(f);

	  if(FtaParserparse()){
		fprintf(stderr,"FTA parse failed.\n");
		return(-1);
	  }

	  return ftaschema_parse();
}





static gs_schemahandle_t ftaschema_parse(){

		  if(fta_parse_result->parse_type != STREAM_PARSE){
			fprintf(stderr,"ERROR, input is not a stream file.\n");
			return(-1);
		  }


//			Get the tuple information.
	  if(fta_parse_result->tables->size() != 1){
	  	fprintf(stderr,"ERROR parsing schema file: %d tables, expecting 1.\n",
	  		fta_parse_result->tables->size() );
	  	return(-1);
	  }

	  string table_name = fta_parse_result->tables->get_table_name(0);
	  vector<field_entry *> tuple_flds = fta_parse_result->tables->get_fields(table_name);
	  int n_tuples = tuple_flds.size();

//			get the parameter info.
	  vector<var_pair_t *> plist = fta_parse_result->fta_parse_tree->plist;
	  int n_params = plist.size();

//			Get the query name
	  string query_name;
	  if(fta_parse_result->fta_parse_tree->nmap.count("query_name") == 0){
	  	fprintf(stderr,"WARNING: query name is empty. using default_query.\n");
	  	query_name = "default_query";
	  }else{
	  	query_name = fta_parse_result->fta_parse_tree->nmap["query_name"];
	  }

	  if(query_name != table_name){
	  	fprintf(stderr,"WARNING table name (%s) is different than query name (%s).\n",
	  		table_name.c_str(), query_name.c_str() );
	  }


//			Construct the query representation.

	  query_rep *qrep = new query_rep(query_name, n_tuples, n_params);

//			Pack the tuple information.
	  int fi;
	  for(fi=0;fi<n_tuples;++fi){
	  	if((qrep->set_field_info(fi,tuple_flds[fi])) == UNDEFINED_TYPE){
	  		fprintf(stderr,"ERROR tuple field %s (number %d) has undefined type %s.\n",
	  			tuple_flds[fi]->get_name().c_str(), fi, tuple_flds[fi]->get_type().c_str());
	  	}
	  }
	  if(qrep->finalize_field_info()){
	  	fprintf(stderr,"ERROR undefined type in tuple.\n");
	  	return(-1);
	  }

//			Pack the param info
	  int pi;
	  for(pi=0;pi<n_params;++pi){
	  	if((qrep->set_param_info(pi, plist[pi])) == UNDEFINED_TYPE){
	  		fprintf(stderr,"ERROR parameter %s (number %d) has undefined type %s.\n",
	  			plist[pi]->name.c_str(), pi, plist[pi]->val.c_str());
	  	}
	  }
	  if(qrep->finalize_param_info()){
	  	fprintf(stderr,"ERROR undefined type in parameter block.\n");
	  	return(-1);
	  }

//			finish up

	schema_list.push_back(qrep);

	return(schema_list.size()-1);
}


/*			Release memory used by the schema representation.
			return non-zero on error.
*/
//			Currently, do nothing.  I'll need to
//			systematically plug memory leaks and write destructors
//			to make implementing this function worth while.
gs_int32_t ftaschema_free(gs_schemahandle_t sh){
	return(0);
}


/* name of fta schema null terminated */
/* Returns NULL if sh is out of bounds.	*/
/* NO ALLOCATION IS PERFORMED!   Must treat result as const. */
gs_sp_t ftaschema_name(gs_schemahandle_t sh){
	if(sh < 0 || sh >= schema_list.size()){
		return(NULL);
	}
	strcpy(tmpstr,schema_list[sh]->name.c_str());
	return(tmpstr);
}


/////////////		Tuple management


/* number of entries in a tuple */
/* Return -1 if the schema handle is out of range.	*/
gs_int32_t ftaschema_tuple_len(gs_schemahandle_t sh){
	if(sh < 0 || sh >= schema_list.size()){
		return(-1);
	}
	return(schema_list[sh]->field_info.size());
}

/* tuple entry name */
/* Returns NULL if sh or index is out of bounds.	*/
/* NO ALLOCATION IS PERFORMED!   Must treat result as const. */
gs_sp_t ftaschema_tuple_name(gs_schemahandle_t sh, gs_uint32_t index){
	if(sh < 0 || sh >= schema_list.size()){
		return(NULL);
	}
	if(index >= schema_list[sh]->field_info.size()){
		return(NULL);
	}
	strcpy(tmpstr,schema_list[sh]->field_info[index].field_name.c_str());
	return( tmpstr);
}

/*			Direct tuple access functions.
*/
gs_uint32_t fta_unpack_uint(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	gs_uint32_t retval;
	if(offset+sizeof(gs_uint32_t) > len){
		*problem = 1;
		return(0);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint32_t));
//	return(htonl(retval));
	return(retval);
}
gs_uint32_t fta_unpack_uint_nocheck(void *data, gs_uint32_t offset){
	gs_uint32_t retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint32_t));
	return(retval);
}

gs_uint32_t fta_unpack_ushort(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	gs_uint32_t retval;
	if(offset+sizeof(gs_uint32_t) > len){
		*problem = 1;
		return(0);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint32_t));
// 	return(htonl(retval));
	return(retval);
}
gs_uint32_t fta_unpack_ushort_nocheck(void *data, gs_uint32_t offset){
	gs_uint32_t retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint32_t));
	return(retval);
}

gs_uint32_t fta_unpack_bool(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	gs_uint32_t retval;
	if(offset+sizeof(gs_uint32_t) > len){
		*problem = 1;
		return(0);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint32_t));
//	return(htonl(retval));
	return(retval);
}
gs_uint32_t fta_unpack_bool_nocheck(void *data, gs_uint32_t offset){
	gs_uint32_t retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint32_t));
//	return(htonl(retval));
	return(retval);
}

gs_int32_t fta_unpack_int(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	gs_int32_t retval;
	if(offset+sizeof(gs_int32_t) > len){
		*problem = 1;
		return(0);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_int32_t));
//	return(htonl(retval));
	return(retval);
}
gs_int32_t fta_unpack_int_nocheck(void *data, gs_uint32_t offset){
	gs_int32_t retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_int32_t));
	return(retval);
}

gs_uint64_t fta_unpack_ullong(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	gs_uint64_t retval;
	if(offset+sizeof(gs_uint64_t) > len){
		*problem = 1;
		return(0);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint64_t));
//	return(htonll(retval));
	return(retval);
}
gs_uint64_t fta_unpack_ullong_nocheck(void *data, gs_uint32_t offset){
	gs_uint64_t retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint64_t));
	return(retval);
}

gs_int64_t fta_unpack_llong(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	gs_int64_t retval;
	if(offset+sizeof( gs_int64_t) > len){
		*problem = 1;
		return(0);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_int64_t));
//	return(htonl(retval));
	return(retval);
}
gs_int64_t fta_unpack_llong_nocheck(void *data, gs_uint32_t offset){
	gs_int64_t retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_int64_t));
	return(retval);
}

gs_float_t fta_unpack_float(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	gs_float_t retval;
	if(offset+sizeof( gs_float_t) > len){
		*problem = 1;
		return(0.0);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_float_t));
//	return(ntohf(retval));
	return(retval);
}
gs_float_t fta_unpack_float_nocheck(void *data, gs_uint32_t offset){
	gs_float_t retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_float_t));
	return(retval);
}

timeval fta_unpack_timeval(void *data, gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	timeval retval;
	if(offset+sizeof( timeval) > len){
		*problem = 1;
		return(retval);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(timeval));
//	retval.tv_sec = htonl(retval.tv_sec);
//	retval.tv_usec = htonl(retval.tv_usec);
	retval.tv_sec = retval.tv_sec;
	retval.tv_usec = retval.tv_usec;
	return(retval);
}
timeval fta_unpack_timeval_nocheck(void *data, gs_uint32_t offset){
	timeval retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(timeval));
	retval.tv_sec = retval.tv_sec;
	retval.tv_usec = retval.tv_usec;
	return(retval);
}

vstring fta_unpack_vstr(void *data,  gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	vstring retval;
	vstring32 unpack_s;

	if(offset+sizeof( vstring32) > len){
		*problem = 1;
		return(retval);
	}

	memcpy(&unpack_s, ((gs_sp_t)data)+offset, sizeof(vstring32));

//	retval.length = htonl(unpack_s.length);
//	unpack_s.offset = htonl(unpack_s.offset);
	retval.length = unpack_s.length;
	unpack_s.offset = unpack_s.offset;
	retval.reserved = SHALLOW_COPY;

	if(unpack_s.offset + retval.length > len){
		*problem = 1;
		return(retval);
	}
	retval.offset = (gs_p_t)data + unpack_s.offset;
	return(retval);
}

gs_sp_t fta_unpack_fstring(void *data,  gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	return( (gs_sp_t)(data)+offset);
}

struct hfta_ipv6_str fta_unpack_ipv6(void *data,  gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
        struct hfta_ipv6_str retval;
        if(offset+sizeof(hfta_ipv6_str) > len){
                *problem = 1;
                return(retval);
        }
        memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(hfta_ipv6_str));
//		htonl(retval.v[0]);
//		htonl(retval.v[1]);
//		htonl(retval.v[2]);
//		htonl(retval.v[3]);
        return(retval);
}
struct hfta_ipv6_str fta_unpack_ipv6_nocheck(void *data, gs_uint32_t offset){
        struct hfta_ipv6_str retval;
        memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(hfta_ipv6_str));
        return(retval);
}


/*
		Direct tuple access functions, but no ntoh xform.
*/
gs_uint32_t fta_unpack_uint_noxf(void *data,  gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	gs_uint32_t retval;
	if(offset+sizeof(gs_uint32_t) > len){
		*problem = 1;
		return(0);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint32_t));
	return(retval);
}
gs_uint32_t fta_unpack_uint_noxf_nocheck(void *data, gs_uint32_t offset){
	gs_uint32_t retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint32_t));
	return(retval);
}

gs_uint32_t fta_unpack_ushort_noxf(void *data,  gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	gs_uint32_t retval;
	if(offset+sizeof(gs_uint32_t) > len){
		*problem = 1;
		return(0);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint32_t));
	return(retval);
}
gs_uint32_t fta_unpack_ushort_noxf_nocheck(void *data, gs_uint32_t offset){
	gs_uint32_t retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint32_t));
	return(retval);
}

gs_uint32_t fta_unpack_bool_noxf(void *data,  gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	gs_uint32_t retval;
	if(offset+sizeof(gs_uint32_t) > len){
		*problem = 1;
		return(0);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint32_t));
	return(retval);
}
gs_uint32_t fta_unpack_bool_noxf_nocheck(void *data, gs_uint32_t offset){
	gs_uint32_t retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint32_t));
	return(retval);
}

gs_int32_t fta_unpack_int_noxf(void *data,  gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	gs_int32_t retval;
	if(offset+sizeof(gs_int32_t) > len){
		*problem = 1;
		return(0);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_int32_t));
	return(retval);
}
gs_int32_t fta_unpack_int_noxf_nocheck(void *data, gs_uint32_t offset){
	gs_int32_t retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_int32_t));
	return(retval);
}

gs_uint64_t fta_unpack_ullong_noxf(void *data,  gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	gs_int64_t retval;
	if(offset+sizeof( gs_uint64_t) > len){
		*problem = 1;
		return(0);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint64_t));
	return(retval);
}
gs_uint64_t fta_unpack_ullong_noxf_nocheck(void *data, gs_uint32_t offset){
	gs_uint64_t retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_uint64_t));
	return(retval);
}

gs_int64_t fta_unpack_llong_noxf(void *data,  gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	gs_int64_t retval;
	if(offset+sizeof( gs_int64_t) > len){
		*problem = 1;
		return(0);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_int64_t));
	return(retval);
}
gs_int64_t fta_unpack_llong_noxf_nocheck(void *data, gs_uint32_t offset){
	gs_int64_t retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_int64_t));
	return(retval);
}

gs_float_t fta_unpack_float_noxf(void *data,  gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	gs_float_t retval;
	if(offset+sizeof( gs_float_t) > len){
		*problem = 1;
		return(0.0);
	}
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_float_t));
	return(retval);
}
gs_float_t fta_unpack_float_noxf_nocheck(void *data, gs_uint32_t offset){
	gs_float_t retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(gs_float_t));
	return(retval);
}

timeval fta_unpack_timeval_noxf_nocheck(void *data, gs_uint32_t offset){
	timeval retval;
	memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(timeval));
	return(retval);
}

vstring fta_unpack_vstr_noxf(void *data,  gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	vstring retval;
	vstring32 unpack_s;

	if(offset+sizeof( vstring32) > len){
		*problem = 1;
		return(retval);
	}

	memcpy(&unpack_s, ((gs_sp_t)data)+offset, sizeof(vstring32));

	retval.length = unpack_s.length;
	retval.reserved = SHALLOW_COPY;

	if(unpack_s.offset + unpack_s.length > len){
		*problem = 1;
		return(retval);
	}
	retval.offset = (gs_p_t)data + unpack_s.offset;
	return(retval);
}

gs_sp_t fta_unpack_fstring_noxf(void *data,  gs_int32_t len, gs_uint32_t offset, gs_int32_t *problem){
	return( (gs_sp_t)(data)+offset);
}

struct hfta_ipv6_str fta_unpack_ipv6_noxf_nocheck(void *data, gs_uint32_t offset){
        struct hfta_ipv6_str retval;
        memcpy(&retval, ((gs_sp_t)data)+offset, sizeof(hfta_ipv6_str));
        return(retval);
}


/* returns fields offset by name	*/
/* if sh is out of bounds, or if fieldname is not the name of a field,
   or len is too small,
   return value is -1
*/
gs_int32_t ftaschema_get_field_offset_by_name(gs_schemahandle_t sh, gs_csp_t fieldname){
	if(sh < 0 || sh >= schema_list.size()){
			return(-1);
	}

	int f;
	for(f=0;f<schema_list[sh]->field_info.size();++f){
		if(strcmp(fieldname, schema_list[sh]->field_info[f].field_name.c_str()) == 0)
			return(schema_list[sh]->field_info[f].offset);
	}

//		Nothing found
	return(-1);

}


/* returns fields type by name	*/
/* if sh is out of bounds, or if fieldname is not the name of a field,
   or len is too small,
   return value is -1
*/
gs_int32_t ftaschema_get_field_type_by_name(gs_schemahandle_t sh, gs_csp_t fieldname){
	if(sh < 0 || sh >= schema_list.size()){
			return(UNDEFINED_TYPE);
	}

	int f;
	for(f=0;f<schema_list[sh]->field_info.size();++f){
		if(strcmp(fieldname, schema_list[sh]->field_info[f].field_name.c_str()) == 0)
		return(schema_list[sh]->field_info[f].pdt->type_indicator());
	}

//		Nothing found
	return( UNDEFINED_TYPE);
}


/* returns tuple value based on name */
/* if sh is out of bounds, or if fieldname is not the name of a field,
   or len is too small,
   return value is of type UNDEFINED_TYPE
*/
access_result ftaschema_get_field_by_name(gs_schemahandle_t sh, gs_csp_t fieldname,
					  void * data, gs_uint32_t len){
	access_result retval;
	retval.field_data_type = UNDEFINED_TYPE;

	if(sh < 0 || sh >= schema_list.size()){
			return(retval);
	}

	int f;
	for(f=0;f<schema_list[sh]->field_info.size();++f){
		if(strcmp(fieldname, schema_list[sh]->field_info[f].field_name.c_str()) == 0)
			return(ftaschema_get_field_by_index(sh,f,data,len));
	}

//		Nothing found
	return(retval);

}

/* return tuple value by index */
/* if sh is out of bounds, or if fieldname is not the name of a field,
   or len is too small,
   return value is of type UNDEFINED_TYPE
*/
access_result ftaschema_get_field_by_index(gs_schemahandle_t sh, gs_uint32_t index,
					  void * data, gs_uint32_t len){
	access_result retval;
	retval.field_data_type = UNDEFINED_TYPE;
	gs_int32_t problem = 0;

	if(sh < 0 || sh >= schema_list.size()){
			return(retval);
	}
	if(index >= schema_list[sh]->field_info.size()){
		return(retval);
	}

	switch(schema_list[sh]->field_info[index].pdt->get_type()){
	case u_int_t:
		retval.r.ui = fta_unpack_uint(data,  len,
			schema_list[sh]->field_info[index].offset, &problem);
		if(!problem) retval.field_data_type = UINT_TYPE;
		break;
	case ip_t:
		retval.r.ui = fta_unpack_uint(data,  len,
			schema_list[sh]->field_info[index].offset, &problem);
		if(!problem) retval.field_data_type = IP_TYPE;
		break;
	case int_t:
		retval.r.i = fta_unpack_int(data,  len,
			schema_list[sh]->field_info[index].offset, &problem);
		if(!problem) retval.field_data_type = INT_TYPE;
		break;
	case u_llong_t:
		retval.r.ul = fta_unpack_ullong(data,  len,
			schema_list[sh]->field_info[index].offset, &problem);
		if(!problem) retval.field_data_type = ULLONG_TYPE;
		break;
	case llong_t:
		retval.r.l = fta_unpack_llong(data,  len,
			schema_list[sh]->field_info[index].offset, &problem);
		if(!problem) retval.field_data_type = LLONG_TYPE;
		break;
	case u_short_t:
		retval.r.ui = fta_unpack_ushort(data,  len,
			schema_list[sh]->field_info[index].offset, &problem);
		if(!problem) retval.field_data_type = USHORT_TYPE;
		break;
	case floating_t:
		retval.r.f = fta_unpack_float(data,  len,
			schema_list[sh]->field_info[index].offset, &problem);
		if(!problem) retval.field_data_type = FLOAT_TYPE;
		break;
	case bool_t:
		retval.r.ui = fta_unpack_bool(data,  len,
			schema_list[sh]->field_info[index].offset, &problem);
		if(!problem) retval.field_data_type = BOOL_TYPE;
		break;
	case v_str_t:
		retval.r.vs = fta_unpack_vstr(data,  len,
			schema_list[sh]->field_info[index].offset, &problem);
		if(!problem) retval.field_data_type = VSTR_TYPE;
		break;
	case fstring_t:
		retval.r.fs.data = fta_unpack_fstring(data,  len,
			schema_list[sh]->field_info[index].offset, &problem);
		retval.r.fs.size = schema_list[sh]->field_info[index].pdt->type_size();
		if(!problem) retval.field_data_type = FSTRING_TYPE;
	case timeval_t:
		retval.r.t = fta_unpack_timeval(data,  len,
			schema_list[sh]->field_info[index].offset, &problem);
		if(!problem) retval.field_data_type = TIMEVAL_TYPE;
		break;
	case ipv6_t:
		retval.r.ip6 = fta_unpack_ipv6(data,  len,
			schema_list[sh]->field_info[index].offset, &problem);
		if(!problem) retval.field_data_type = IPV6_TYPE;
		break;
	case undefined_t:
		break;
	}
	return(retval);
}


//		Get location of eof, temporal-tuple metadata.
gs_int32_t ftaschema_get_tuple_metadata_offset(gs_schemahandle_t sh){
        if(sh < 0 || sh >= schema_list.size())
                return 0;               // probably need to return a error instead of just telling its not eof tuple

        return ( schema_list[sh]->min_tuple_size);
}


/* checks whether tuple is temporal
  return value 1 indicates that tuple istemporal, 0 - not temporal
*/
gs_int32_t ftaschema_is_temporal_tuple(gs_schemahandle_t sh, void *data) {

	if(sh < 0 || sh >= schema_list.size())
		return 0;		// probably need to return a error instead of just telling its not temporal

	return (*((gs_sp_t)data + schema_list[sh]->min_tuple_size) == TEMPORAL_TUPLE);
}
//inline gs_int32_t ftaschema_is_temporal_tuple_offset(int metadata_offset, void *data) {
//	return (*((gs_sp_t)data + metadata_offset) == TEMPORAL_TUPLE);
//}



/* checks whether tuple is special end-of_file tuple
  return value 1 indicates that tuple is eof_tuple, 0 - otherwise
*/
gs_int32_t ftaschema_is_eof_tuple(gs_schemahandle_t sh, void *data) {
        if(sh < 0 || sh >= schema_list.size())
                return 0;               // probably need to return a error instead of just telling its not eof tuple

        return (*((gs_sp_t)data + schema_list[sh]->min_tuple_size) == EOF_TUPLE);

}
inline gs_int32_t ftaschema_is_eof_tuple_offset(int metadata_offset, void *data) {
        return (*((gs_sp_t)data + metadata_offset) == EOF_TUPLE);
}


/* extracts the trace from the temporal tuple
  return value 0 indicates success, non-zero - error
*/
gs_int32_t ftaschema_get_trace(gs_schemahandle_t sh, void* data, gs_int32_t len,
	gs_uint64_t* trace_id, gs_uint32_t* sz, fta_stat** trace ) {

	if(sh < 0 || sh >= schema_list.size() || *((gs_sp_t)data + schema_list[sh]->min_tuple_size) != TEMPORAL_TUPLE)
		return 1;

	memcpy(trace_id, (gs_sp_t)data + schema_list[sh]->min_tuple_size + sizeof(gs_int8_t), sizeof(gs_uint64_t));
	*trace = (fta_stat*)((gs_sp_t)data + schema_list[sh]->min_tuple_size + sizeof(gs_int8_t) + sizeof(gs_uint64_t));
	*sz = (len - schema_list[sh]->min_tuple_size - sizeof(gs_int8_t)- sizeof(gs_uint64_t)) / sizeof(fta_stat);

	return 0;
}


////////////	Param block management

/* number of parameters */
/*	Return -1 if sh is out of bounds	*/
gs_int32_t ftaschema_parameter_len(gs_schemahandle_t sh){
	if(sh < 0 || sh >= schema_list.size()){
		return(-1);
	}
	return(schema_list[sh]->param_info.size());
}

/* parameter entry name */
/*	Return NULL if sh or index is out of bounds.	*/
/*	NO COPYING IS PERFORMED */
gs_sp_t ftaschema_parameter_name(gs_schemahandle_t sh, gs_uint32_t index){
	if(sh < 0 || sh >= schema_list.size()){
		return(NULL);
	}
    if(index >= schema_list[sh]->param_info.size()){
    	return(NULL);
    }
	strcpy(tmpstr,schema_list[sh]->param_info[index].param_name.c_str());
    return(tmpstr);
}

/* set parameter value for parameter handle */
/*	Pass in the parameter in its char string representation. */
/*  Return value is -1 on error, else 0	*/
gs_int32_t ftaschema_setparam_by_name(gs_schemahandle_t sh, gs_sp_t param_name,
			       gs_sp_t param_val, gs_int32_t len){
	if(sh < 0 || sh >= schema_list.size()){
			return(-1);
	}

	int f;
	for(f=0;f<schema_list[sh]->param_info.size();++f){
		if(strcmp(param_name, schema_list[sh]->param_info[f].param_name.c_str()) == 0)
			return(ftaschema_setparam_by_index(sh,f,param_val,len));
	}

//		not found
	return(-1);
}

/* set parameter value for parameter handle */
/*	Pass in the parameter in its char string representation. */
/*  Return value is -1 on error, else 0	*/
gs_int32_t ftaschema_setparam_by_index(gs_schemahandle_t sh, gs_int32_t index,
				gs_sp_t param_val, gs_int32_t len){

    void *tmp_ptr;
	unsigned int ui,d1,d2,d3,d4;
	int i;
	unsigned long long int ulli;
	long long int lli;
	double f;


	if(sh < 0 || sh >= schema_list.size()){
		return(-1);
	}
    if(index >= schema_list[sh]->param_info.size()){
    	return(-1);
    }

    param_block_info *pb = &(schema_list[sh]->param_info[index]);
	switch(pb->pdt->get_type()){
		case int_t:
			if(sscanf(param_val,"%d",&i) == 1){
				pb->value.r.i=i;
				pb->value_set = true;
				return(0);
			}else
				return(-1);
			break;
		case u_int_t:
		case u_short_t:
		case bool_t:
			if(sscanf(param_val,"%u",&ui) == 1){
				pb->value.r.ui=ui;
				pb->value_set = true;
				return(0);
			}else
				return(-1);
			break;
		case ip_t:
			if(sscanf(param_val,"%d.%d.%d.%d",&d1,&d2,&d3,&d4) == 4){
				pb->value.r.ui = (d1 << 24)+(d2 << 16)+(d3 << 8)+d4;
				pb->value_set = true;
				return(0);
			}else
				return(-1);
		case u_llong_t:
			if(sscanf(param_val,"%llu",&ulli) == 1){
				pb->value.r.ul=ulli;
				pb->value_set = true;
				return(0);
			}else
				return(-1);
			break;
		case llong_t:
			if(sscanf(param_val,"%lld",&lli) == 1){
				pb->value.r.l=lli;
				pb->value_set = true;
				return(0);
			}else
				return(-1);
			break;
		case floating_t:
			if(sscanf(param_val,"%lf",&f) == 1){
				pb->value.r.f=f;
				pb->value_set = true;
				return(0);
			}else
				return(-1);
			break;
		case timeval_t:
			if(sscanf(param_val,"(%d,%d)",&d1, &d2) == 2){
				pb->value.r.t.tv_sec = d1;
				pb->value.r.t.tv_usec = d2;
				pb->value_set = true;
				return(0);
			}else
				return(-1);
			break;
		case v_str_t:
			if(pb->value.r.vs.offset != 0){
				tmp_ptr = (void *)(pb->value.r.vs.offset);
				free(tmp_ptr);
			}
			tmp_ptr = malloc(len);
			pb->value.r.vs.offset = (gs_p_t)tmp_ptr;
			memcpy(tmp_ptr,param_val, len);
			pb->value.r.vs.length = len;
//			pb->value.r.vs.reserved = 0;
			pb->value.r.vs.reserved = INTERNAL;
			pb->value_set = true;
			return(0);
			break;
		case fstring_t:
			fprintf(stderr,"ERROR, fstring parameters not supported, use vstring.\n");
			exit(1);
			break;
		case ipv6_t:
			fprintf(stderr,"ERROR, ipv6_t parameters not supported.\n");
			exit(1);
			break;
		case undefined_t:
			fprintf(stderr,"INTERNAL ERROR undefined_t type in ftaschema_setparam_by_index\n");
			exit(1);
		default:
			fprintf(stderr,"INTERNAL ERROR unknown type in ftaschema_setparam_by_index\n");
			exit(1);
	}

	return(-1);
}

gs_int32_t ftaschema_create_param_block(gs_schemahandle_t sh, void ** block, gs_int32_t * size){
	if(sh < 0 || sh >= schema_list.size()){
		return(-1);
	}

	*size = schema_list[sh]->min_param_size;
	int p;

	for(p=0;p<schema_list[sh]->param_info.size();++p){
		if(! schema_list[sh]->param_info[p].value_set) return 1;

		switch(schema_list[sh]->param_info[p].pdt->get_type()){
		case v_str_t:
			*size += schema_list[sh]->param_info[p].value.r.vs.length;
			break;
		case fstring_t:
			fprintf(stderr,"ERROR, fstring parameters not supported, use vstring.\n");
			exit(1);
			break;
		case undefined_t:
			return(-1);
			break;
		default:
			break;
		}
	}

	*block = malloc(*size);
	if(*block == NULL) return(-1);

	int data_pos = schema_list[sh]->min_param_size;

	gs_int32_t tmp_int;
	gs_uint32_t tmp_uint;
	gs_int64_t tmp_ll;
	gs_uint64_t tmp_ull;
	gs_float_t tmp_f;
	timeval tmp_tv;
	vstring tmp_vstr;
	void *tmp_ptr;

	for(p=0;p<schema_list[sh]->param_info.size();++p){
		param_block_info *pb = &(schema_list[sh]->param_info[p]);
		switch(pb->pdt->get_type()){
			case int_t:
//				tmp_int = htonl(pb->value.r.i);
				tmp_int = pb->value.r.i;
				memcpy(((gs_sp_t)(*block))+pb->offset,&tmp_int,sizeof(gs_int32_t));
				break;
			case u_int_t:
			case u_short_t:
			case bool_t:
			case ip_t:
//				tmp_uint = htonl(pb->value.r.ui);
				tmp_uint = pb->value.r.ui;
				memcpy(((gs_sp_t)(*block))+pb->offset,&tmp_uint,sizeof(gs_uint32_t));
				break;
			case u_llong_t:
//				tmp_ull = htonll(pb->value.r.ul);
				tmp_ull = pb->value.r.ul;
				memcpy(((gs_sp_t)(*block))+pb->offset,&tmp_ull,sizeof(gs_uint64_t));
				break;
			case llong_t:
//				tmp_ll = htonll(pb->value.r.l);
				tmp_ll = pb->value.r.l;
				memcpy(((gs_sp_t)(*block))+pb->offset,&tmp_ll,sizeof(gs_int64_t));
				break;
			case floating_t:
//				tmp_f = ntohf(pb->value.r.f);
				tmp_f = pb->value.r.f;
				memcpy(((gs_sp_t)(*block))+pb->offset,&tmp_f,sizeof(gs_float_t));
				break;
			case timeval_t:
//				tmp_tv.tv_sec = htonl(pb->value.r.t.tv_sec);
//				tmp_tv.tv_usec = htonl(pb->value.r.t.tv_usec);
				tmp_tv.tv_sec = pb->value.r.t.tv_sec;
				tmp_tv.tv_usec =pb->value.r.t.tv_usec;
				memcpy(((gs_sp_t)(*block))+pb->offset,&tmp_tv,sizeof(timeval));
				break;
			case v_str_t:
//				tmp_vstr.offset = htonl(data_pos);
//				tmp_vstr.length = htonl(pb->value.r.vs.length);
				tmp_vstr.offset = data_pos;
				tmp_vstr.length = pb->value.r.vs.length;
//				tmp_vstr.reserved = htonl(pb->value.r.vs.reserved);
//				tmp_vstr.reserved = htonl(PACKED);
				tmp_vstr.reserved = PACKED;
				memcpy(((gs_sp_t)(*block))+pb->offset,&tmp_vstr,sizeof(vstring));
				tmp_ptr = (void *)(pb->value.r.vs.offset);
				memcpy(((gs_sp_t)(*block))+data_pos, tmp_ptr, pb->value.r.vs.length);
				data_pos += pb->value.r.vs.length;
				break;
		case fstring_t:
			fprintf(stderr,"ERROR, fstring parameters not supported, use vstring.\n");
			exit(1);
			break;
		case ipv6_t:
			fprintf(stderr,"ERROR, ipv6_t parameters not supported.\n");
			exit(1);
			break;
		case undefined_t:
			fprintf(stderr,"INTERNAL ERROR undefined_t type in ftaschema_setparam_by_index\n");
			exit(1);
		default:
			fprintf(stderr,"INTERNAL ERROR unknown type in ftaschema_create_param_block\n");
			exit(1);
		}
	}
	return(0);

}


///////////////////////////////////////////////////////////////
//		Diagnostic functions

void ftaschema_debugdump(gs_int32_t handle){
	if(handle<0){
		printf("ftaschema_debugdump: handle is negative.\n");
		return;
	}
	if(handle >= schema_list.size()){
		printf("ftaschema_debugdump: handle out of bounds (%d, max is %lu)\n",
				handle, schema_list.size());
	}

	query_rep *q = schema_list[handle];

	printf("Handle is %d, query name is <%s>\n",handle,q->name.c_str());
	printf("Output tuple has %lu fields:\n",q->field_info.size());
	int f;
	for(f=0;f<q->field_info.size();++f){
		printf("\t%s (%s) : %d\n",q->field_info[f].field_name.c_str(),
			(q->field_info[f].pdt->get_type_str()).c_str(),
			q->field_info[f].offset
			);
	}
	printf("Min tuple size is %d\n\n",q->min_tuple_size);
	printf("Param block is:\n");
	int p;
	for(p=0;p<q->param_info.size();++p){
		printf("\t%s (%s) : %d\n",q->param_info[p].param_name.c_str(),
			(q->param_info[p].pdt->get_type_str()).c_str(),
			q->param_info[p].offset
			);
	}
	printf("Min param block size is %d\n\n",q->min_param_size);



}


