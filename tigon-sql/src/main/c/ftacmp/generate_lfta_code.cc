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
#include <stdio.h>
#include <stdlib.h>
//#include <algo.h>
#include<algorithm>

#include "parse_fta.h"
#include "parse_schema.h"
#include "analyze_fta.h"
#include "generate_utils.h"
#include "query_plan.h"
#include "generate_lfta_code.h"
#include "generate_nic_code.h"

using namespace std;

extern int DEFAULT_LFTA_HASH_TABLE_SIZE;

// default value for correlation between the interface card and
// the system clock
#define DEFAULT_TIME_CORR 16


//	For fast hashing
//#define NRANDS 100
extern string hash_nums[NRANDS];
/*
= {
"12916008961267169387ull", "13447227858232756685ull",
"15651770379918602919ull", "1154671861688431608ull",
"6777078091984849858ull", "14217205709582564356ull",
"4955408621820609982ull", "15813680319165523695ull",
"9897969721407807129ull", "5799700135519793083ull",
"3446529189623437397ull", "2766403683465910630ull",
"3759321430908793328ull", "6569396511892890354ull",
"11124853911180290924ull", "17425412145238035549ull",
"6879931585355039943ull", "16598635011539670441ull",
"9615975578494811651ull", "4378135509538422740ull",
"741282195344332574ull", "17368612862906255584ull",
"17294299200556814618ull", "518343398779663051ull",
"3861893449302272757ull", "8951107288843549591ull",
"15785139392894559409ull", "5917810836789601602ull",
"16169988133001117004ull", "9792861259254509262ull",
"5089058010244872136ull", "2130075224835397689ull",
"844136788226150435ull", "1303298091153875333ull",
"3579898206894361183ull", "7529542662845336496ull",
"13151949992653382522ull", "2145333536541545660ull",
"11258221828939586934ull", "3741808146124570279ull",
"16272841626371307089ull", "12174572036188391283ull",
"9749343496254107661ull", "9141275584134508830ull",
"10134192232065698216ull", "12944268412561423018ull",
"17499725811865666340ull", "5281482378159088661ull",
"13254803486023572607ull", "4526762838498717025ull",
"15990846379668494011ull", "10680949816169027468ull",
"7116154096012931030ull", "5296740689865236632ull",
"5222427027515795922ull", "6893215299448261251ull",
"10164707755932877485ull", "15325979189512082255ull",
"3713267224148573289ull", "12292682741753167354ull",
"4098115959960163588ull", "16095675565885113990ull",
"11391590846210510720ull", "8432889531466002673ull",
"7146668520368482523ull", "7678169991822407997ull",
"9882712513525031447ull", "13904414563513869160ull",
"1080076724395768626ull", "8448147843172150388ull",
"17633093729608185134ull", "10044622457050142303ull",
"4128911859292425737ull", "30642269109444395ull",
"16124215396922640581ull", "15444089895060081110ull",
"16437006538696302944ull", "800338649777443426ull",
"5355794945275091932ull", "11656354278827687117ull",
"1110873718944691255ull", "10829576045617693977ull",
"3846916616884579955ull", "17055821716837625668ull",
"13418968402643535758ull", "11671612594828802128ull",
"11597298928184328586ull", "13196028510862205499ull",
"16539578557089782373ull", "3182048322921507591ull",
"10016080431267550241ull", "148751875162592690ull",
"10400930266590768572ull", "4023803397139127870ull",
"17766462746879108920ull", "14807761432134600873ull",
"13521540421053792403ull", "13980983198941385205ull",
"16257584414193564367ull", "1760484796451765024ull"
};
*/


// ----------------------------------------------
//		Data extracted from the query plan node
//		for use by code generation.

static cplx_lit_table *complex_literals;  //Table of literals with constructors.
static vector<handle_param_tbl_entry *> param_handle_table;
static param_table *param_tbl;		// Table of all referenced parameters.

static vector<scalarexp_t *> sl_list;
static vector<cnf_elem *> where;

static gb_table *gb_tbl;			// Table of all group-by attributes.
static aggregate_table *aggr_tbl;	// Table of all referenced aggregates.

static bool packed_return;		// unpack using structyure, not fcns
static nic_property *nicprop;	// nic properties for this interface.
static int global_id;


//		The partial_fcns vector can now refer to
//		partial functions, or expensive functions
//		which can be cached (if there are multiple refs).  A couple
//		of int vectors distinguish the cases.
static vector<scalarexp_t *> partial_fcns;
static vector<int> fcn_ref_cnt;
static vector<bool> is_partial_fcn;
int sl_fcns_start = 0, sl_fcns_end = 0;
int wh_fcns_start = 0, wh_fcns_end = 0;
int gb_fcns_start = 0, gb_fcns_end = 0;
int ag_fcns_start = 0, ag_fcns_end = 0;


//		These vectors are for combinable predicates.
static vector<int> pred_class;	//	identifies the group
static vector<int> pred_pos;	//  position in the group.



static char tmpstr[1000];

//////////////////////////////////////////////////////////////////////
///			Various utilities

string generate_fta_name(string node_name){
	string ret = normalize_name(node_name);
	if(ret == ""){
		ret = "default";
	}
	ret += "_fta";

	return(ret);
}


string generate_aggr_struct_name(string node_name){
	string ret = normalize_name(node_name);
	if(ret == ""){
		ret = "default";
	}
	ret += "_aggr_struct";

	return(ret);
}

string generate_fj_struct_name(string node_name){
	string ret = normalize_name(node_name);
	if(ret == ""){
		ret = "default";
	}
	ret += "_fj_struct";

	return(ret);
}

string generate_unpack_code(int tblref, int schref, string field, table_list *schema, string node_name, string end_goto = string("end")){
	string ret;
	if(! packed_return){
		sprintf(tmpstr,"\tretval =  %s(p, &unpack_var_%s_%d);\n",
    			schema->get_fcn(schref,field).c_str(), field.c_str(), tblref);
    	ret += tmpstr;
    	if(!schema->get_modifier_list(schref,field)->contains_key("required"))
    		ret += "\tif(retval) goto "+end_goto+";\n";

	}else{
//			TODO: ntoh xforms (aug 2010 : removing ntoh, hton)
		data_type dt(schema->get_type_name(schref,field), schema->get_modifier_list(schref,field));
		if(dt.is_buffer_type()){
			if(dt.get_type() != v_str_t){
			  ret += "\tif(sizeof(struct "+node_name+"_input_struct)+"+node_name+"_input_struct_var->unpack_var_"+field+".length+int("+node_name+"_input_struct_var->unpack_var_"+field+".data) > sz)\n";
			  ret += "\t\tgoto "+end_goto+";\n";
			  ret+= "\t\t"+node_name+"_input_struct_var->unpack_var_"+field+".data += "+node_name+"_input_struct_var->unpack_var_"+field+".length;\n";
			  ret += "\tunpack_var_"+field+"_"+int_to_string(tblref)+
				" = "+node_name+"_input_struct_var->unpack_var_"+field+";+\n";
			}else{
				fprintf(stderr,"INTERNAL ERROR buffer type not string type in generate_lfta_code.cc:generate_unpack_code\n");
				exit(1);
			}
		}else{
			ret += "\tunpack_var_"+field+"_"+int_to_string(tblref)+
				" = "+node_name+"_input_struct_var->unpack_var_"+field+";\n";
		}
	}
	return ret;
}

string generate_aggr_struct(string node_name, gb_table *gb_tbl, aggregate_table *aggr_tbl){
  string ret = "struct " + generate_aggr_struct_name(node_name) + "{\n";

  int g;
  for(g=0;g<gb_tbl->size();g++){
	  sprintf(tmpstr,"gb_var%d",g);
	  ret += "\t"+gb_tbl->get_data_type(g)->make_cvar(tmpstr)+";\n";
  }

  int a;
  for(a=0;a<aggr_tbl->size();a++){
	  ret += "\t";
	  sprintf(tmpstr,"aggr_var%d",a);
	  if(aggr_tbl->is_builtin(a))
	  	ret+="\t"+aggr_tbl->get_data_type(a)->make_cvar(tmpstr)+";\n";
	  else
	  	ret+="\t"+aggr_tbl->get_storage_type(a)->make_cvar(tmpstr)+";\n";
  }

/*
  ret += "\tstruct "+generate_aggr_struct_name(node_name)+" *next;\n";
*/

  ret += "};\n\n";

  return(ret);
}


string generate_fj_struct(filter_join_qpn *fs, string node_name ){
  string ret;

  if(fs->use_bloom == false){	// uses hash table instead
  	ret = "struct " + generate_fj_struct_name(node_name) + "{\n";
	int k;
	for(k=0;k<fs->hash_eq.size();++k){
		sprintf(tmpstr,"key_var%d",k);
		ret += "\t"+fs->hash_eq[k]->pr->get_left_se()->get_data_type()->make_cvar(tmpstr)+";\n";
	}
	ret += "\tlong long int ts;\n";
    ret += "};\n\n";
  }

  return(ret);
}




string generate_fta_struct(string node_name, gb_table *gb_tbl,
		aggregate_table *aggr_tbl, param_table *param_tbl,
		cplx_lit_table *complex_literals,
		vector<handle_param_tbl_entry *> &param_handle_table,
		bool is_aggr_query, bool is_fj, bool uses_bloom,
		table_list *schema){

	string ret = "struct " + generate_fta_name(node_name) + "{\n";
	ret += "\tstruct FTA f;\n";

//-------------------------------------------------------------
//		Aggregate-specific fields

	if(is_aggr_query){
/*
		ret += "\tstruct "+generate_aggr_struct_name(node_name)+" *aggr_head, *flush_head;\n";
*/
		ret+="\tstruct "+generate_aggr_struct_name(node_name)+" *aggr_table; // the groups\n";
		ret+="\tgs_uint32_t *aggr_table_hashmap; // hash val, plus control info.\n";
//		ret+="\tint bitmap_size;\n";
		ret += "\tint n_aggrs; // # of non-empty slots in aggr_table\n";
		ret += "\tint max_aggrs; // size of aggr_table and its hashmap.\n";
		ret += "\tint max_windows; // max number of open windows.\n";
		ret += "\tunsigned int generation; // initially zero, increment on\n";
		ret += "\t     // every hash table flush - whether regular or induced.\n";
		ret += "\t     // Old groups are identified by a generation mismatch.\n";
		ret += "\tunsigned int flush_pos; // next aggr_table entry to examine\n";
		ret += "\tunsigned int flush_ctr; // control slow flushing\n";



		int g;
		bool uses_temporal_flush = false;
		for(g=0;g<gb_tbl->size();g++){
			data_type *dt = gb_tbl->get_data_type(g);
			if(dt->is_temporal()){
/*
				fprintf(stderr,"group by attribute %s is temporal, ",
						gb_tbl->get_name(g).c_str());
				if(dt->is_increasing()){
					fprintf(stderr,"increasing.\n");
				}else{
					fprintf(stderr,"decreasing.\n");
				}
*/
				data_type *gdt = gb_tbl->get_data_type(g);
				if(gdt->is_buffer_type()){
					fprintf(stderr, "\t but temporal BUFFER types are not supported, skipping.\n");
				}else{
					sprintf(tmpstr,"\t%s last_gb_%d;\n",gdt->get_cvar_type().c_str(),g);
					ret += tmpstr;
					sprintf(tmpstr,"\t%s flush_start_gb_%d;\n",gdt->get_cvar_type().c_str(),g);
					ret += tmpstr;
					sprintf(tmpstr,"\t%s last_flushed_gb_%d;\n",gdt->get_cvar_type().c_str(),g);
					ret += tmpstr;
					uses_temporal_flush = true;
				}

			}

		}
		if(! uses_temporal_flush){
			fprintf(stderr,"Warning: no temporal flush.\n");
		}
	}

// ---------------------------------------------------------
//			Filter-join specific fields

	if(is_fj){
		if(uses_bloom){
			ret +=
"\tunsigned char * bf_table; //array of bloom filters with layout \n"
"\t\t// bit 0 bf 0| bit 0 bf 1| bit 0 bf 2| bit 1 bf 0| bit 1 bf 1|.....\n"
"\tint first_exec;\n"
"\tlong long int last_bin;\n"
"\tint last_bloom_pos;\n"
"\n"
;
		}else{		// limited hash table
			ret +=
"  struct "+generate_fj_struct_name(node_name)+" *join_table;\n"
"\n"
;
		}

	}

//--------------------------------------------------------
//			Common fields

//			Create places to hold the parameters.
	int p;
	vector<string> param_vec = param_tbl->get_param_names();
	for(p=0;p<param_vec.size();p++){
		data_type *dt = param_tbl->get_data_type(param_vec[p]);
		sprintf(tmpstr,"\t%s param_%s;\n",dt->get_cvar_type().c_str(),
				param_vec[p].c_str());
		ret += tmpstr;
		if(param_tbl->handle_access(param_vec[p])){
			ret += "\tstruct search_handle *param_handle_"+param_vec[p]+";\n";
		}
	}

//			Create places to hold complex literals.
	int cl;
	for(cl=0;cl<complex_literals->size();cl++){
		literal_t *l = complex_literals->get_literal(cl);
		data_type *dtl = new data_type( l->get_type() );
		sprintf(tmpstr,"\t%s complex_literal_%d;\n",dtl->get_cvar_type().c_str(),cl);
		ret += tmpstr;
	}

//			Create places to hold the pass-by-handle parameters.
	for(p=0;p<param_handle_table.size();++p){
		sprintf(tmpstr,"\tgs_param_handle_t handle_param_%d;\n",p);
		ret += tmpstr;
	}

//			Create places to hold the last values of temporal
//			attributes referenced in select clause
//			we also need to store values of the temoral attributed
//			of last flushed tuple in aggr queries
//			to make sure we generate the cirrect temporal tuple
//			in the presense of slow flushes


	col_id_set temp_cids;		//	col ids of temp attributes in select clause

	int s;
	col_id_set::iterator csi;

	for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if (sdt->is_temporal()) {
			gather_se_col_ids(sl_list[s],temp_cids, gb_tbl);
		}
	}

	for(csi=temp_cids.begin(); csi != temp_cids.end();++csi){
		int tblref = (*csi).tblvar_ref;
		int schref = (*csi).schema_ref;
		string field = (*csi).field;
		data_type dt(schema->get_type_name(schref,field), schema->get_modifier_list(schref,field));
		sprintf(tmpstr,"\t%s last_%s_%d;\n", dt.get_cvar_type().c_str(), field.c_str(), tblref);
		ret += tmpstr;
	}

	ret += "\tgs_uint64_t trace_id;\n\n";

//	Fields to store the runtime stats

	ret += "\tgs_uint32_t in_tuple_cnt;\n";
	ret += "\tgs_uint32_t out_tuple_cnt;\n";
	ret += "\tgs_uint32_t out_tuple_sz;\n";
	ret += "\tgs_uint32_t accepted_tuple_cnt;\n";
	ret += "\tgs_uint64_t cycle_cnt;\n";
	ret += "\tgs_uint32_t collision_cnt;\n";
	ret += "\tgs_uint32_t eviction_cnt;\n";
	ret += "\tgs_float_t sampling_rate;\n";



	ret += "};\n\n";

	return(ret);
}

//------------------------------------------------------------
//		Set colref tblvars to 0..
//		(special processing for join-like operators in an lfta).

void reset_se_col_ids_tblvars(scalarexp_t *se,  gb_table *gtbl){
	vector<scalarexp_t *> operands;
	int o;

	if(! se)
		return;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return;
	case SE_UNARY_OP:
		reset_se_col_ids_tblvars(se->get_left_se(),gtbl);
		return;
	case SE_BINARY_OP:
		reset_se_col_ids_tblvars(se->get_left_se(),gtbl);
		reset_se_col_ids_tblvars(se->get_right_se(),gtbl);
		return;
	case SE_COLREF:
		if(! se->is_gb() ){
			se->get_colref()->set_tablevar_ref(0);
		}else{
			if(gtbl==NULL){
				fprintf(stderr,"INTERNAL ERROR: gbvar ref in gather_se_col_ids, but gtbl is NULL.\n");
				exit(1);
			}
			reset_se_col_ids_tblvars(gtbl->get_def(se->get_gb_ref()),gtbl);
		}
		return;
	case SE_AGGR_STAR:
		return;
	case SE_AGGR_SE:
		reset_se_col_ids_tblvars(se->get_left_se(),gtbl);
		return;
	case SE_FUNC:
		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			reset_se_col_ids_tblvars(operands[o], gtbl);
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in reset_se_col_ids_tblvars, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
}


//		reset  column tblvars accessed in this pr.

void reset_pr_col_ids_tblvars(predicate_t *pr,  gb_table *gtbl){
	vector<scalarexp_t *> op_list;
	int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		reset_se_col_ids_tblvars(pr->get_left_se(), gtbl);
		return;
	case PRED_COMPARE:
		reset_se_col_ids_tblvars(pr->get_left_se(),gtbl) ;
		reset_se_col_ids_tblvars(pr->get_right_se(),gtbl) ;
		return;
	case PRED_UNARY_OP:
		reset_pr_col_ids_tblvars(pr->get_left_pr(),gtbl) ;
		return;
	case PRED_BINARY_OP:
		reset_pr_col_ids_tblvars(pr->get_left_pr(),gtbl) ;
		reset_pr_col_ids_tblvars(pr->get_right_pr(),gtbl) ;
		return;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			reset_se_col_ids_tblvars(op_list[o],gtbl) ;
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in reset_pr_col_ids_tblvars, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
	}
}




//			Generate code that makes reference
//			to the tuple, and not to any aggregates.
static string generate_se_code(scalarexp_t *se,table_list *schema){
	string ret;
    data_type *ldt, *rdt;
	int o;
	vector<scalarexp_t *> operands;


	switch(se->get_operator_type()){
	case SE_LITERAL:
		if(se->is_handle_ref()){
			sprintf(tmpstr,"t->handle_param_%d",se->get_handle_ref() );
			ret = tmpstr;
			return(ret);
		}
		if(se->get_literal()->is_cpx_lit()){
			sprintf(tmpstr,"t->complex_literal_%d",se->get_literal()->get_cpx_lit_ref() );
			ret = tmpstr;
			return(ret);
		}
		return(se->get_literal()->to_C_code("")); // not complex, no constructor
	case SE_PARAM:
		if(se->is_handle_ref()){
			sprintf(tmpstr,"t->handle_param_%d",se->get_handle_ref() );
			ret = tmpstr;
			return(ret);
		}
		ret += "t->param_";
		ret += se->get_param_name();
		return(ret);
	case SE_UNARY_OP:
        ldt = se->get_left_se()->get_data_type();
        if(ldt->complex_operator(se->get_op()) ){
			ret +=  ldt->get_complex_operator(se->get_op());
			ret += "(";
			ret += generate_se_code(se->get_left_se(),schema);
            ret += ")";
		}else{
			ret += "(";
			ret += se->get_op();
			ret += generate_se_code(se->get_left_se(),schema);
			ret += ")";
		}
		return(ret);
	case SE_BINARY_OP:
        ldt = se->get_left_se()->get_data_type();
        rdt = se->get_right_se()->get_data_type();

        if(ldt->complex_operator(rdt, se->get_op()) ){
			ret +=  ldt->get_complex_operator(rdt, se->get_op());
			ret += "(";
			ret += generate_se_code(se->get_left_se(),schema);
			ret += ", ";
			ret += generate_se_code(se->get_right_se(),schema);
			ret += ")";
		}else{
			ret += "(";
			ret += generate_se_code(se->get_left_se(),schema);
			ret += se->get_op();
			ret += generate_se_code(se->get_right_se(),schema);
			ret += ")";
		}
		return(ret);
	case SE_COLREF:
		if(se->is_gb()){		// OK to ref gb attrs, but they're not yet unpacked ...
							// so return the defining code.
			ret = generate_se_code(gb_tbl->get_def(se->get_gb_ref()), schema );

		}else{
    		sprintf(tmpstr,"unpack_var_%s_%d",
    		  se->get_colref()->get_field().c_str(), se->get_colref()->get_tablevar_ref() );
    		ret = tmpstr;
		}
		return(ret);
	case SE_FUNC:
//				Should not be ref'ing any aggr here.
		if(se->get_aggr_ref() >= 0){
			fprintf(stderr,"INTERNAL ERROR, UDAF reference in generate_se_code.\n");
			return("ERROR in generate_se_code");
		}

		if(se->is_partial()){
			sprintf(tmpstr,"partial_fcn_result_%d",se->get_partial_ref());
			ret = tmpstr;
		}else{
			ret += se->op + "(";
			operands = se->get_operands();
			for(o=0;o<operands.size();o++){
				if(o>0) ret += ", ";
				if(operands[o]->get_data_type()->is_buffer_type() && (! (operands[o]->is_handle_ref()) ) )
					ret += "&";
				ret += generate_se_code(operands[o], schema);
			}
			ret += ")";
		}
		return(ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in generate_se_code (lfta), line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		return("ERROR in generate_se_code");
	}
}

//		generate code that refers only to aggregate data and constants.
static string generate_se_code_fm_aggr(scalarexp_t *se, string var, table_list *schema){

	string ret;
    data_type *ldt, *rdt;
	int o;
	vector<scalarexp_t *> operands;


	switch(se->get_operator_type()){
	case SE_LITERAL:
		if(se->is_handle_ref()){
			sprintf(tmpstr,"t->handle_param_%d",se->get_handle_ref() );
			ret = tmpstr;
			return(ret);
		}
		if(se->get_literal()->is_cpx_lit()){
			sprintf(tmpstr,"t->complex_literal_%d",se->get_literal()->get_cpx_lit_ref() );
			ret = tmpstr;
			return(ret);
		}
		return(se->get_literal()->to_C_code("")); // not complex no constructor
	case SE_PARAM:
		if(se->is_handle_ref()){
			sprintf(tmpstr,"t->handle_param_%d",se->get_handle_ref() );
			ret = tmpstr;
			return(ret);
		}
		ret += "t->param_";
		ret += se->get_param_name();
		return(ret);
	case SE_UNARY_OP:
        ldt = se->get_left_se()->get_data_type();
        if(ldt->complex_operator(se->get_op()) ){
			ret +=  ldt->get_complex_operator(se->get_op());
			ret += "(";
			ret += generate_se_code_fm_aggr(se->get_left_se(),var,schema);
            ret += ")";
		}else{
			ret += "(";
			ret += se->get_op();
			ret += generate_se_code_fm_aggr(se->get_left_se(),var,schema);
			ret += ")";
		}
		return(ret);
	case SE_BINARY_OP:
        ldt = se->get_left_se()->get_data_type();
        rdt = se->get_right_se()->get_data_type();

        if(ldt->complex_operator(rdt, se->get_op()) ){
			ret +=  ldt->get_complex_operator(rdt, se->get_op());
			ret += "(";
			ret += generate_se_code_fm_aggr(se->get_left_se(),var,schema);
			ret += ", ";
			ret += generate_se_code_fm_aggr(se->get_right_se(),var,schema);
			ret += ")";
		}else{
			ret += "(";
			ret += generate_se_code_fm_aggr(se->get_left_se(),var,schema);
			ret += se->get_op();
			ret += generate_se_code_fm_aggr(se->get_right_se(),var,schema);
			ret += ")";
		}
		return(ret);
	case SE_COLREF:
		if(se->is_gb()){		// OK to ref gb attrs, but they're not yet
							// unpacked ... so return the defining code.
			sprintf(tmpstr,"%sgb_var%d",var.c_str(),se->get_gb_ref());
			ret = tmpstr;

		}else{
    		fprintf(stderr,"ERROR reference to non-GB column ref not permitted here,"
				"error in generate_se_code_fm_aggr, line %d, character %d.\n",
				se->get_lineno(), se->get_charno());
    		ret = tmpstr;
		}
		return(ret);
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
		sprintf(tmpstr,"%saggr_var%d",var.c_str(),se->get_aggr_ref());
		ret = tmpstr;
		return(ret);
	case SE_FUNC:
//				Is it a UDAF?
		if(se->get_aggr_ref() >= 0){
			sprintf(tmpstr,"udaf_ret%d",se->get_aggr_ref());
			ret = tmpstr;
			return(ret);
		}

		if(se->is_partial()){
			sprintf(tmpstr,"partial_fcn_result_%d",se->get_partial_ref());
			ret = tmpstr;
		}else{
			ret += se->op + "(";
			operands = se->get_operands();
			for(o=0;o<operands.size();o++){
				if(o>0) ret += ", ";
				if(operands[o]->get_data_type()->is_buffer_type() && (! (operands[o]->is_handle_ref()) ) )
					ret += "&";
				ret += generate_se_code_fm_aggr(operands[o], var, schema);
			}
			ret += ")";
		}
		return(ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in generate_lfta_code.cc::generate_se_code_fm_aggr, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		return("ERROR in generate_se_code");
	}

}


static string unpack_partial_fcn_fm_aggr(scalarexp_t *se, int pfn_id, string var, table_list *schema){
	string ret;
	int o;
	vector<scalarexp_t *> operands;


	if(se->get_operator_type() != SE_FUNC || se->get_aggr_ref() >= 0){
		fprintf(stderr,"INTERNAL ERROR, non-function SE passed to unpack_partial_fcn_fm_aggr. line %d, character %d\n",
				se->get_lineno(), se->get_charno());
		return("ERROR in generate_se_code");
	}

	ret = "\tretval = " + se->get_op() + "( ";
	sprintf(tmpstr, "&partial_fcn_result_%d",pfn_id);
	ret += tmpstr;

	operands = se->get_operands();
	for(o=0;o<operands.size();o++){
		ret += ", ";
		if(operands[o]->get_data_type()->is_buffer_type() && (! (operands[o]->is_handle_ref()) ) )
			ret += "&";
		ret += generate_se_code_fm_aggr(operands[o], var, schema);
	}
	ret += ");\n";

	return(ret);
}

static string generate_cached_fcn(scalarexp_t *se, table_list *schema){
	string ret;
	int o;
	vector<scalarexp_t *> operands;

	if(se->get_operator_type() != SE_FUNC || se->get_aggr_ref() >= 0){
		fprintf(stderr,"INTERNAL ERROR, non-function SE passed to generate_cached_fcn. line %d, character %d\n",
				se->get_lineno(), se->get_charno());
		return("ERROR in generate_se_code");
	}

	ret = se->get_op() + "( ";

	operands = se->get_operands();
	for(o=0;o<operands.size();o++){
		if(o) ret += ", ";
		if(operands[o]->get_data_type()->is_buffer_type() && (! (operands[o]->is_handle_ref()) ) )
			ret += "&";
		ret += generate_se_code(operands[o], schema);
	}
	ret += ");\n";

	return(ret);
}



static string unpack_partial_fcn(scalarexp_t *se, int pfn_id, table_list *schema){
	string ret;
	int o;
	vector<scalarexp_t *> operands;


	if(se->get_operator_type() != SE_FUNC || se->get_aggr_ref() >= 0){
		fprintf(stderr,"INTERNAL ERROR, non-function SE passed to unpack_partial_fcn. line %d, character %d\n",
				se->get_lineno(), se->get_charno());
		return("ERROR in generate_se_code");
	}

	ret = "\tretval = " + se->get_op() + "( ",
	sprintf(tmpstr, "&partial_fcn_result_%d",pfn_id);
	ret += tmpstr;

	operands = se->get_operands();
	for(o=0;o<operands.size();o++){
		ret += ", ";
		if(operands[o]->get_data_type()->is_buffer_type() && (! (operands[o]->is_handle_ref()) ) )
			ret += "&";
		ret += generate_se_code(operands[o], schema);
	}
	ret += ");\n";

	return(ret);
}





static string generate_C_comparison_op(string op){
  if(op == "=") return("==");
  if(op == "<>") return("!=");
  return(op);
}

static string generate_C_boolean_op(string op){
	if( (op == "AND") || (op == "And") || (op == "and") ){
		return("&&");
	}
	if( (op == "OR") || (op == "Or") || (op == "or") ){
		return("||");
	}
	if( (op == "NOT") || (op == "Not") || (op == "not") ){
		return("!");
	}

	fprintf(stderr,"INTERNAL ERROR: unknown boolean operator %s\n",op.c_str());
	return("ERROR UNKNOWN BOOLEAN OPERATOR :"+op);
}


static string generate_predicate_code(predicate_t *pr,table_list *schema){
	string ret;
	vector<literal_t *>  litv;
	int i;
    data_type *ldt, *rdt;
	vector<scalarexp_t *> op_list;
	int o,cref,ppos;
	unsigned int bitmask;

	switch(pr->get_operator_type()){
	case PRED_IN:
        ldt = pr->get_left_se()->get_data_type();

		ret += "( ";
		litv = pr->get_lit_vec();
		for(i=0;i<litv.size();i++){
			if(i>0) ret += " || ";
			ret += "( ";

	        if(ldt->complex_comparison(ldt) ){
				ret +=  ldt->get_comparison_fcn(ldt) ;
				ret += "( ";
				if(ldt->is_buffer_type() ) ret += "&";
				ret += generate_se_code(pr->get_left_se(), schema);
				ret += ", ";
				if(ldt->is_buffer_type() ) ret += "&";
				if(litv[i]->is_cpx_lit()){
					sprintf(tmpstr,"t->complex_literal_%d",litv[i]->get_cpx_lit_ref() );
					ret += tmpstr;
				}else{
					ret += litv[i]->to_C_code("");
				}
				ret += ") == 0";
			}else{
				ret += generate_se_code(pr->get_left_se(), schema);
				ret += " == ";
				ret += litv[i]->to_C_code("");
			}

			ret += " )";
		}
		ret += " )";
		return(ret);

	case PRED_COMPARE:
        ldt = pr->get_left_se()->get_data_type();
        rdt = pr->get_right_se()->get_data_type();

		ret += "( ";
        if(ldt->complex_comparison(rdt) ){
			ret += ldt->get_comparison_fcn(rdt);
			ret += "(";
			if(ldt->is_buffer_type() ) ret += "&";
			ret += generate_se_code(pr->get_left_se(),schema);
			ret += ", ";
			if(rdt->is_buffer_type() ) ret += "&";
			ret += generate_se_code(pr->get_right_se(),schema);
			ret += ") ";
			ret +=  generate_C_comparison_op(pr->get_op());
			ret += "0";
		}else{
			ret += generate_se_code(pr->get_left_se(),schema);
			ret +=  generate_C_comparison_op(pr->get_op());
			ret += generate_se_code(pr->get_right_se(),schema);
		}
		ret += " )";
		return(ret);
	case PRED_UNARY_OP:
		ret += "( ";
		ret +=  generate_C_boolean_op(pr->get_op());
		ret += generate_predicate_code(pr->get_left_pr(),schema);
		ret += " )";
		return(ret);
	case PRED_BINARY_OP:
		ret += "( ";
		ret += generate_predicate_code(pr->get_left_pr(),schema);
		ret +=  generate_C_boolean_op(pr->get_op());
		ret += generate_predicate_code(pr->get_right_pr(),schema);
		ret += " )";
		return(ret);
	case PRED_FUNC:
		op_list = pr->get_op_list();
		cref = pr->get_combinable_ref();
		if(cref >= 0){	// predicate is a combinable pred reference
			//		Trust, but verify
			if(pred_class.size() >= cref && pred_class[cref] >= 0){
				ppos = pred_pos[cref];
				bitmask = 1 << ppos % 32;
				sprintf(tmpstr,"(pref_common_pred_val_%d_%d & %u)",pred_class[cref],ppos/32,bitmask);
				ret = tmpstr;
				return ret;
			}
		}

		ret =  pr->get_op() + "(";
		if (pr->is_sampling_fcn) {
			ret += "t->sampling_rate";
			if (!op_list.empty())
				ret += ", ";
		}
		for(o=0;o<op_list.size();++o){
			if(o>0) ret += ", ";
			if(op_list[o]->get_data_type()->is_buffer_type() && (! (op_list[o]->is_handle_ref()) ) )
					ret += "&";
			ret += generate_se_code(op_list[o],schema);
		}
		ret += " )";
		return(ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in generate_predicate_code, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		return("ERROR in generate_predicate_code");
	}
}


static string generate_equality_test(string &lhs_op, string &rhs_op, data_type *dt){
	string ret;

    if(dt->complex_comparison(dt) ){
		ret += dt->get_comparison_fcn(dt);
		ret += "(";
			if(dt->is_buffer_type() ) ret += "&";
		ret += lhs_op;
		ret += ", ";
			if(dt->is_buffer_type() ) ret += "&";
		ret += rhs_op;
		ret += ") == 0";
	}else{
		ret += lhs_op;
		ret += " == ";
		ret += rhs_op;
	}

	return(ret);
}

static string generate_comparison(string &lhs_op, string &rhs_op, data_type *dt){
	string ret;

    if(dt->complex_comparison(dt) ){
		ret += dt->get_comparison_fcn(dt);
		ret += "(";
			if(dt->is_buffer_type() ) ret += "&";
		ret += lhs_op;
		ret += ", ";
			if(dt->is_buffer_type() ) ret += "&";
		ret += rhs_op;
		ret += ") == 0";
	}else{
		ret += lhs_op;
		ret += " == ";
		ret += rhs_op;
	}

	return(ret);
}

//		Here I assume that only MIN and MAX aggregates can be computed
//		over BUFFER data types.

static string generate_aggr_update(string var, aggregate_table *atbl,int aidx, table_list *schema){
	string retval = "\t\t";
	string op = atbl->get_op(aidx);

//		Is it a UDAF
	if(! atbl->is_builtin(aidx)) {
		int o;
		retval += op+"_LFTA_AGGR_UPDATE_(";
		if(atbl->get_storage_type(aidx)->get_type() != fstring_t) retval+="&";
		retval+="("+var+")";
		vector<scalarexp_t *> opl = atbl->get_operand_list(aidx);
		for(o=0;o<opl.size();++o){
			retval += ",";
			if(opl[o]->get_data_type()->is_buffer_type() && (! (opl[o]->is_handle_ref()) ) )
					retval.append("&");
			retval += generate_se_code(opl[o], schema);
		}
		retval += ");\n";

		return retval;
	}

//		Built-in aggregate processing.

	data_type *dt = atbl->get_data_type(aidx);

	if(op == "COUNT"){
		retval.append(var);
		retval.append("++;\n");
		return(retval);
	}
	if(op == "SUM"){
		retval.append(var);
		retval.append(" += ");
		retval.append(generate_se_code(atbl->get_aggr_se(aidx), schema) );
		retval.append(";\n");
		return(retval);
	}
	if(op == "MIN"){
		sprintf(tmpstr,"aggr_tmp_%d = %s;\n",aidx,generate_se_code(atbl->get_aggr_se(aidx), schema ).c_str() );
		retval.append(tmpstr);
		if(dt->complex_comparison(dt)){
			if(dt->is_buffer_type())
			  sprintf(tmpstr,"\t\tif(%s(&aggr_tmp_%d,&(%s)) < 0)\n",dt->get_comparison_fcn(dt).c_str(), aidx, var.c_str());
			else
			  sprintf(tmpstr,"\t\tif(%s(aggr_tmp_%d,%s) < 0)\n",dt->get_comparison_fcn(dt).c_str(), aidx, var.c_str());
		}else{
			sprintf(tmpstr,"\t\tif(aggr_tmp_%d < %s)\n",aidx,var.c_str());
		}
		retval.append(tmpstr);
		if(dt->is_buffer_type()){
			sprintf(tmpstr,"\t\t\t%s(f,&(%s),&aggr_tmp_%d);\n",dt->get_buffer_replace().c_str(),var.c_str(),aidx);
		}else{
			sprintf(tmpstr,"\t\t\t%s = aggr_tmp_%d;\n",var.c_str(),aidx);
		}
		retval.append(tmpstr);

		return(retval);
	}
	if(op == "MAX"){
		sprintf(tmpstr,"aggr_tmp_%d = %s;\n",aidx,generate_se_code(atbl->get_aggr_se(aidx), schema ).c_str() );
		retval.append(tmpstr);
		if(dt->complex_comparison(dt)){
			if(dt->is_buffer_type())
			 sprintf(tmpstr,"\t\tif(%s(&aggr_tmp_%d,&(%s)) > 0)\n",dt->get_comparison_fcn(dt).c_str(), aidx, var.c_str());
			else
			 sprintf(tmpstr,"\t\tif(%s(aggr_tmp_%d,%s) > 0)\n",dt->get_comparison_fcn(dt).c_str(), aidx, var.c_str());
		}else{
			sprintf(tmpstr,"\t\tif(aggr_tmp_%d > %s)\n",aidx,var.c_str());
		}
		retval.append(tmpstr);
		if(dt->is_buffer_type()){
			sprintf(tmpstr,"\t\t\t%s(f,&(%s),&aggr_tmp_%d);\n",dt->get_buffer_replace().c_str(),var.c_str(),aidx);
		}else{
			sprintf(tmpstr,"\t\t\t%s = aggr_tmp_%d;\n",var.c_str(),aidx);
		}
		retval.append(tmpstr);

		return(retval);

	}
	if(op == "AND_AGGR"){
		retval.append(var);
		retval.append(" &= ");
		retval.append(generate_se_code(atbl->get_aggr_se(aidx), schema) );
		retval.append(";\n");
		return(retval);
	}
	if(op == "OR_AGGR"){
		retval.append(var);
		retval.append(" |= ");
		retval.append(generate_se_code(atbl->get_aggr_se(aidx), schema) );
		retval.append(";\n");
		return(retval);
	}
	if(op == "XOR_AGGR"){
		retval.append(var);
		retval.append(" ^= ");
		retval.append(generate_se_code(atbl->get_aggr_se(aidx), schema) );
		retval.append(";\n");
		return(retval);
	}
	fprintf(stderr,"INTERNAL ERROR : aggregate %s not recognized in generate_aggr_update.\n",op.c_str());
	return("ERROR: aggregate not recognized: "+op);

}



static string generate_aggr_init(string var, aggregate_table *atbl,int aidx, table_list *schema){
	string retval;
	string op = atbl->get_op(aidx);

//		Is it a UDAF
	if(! atbl->is_builtin(aidx)) {
		int o;
		retval += "\t\t"+op+"_LFTA_AGGR_INIT_(";
		if(atbl->get_storage_type(aidx)->get_type() != fstring_t) retval+="&";
		retval+="("+var+"));\n";
//			Add 1st tupl
		retval += "\t"+atbl->get_op(aidx)+"_LFTA_AGGR_UPDATE_(";
		if(atbl->get_storage_type(aidx)->get_type() != fstring_t) retval+="&";
		retval+="("+var+")";
		vector<scalarexp_t *> opl = atbl->get_operand_list(aidx);
		for(o=0;o<opl.size();++o){
			retval += ",";
			if(opl[o]->get_data_type()->is_buffer_type() && (! (opl[o]->is_handle_ref()) ) )
					retval.append("&");
			retval += generate_se_code(opl[o],schema);
		}
		retval += ");\n";
		return(retval);
	}

//		Built-in aggregate processing.


	data_type *dt = atbl->get_data_type(aidx);

	if(op == "COUNT"){
		retval = "\t\t"+var;
		retval.append(" = 1;\n");
		return(retval);
	}

	if(op == "SUM" || op == "MIN" || op == "MAX" || op == "AND_AGGR" ||
									op == "OR_AGGR" || op == "XOR_AGGR"){
		if(dt->is_buffer_type()){
			sprintf(tmpstr,"\t\taggr_tmp_%d = %s;\n",aidx,generate_se_code(atbl->get_aggr_se(aidx), schema ).c_str() );
			retval.append(tmpstr);
			sprintf(tmpstr,"\t\t%s(f,&(%s),&aggr_tmp_%d);\n",dt->get_buffer_assign_copy().c_str(),var.c_str(),aidx);
			retval.append(tmpstr);
		}else{
			retval = "\t\t"+var;
			retval += " = ";
			retval.append(generate_se_code(atbl->get_aggr_se(aidx), schema));
			retval.append(";\n");
		}
		return(retval);
	}

	fprintf(stderr,"INTERNAL ERROR : aggregate %s not recognized in generate_aggr_init.\n",op.c_str());
	return("ERROR: aggregate not recognized: "+op);
}


////////////////////////////////////////////////////////////


string generate_preamble(table_list *schema, //map<string,string> &int_fcn_defs,
 	std::string &node_name, std::string &schema_embed_str){
//			Include these only once, not once per lfta
//	string ret = "#include \"rts.h\"\n";
//	ret +=  "#include \"fta.h\"\n\n");

	string ret = "#ifndef LFTA_IN_NIC\n";
	ret += "char *"+generate_schema_string_name(node_name)+" = " +schema_embed_str+";\n";
	ret += "#include<stdio.h>\n";
	ret += "#include <limits.h>\n";
	ret += "#include <float.h>\n";
	ret += "#include \"rdtsc.h\"\n";
	ret += "#endif\n";



	return(ret);
}


string generate_tuple_from_aggr(string node_name, table_list *schema, string idx){
	int a,p,s;
//			 need to create and output the tuple.
	string ret = "/*\t\tCreate an output tuple for the aggregate being kicked out \t*/\n";
//			Check for any UDAFs with LFTA_BAILOUT
	ret += "\tlfta_bailout = 0;\n";
   	for(a=0;a<aggr_tbl->size();a++){
		if(aggr_tbl->has_bailout(a)){
			ret += "\tlfta_bailout+="+aggr_tbl->get_op(a)+"_LFTA_AGGR_BAILOUT_(";
			if(aggr_tbl->get_storage_type(a)->get_type() != fstring_t) ret+="&";
			ret+="(t->aggr_table["+idx+"].aggr_var"+int_to_string(a)+"));\n";
		}
	}
	ret += "\tif(! lfta_bailout){\n";

//			First, compute the size of the tuple.

//			Unpack UDAF return values
   	for(a=0;a<aggr_tbl->size();a++){
		if(! aggr_tbl->is_builtin(a)){
			ret += "\t"+aggr_tbl->get_op(a)+"_LFTA_AGGR_OUTPUT_(&(udaf_ret"+int_to_string(a)+"),";
			if(aggr_tbl->get_storage_type(a)->get_type() != fstring_t) ret+="&";
			ret+="(t->aggr_table["+idx+"].aggr_var"+int_to_string(a)+"));\n";

		}
	}


//			Unpack partial fcns ref'd by the select clause.
  if(sl_fcns_start != sl_fcns_end){
	    ret += "\t\tunpack_failed = 0;\n";
    for(p=sl_fcns_start;p<sl_fcns_end;p++){
	  if(is_partial_fcn[p]){
	  	ret += "\t" + unpack_partial_fcn_fm_aggr(partial_fcns[p], p,
	  		 "t->aggr_table["+idx+"].",schema);
	  	ret += "\t\tif(retval) unpack_failed = 1;\n";
	  }
    }
	  							// BEGIN don't allocate tuple if
	ret += "\t\tif( unpack_failed == 0 ){\n"; // unpack failed.
  }

//			Unpack any BUFFER type selections into temporaries
//			so that I can compute their size and not have
//			to recompute their value during tuple packing.
//			I can use regular assignment here because
//			these temporaries are non-persistent.

	  for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if(sdt->is_buffer_type()){
			sprintf(tmpstr,"\t\t\tselvar_%d = ",s);
			ret += tmpstr;
  			ret += generate_se_code_fm_aggr(sl_list[s],"t->aggr_table["+idx+"].",schema);
  			ret += ";\n";
		}
	  }


//		The size of the tuple is the size of the tuple struct plus the
//		size of the buffers to be copied in.

	  ret += "\t\t\ttuple_size = sizeof( struct ";
	  ret +=  generate_tuple_name(node_name);
	  ret += ")";
	  for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if(sdt->is_buffer_type()){
			sprintf(tmpstr," + %s(&selvar_%d)", sdt->get_buffer_size().c_str(),s);
			ret += tmpstr;
		}
	  }
	  ret += ";\n";


	  ret += "\t\t\ttuple = allocate_tuple(f, tuple_size );\n";
	  ret += "\t\t\tif( tuple != NULL){\n";


//			Test passed, make assignments to the tuple.

	  ret += "\t\t\t\ttuple_pos = sizeof( struct ";
	  ret +=  generate_tuple_name(node_name) ;
	  ret += ");\n";

//			Mark tuple as REGULAR_TUPLE
	  ret += "\n\t\t\t\ttuple->tuple_type = REGULAR_TUPLE;\n";

	  for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if(sdt->is_buffer_type()){
			sprintf(tmpstr,"\t\t\t\t%s(&(tuple->tuple_var%d), &selvar_%d, (char *)tuple, ((char *)tuple)+tuple_pos);\n", sdt->get_buffer_tuple_copy().c_str(),s, s);
			ret += tmpstr;
			sprintf(tmpstr,"\t\t\t\ttuple_pos += %s(&selvar_%d);\n", sdt->get_buffer_size().c_str(), s);
			ret += tmpstr;
		}else{
  			sprintf(tmpstr,"\t\t\t\ttuple->tuple_var%d = ",s);
  			ret += tmpstr;
//  			if(sdt->needs_hn_translation())
//  				ret += sdt->hton_translation() +"( ";
  			ret += generate_se_code_fm_aggr(sl_list[s],"t->aggr_table["+idx+"].",schema);
//  			if(sdt->needs_hn_translation())
//  				ret += ") ";
  			ret += ";\n";
		}
	  }

//		Generate output.
	  ret += "\t\t\t\tpost_tuple(tuple);\n";
	  ret += "\t\t\t\t#ifdef LFTA_STATS\n";
	  ret+="\t\t\t\tt->out_tuple_cnt++;\n";
	  ret+="\t\t\t\tt->out_tuple_sz+=tuple_size;\n";
	  ret += "\t\t\t\t#endif\n\n";
	  ret += "\t\t\t}\n";

	  if(sl_fcns_start != sl_fcns_end)	// END don't allocate tuple if
	  	ret += "\t\t}\n";				// unpack failed.
	  ret += "\t}\n";

//			Need to release memory held by BUFFER types.
		int g;

	  for(g=0;g<gb_tbl->size();g++){
		data_type *gdt = gb_tbl->get_data_type(g);
		if(gdt->is_buffer_type()){
			sprintf(tmpstr,"\t\t\t%s(&(t->aggr_table[%s].gb_var%d));\n",gdt->get_buffer_destroy().c_str(),idx.c_str(),g);
			ret += tmpstr;
		}
	  }
	  for(a=0;a<aggr_tbl->size();a++){
		if(aggr_tbl->is_builtin(a)){
			data_type *adt = aggr_tbl->get_data_type(a);
			if(adt->is_buffer_type()){
				sprintf(tmpstr,"\t\t\t%s(&(t->aggr_table[%s].aggr_var%d));\n",adt->get_buffer_destroy().c_str(),idx.c_str(),a);
				ret += tmpstr;
			}
		}else{
			ret += "\t\t"+aggr_tbl->get_op(a)+"_LFTA_AGGR_DESTROY_(";
			if(aggr_tbl->get_storage_type(a)->get_type() != fstring_t) ret+="&";
			ret+="(t->aggr_table["+idx+"].aggr_var"+int_to_string(a)+"));\n";
		}
	  }

	ret += "\t\tt->n_aggrs--;\n";

	return(ret);

}

string generate_gb_match_test(string idx){
	int g;
	string ret="\tif (IS_FILLED(t->aggr_table_bitmap, "+idx+") &&  IS_NEW(t->aggr_table_bitmap,"+idx+")";
	if(gb_tbl->size()>0){
		ret+="\n\t/* \t\tcheck if the grouping variables are equal */\n";
		ret+="\t\t";

//			Next, scan list for a match on the group-by attributes.
	  string rhs_op, lhs_op;
	  for(g=0;g<gb_tbl->size();g++){
		  ret += " && ";
		  ret += "(";
		  sprintf(tmpstr,"gb_attr_%d",g); lhs_op = tmpstr;
		  sprintf(tmpstr,"t->aggr_table[%s].gb_var%d",idx.c_str(),g); rhs_op = tmpstr;
		  ret += generate_equality_test(lhs_op, rhs_op, gb_tbl->get_data_type(g) );
		  ret += ")";
	  }
	 }

	  ret += "){\n";

	return ret;
}

string generate_gb_update(string node_name, table_list *schema, string idx, bool has_udaf){
	int g;
	string ret;

	  ret += "/*\t\tMatch found : update in place.\t*/\n";
	  int a;
	  has_udaf = false;
	  for(a=0;a<aggr_tbl->size();a++){
		  sprintf(tmpstr,"t->aggr_table[%s].aggr_var%d",idx.c_str(),a);
		  ret += generate_aggr_update(tmpstr,aggr_tbl,a, schema);
		  if(! aggr_tbl->is_builtin(a)) has_udaf = true;
	  }

//			garbage collect copied buffer type gb attrs.
  for(g=0;g<gb_tbl->size();g++){
	  data_type *gdt = gb_tbl->get_data_type(g);
	  if(gdt->is_buffer_type()){
	  	sprintf(tmpstr,"\t\t\t%s(&(gb_attr_%d));\n",gdt->get_buffer_destroy().c_str(),g);
		ret+=tmpstr;
	  }
	}



	  bool first_udaf = true;
	  if(has_udaf){
		ret += "\t\tif(";
	   	for(a=0;a<aggr_tbl->size();a++){
			if(! aggr_tbl->is_builtin(a)){
				if(! first_udaf)ret += " || ";
				else first_udaf = false;
				ret += aggr_tbl->get_op(a)+"_LFTA_AGGR_FLUSHME_(";
				if(aggr_tbl->get_storage_type(a)->get_type() != fstring_t) ret+="&";
				ret+="(t->aggr_table["+idx+"].aggr_var"+int_to_string(a)+"))";
			}
		}
		ret+="){\n";
		ret+=" fta_aggr_flush_old_"+node_name+"(f,t->max_aggrs);\n";
		ret += generate_tuple_from_aggr(node_name,schema,idx);
		ret += "\t\tt->aggr_table_hashmap["+idx+"] &= ~SLOT_FILLED;\n";
		ret+="\t\t}\n";
	}
	return ret;
}


string generate_init_group( table_list *schema, string idx){
	  int g,a;
	  string ret="\t\t\tt->aggr_table_hashmap["+idx+"] = hash2 | SLOT_FILLED | gen_val;\n";
//			Fill up the aggregate block.
	  for(g=0;g<gb_tbl->size();g++){
		  sprintf(tmpstr,"\t\t\tt->aggr_table[%s].gb_var%d = gb_attr_%d;\n",idx.c_str(),g,g);
		  ret += tmpstr;
	  }
	  for(a=0;a<aggr_tbl->size();a++){
		  sprintf(tmpstr,"t->aggr_table[%s].aggr_var%d",idx.c_str(),a);
		  ret += generate_aggr_init(tmpstr, aggr_tbl,a,  schema);
	  }
	  ret+="\t\tt->n_aggrs++;\n";
	return ret;
}


string generate_fta_flush(string node_name, table_list *schema,
		ext_fcn_list *Ext_fcns){

   string ret;
   string select_var_defs ;
	int s, p;

//		Flush from previous epoch

	ret+="static void fta_aggr_flush_old_"+node_name+"(struct FTA *f, unsigned int nflush){\n";

	ret += "\tgs_int32_t tuple_size, tuple_pos;\n";
    ret += "\tstruct "+generate_tuple_name(node_name)+" *tuple;\n";
	ret += "\tint i, lfta_bailout;\n";
	ret += "\tunsigned int gen_val;\n";

	ret += "\tstruct "+generate_fta_name(node_name)+" * t = (struct ";
	ret += generate_fta_name(node_name)+" *) f;\n";

	ret += "\n";


//		Variables needed to store selected attributes of BUFFER type
//		temporarily, in order to compute their size for storage
//		in an output tuple.

  select_var_defs = "";
  for(s=0;s<sl_list.size();s++){
	data_type *sdt = sl_list[s]->get_data_type();
	if(sdt->is_buffer_type()){
	  sprintf(tmpstr,"\t%s selvar_%d;\n",sdt->get_cvar_type().c_str(),s);
	  select_var_defs.append(tmpstr);
	}
  }
  if(select_var_defs != ""){
	ret += "/*\t\tTemporaries for computing buffer sizes.\t*/\n";
    ret += select_var_defs;
  }


//		Variables to store results of partial functions.
  if(sl_fcns_start != sl_fcns_end){
	ret += "/*\t\tVariables to store the results of partial functions.\t*/\n";
  	for(p=sl_fcns_start;p<sl_fcns_end;p++){
		sprintf(tmpstr,"\t%s partial_fcn_result_%d;\n",
			partial_fcns[p]->get_data_type()->get_cvar_type().c_str(), p);
		ret += tmpstr;
  	}
	ret += "\tgs_retval_t retval = 0;\n\tint unpack_failed = 0;\n;";
  }

//		Variables for udaf output temporaries
	bool no_udaf = true;
	int a;
   	for(a=0;a<aggr_tbl->size();a++){
		if(! aggr_tbl->is_builtin(a)){
			if(no_udaf){
				ret+="/*\t\tUDAF output vars.\t*/\n";
				no_udaf = false;
			}
			int afcn_id = aggr_tbl->get_fcn_id(a);
			data_type *adt = Ext_fcns->get_fcn_dt(afcn_id);
			sprintf(tmpstr,"udaf_ret%d", a);
			ret+="\t"+adt->make_cvar(tmpstr)+";\n";
		}
	}


//  ret+="\tt->flush_finished=1; /* flush will be completed */\n";
  ret+="\n";
  ret+="\tgen_val = t->generation & SLOT_GEN_BITS;\n";
  ret+="\tfor (i=t->flush_pos; (i < t->max_aggrs) && t->n_aggrs && nflush>0; ++i){\n";
  ret+="\t\tif ( (t->aggr_table_hashmap[i] & SLOT_FILLED) && (((t->aggr_table_hashmap[i] & SLOT_GEN_BITS) != gen_val ) || (";
		bool first_g=true;
		int g;
		for(g=0;g<gb_tbl->size();g++){
		  data_type *gdt = gb_tbl->get_data_type(g);
		  if(gdt->is_temporal()){
			if(first_g) first_g=false; else ret+=" || ";
			ret += "t->last_gb_"+int_to_string(g)+" > t->aggr_table[i].gb_var"+int_to_string(g)+" ";
		  }
		}
  ret += "))) {\n";
  ret+="\t\t\tt->aggr_table_hashmap[i] = 0;\n";
  ret+=
"#ifdef LFTA_STATS\n"
"\t\t\tt->eviction_cnt++;\n"
"#endif\n"
;


  ret+=generate_tuple_from_aggr(node_name,schema,"i");

//  ret+="\t\t\tt->n_aggrs--;\n";  // done in generate_tuple_from_aggr
  ret+="\t\t\tnflush--;\n";
  ret+="\t\t}\n";
  ret+="\t}\n";
  ret+="\tt->flush_pos=i;\n";
  ret+="\tif(t->n_aggrs == 0) {\n";
  ret+="\t\tt->flush_pos = t->max_aggrs;\n";
  ret += "\t}\n\n";

  ret+="\tif(t->flush_pos == t->max_aggrs) {\n";

  for(int g=0;g<gb_tbl->size();g++){
	data_type *dt = gb_tbl->get_data_type(g);
	if(dt->is_temporal()){
		data_type *gdt = gb_tbl->get_data_type(g);
		if(!gdt->is_buffer_type()){
			sprintf(tmpstr,"\t\tt->last_flushed_gb_%d = t->flush_start_gb_%d;\n",g,g);
			ret += tmpstr;
		}
	}
  }
  ret += "\t}\n}\n\n";

  return(ret);
}


string generate_fta_load_params(string node_name){
	int p;
	vector<string> param_names = param_tbl->get_param_names();

	string ret = "static int load_params_"+node_name+"(struct "+generate_fta_name(node_name);
		ret += " *t, int sz, void *value, int initial_call){\n";
    ret += "\tint pos=0;\n";
    ret += "\tint data_pos;\n";

	for(p=0;p<param_names.size();p++){
		data_type *dt = param_tbl->get_data_type(param_names[p]);
		if(dt->is_buffer_type()){
			sprintf(tmpstr,"\t%s tmp_var_%s;\n",dt->get_cvar_type().c_str(), param_names[p].c_str() );
			ret += tmpstr;
		}
	}



	ret += "\n\tdata_pos = ";
	for(p=0;p<param_names.size();p++){
		if(p>0) ret += " + ";
		data_type *dt = param_tbl->get_data_type(param_names[p]);
		ret += "sizeof( ";
		ret +=  dt->get_cvar_type();
		ret += " )";
	}
	ret += ";\n";
	ret += "\tif(data_pos > sz) return 1;\n\n";


	for(p=0;p<param_names.size();p++){
		data_type *dt = param_tbl->get_data_type(param_names[p]);
		if(dt->is_buffer_type()){
			sprintf(tmpstr,"\ttmp_var_%s =  *( (%s *)((char *)value+pos) );\n",param_names[p].c_str(), dt->get_cvar_type().c_str() );
			ret += tmpstr;
			switch( dt->get_type() ){
			case v_str_t:
//				ret += "\ttmp_var_"+param_names[p]+".data = ntohl( tmp_var_"+param_names[p]+".data );\n";		// ntoh conversion
//				ret += "\ttmp_var_"+param_names[p]+".length = ntohl( tmp_var_"+param_names[p]+".length );\n";	// ntoh conversion
				sprintf(tmpstr,"\tif( (int)(tmp_var_%s.data) + tmp_var_%s.length > sz) return 1;\n",param_names[p].c_str(), param_names[p].c_str() );
				ret += tmpstr;
				sprintf(tmpstr,"\ttmp_var_%s.data = (void *)( (char *)value + (int)(tmp_var_%s.data) );\n",param_names[p].c_str(), param_names[p].c_str() );
				ret += tmpstr;
			break;
			default:
				fprintf(stderr,"ERROR: parameter %s is of type %s, a buffered type, but I don't know how to unpack it as a parameter.\n",param_names[p].c_str(), dt->get_type_str().c_str() );
				exit(1);
			break;
			}
//					First, destroy the old
			ret += "\tif(! initial_call)\n";
			sprintf(tmpstr,"\t\t%s(&(t->param_%s));\n",dt->get_buffer_destroy().c_str(),param_names[p].c_str());
			ret += tmpstr;
//					Next, create the new.
			sprintf(tmpstr,"\t%s((struct FTA *)t, &(t->param_%s), &tmp_var_%s);\n", dt->get_buffer_assign_copy().c_str(), param_names[p].c_str(), param_names[p].c_str() );
			ret += tmpstr;
		}else{
//			if(dt->needs_hn_translation()){
//				sprintf(tmpstr,"\tt->param_%s =  %s( *( (%s *)( (char *)value+pos) ) );\n",
//				  param_names[p].c_str(), dt->ntoh_translation().c_str(), dt->get_cvar_type().c_str() );
//			}else{
				sprintf(tmpstr,"\tt->param_%s =  *( (%s *)( (char *)value+pos) );\n",
				  param_names[p].c_str(), dt->get_cvar_type().c_str() );
//			}
			ret += tmpstr;
		}
		sprintf(tmpstr,"\tpos += sizeof( %s );\n",dt->get_cvar_type().c_str() );
		ret += tmpstr;
	}

//			Register the pass-by-handle parameters

	ret += "/* register and de-register the pass-by-handle parameters */\n";

    int ph;
    for(ph=0;ph<param_handle_table.size();++ph){
		data_type pdt(param_handle_table[ph]->type_name);
		switch(param_handle_table[ph]->val_type){
		case cplx_lit_e:
			break;
		case litval_e:
			break;
		case param_e:
			ret += "\tif(! initial_call)\n";
			sprintf(tmpstr, "\t\t%s(t->handle_param_%d);\n",
				param_handle_table[ph]->lfta_deregistration_fcn().c_str(),ph);
			ret += tmpstr;
			sprintf(tmpstr,"\tt->handle_param_%d = %s((struct FTA *)t,",ph,param_handle_table[ph]->lfta_registration_fcn().c_str());
			ret += tmpstr;

			if(pdt.is_buffer_type()) ret += "&(";
			ret += "t->param_"+param_handle_table[ph]->param_name;
			if(pdt.is_buffer_type()) ret += ")";
			ret += ");\n";
			break;
		default:
			sprintf(tmpstr, "INTERNAL ERROR unknown case (%d) found when processing pass-by-handle parameter table.",param_handle_table[ph]->val_type);
			fprintf(stderr,"%s\n",tmpstr);
			ret+=tmpstr;
		}
	}

	ret+="\treturn 0;\n";
	ret += "}\n\n";

	return(ret);
}




string generate_fta_free(string node_name, bool is_aggr_query){

	string ret="static gs_retval_t free_fta_"+node_name+"(struct FTA *f, gs_uint32_t recursive){\n";
	ret+= "\tstruct "+generate_fta_name(node_name)+
		" * t = (struct "+generate_fta_name(node_name)+" *) f;\n";
	ret += "\tint i;\n";

	if(is_aggr_query){
		ret+="\tfta_aggr_flush_old_" + node_name+"(f,t->max_aggrs);\n";
		ret+="\t/* \t\tmark all groups as old */\n";
		ret+="\tt->generation++;\n";
		ret+="\tt->flush_pos = 0;\n";
                ret+="\tfta_aggr_flush_old_" + node_name+"(f,t->max_aggrs);\n";
	}

//			Deregister the pass-by-handle parameters
	ret += "/* de-register the pass-by-handle parameters */\n";
    int ph;
    for(ph=0;ph<param_handle_table.size();++ph){
		sprintf(tmpstr, "\t%s(t->handle_param_%d);\n",
			param_handle_table[ph]->lfta_deregistration_fcn().c_str(),ph);
		ret += tmpstr;
	}


	ret += "\treturn 0;\n}\n\n";
	return(ret);
}


string generate_fta_control(string node_name, table_list *schema, bool is_aggr_query){
	string ret="static gs_retval_t control_fta_"+node_name+"(struct FTA *f,  gs_int32_t command, gs_int32_t sz, void *value){\n";
	ret += "\tstruct "+generate_fta_name(node_name)+" * t = (struct ";
		ret += generate_fta_name(node_name)+" *) f;\n\n";
	ret+="\tint i;\n";


	ret += "\t/* temp status tuple */\n";
	ret += "\tstruct "+generate_tuple_name(node_name)+" *tuple;\n";
 	ret += "\tgs_int32_t tuple_size;\n";


	if(is_aggr_query){
		ret+="\tif(command == FTA_COMMAND_FLUSH){\n";

		ret+="\t\tif (!t->n_aggrs) {\n";
		ret+="\t\t\ttuple = allocate_tuple(f, 0);\n";
		ret+="\t\t\tif( tuple != NULL)\n";
		ret+="\t\t\t\tpost_tuple(tuple);\n";

		ret+="\t\t}else{\n";

                ret+="\t\t\tfta_aggr_flush_old_" + node_name+"(f,t->max_aggrs);\n";
		ret+="\t\t\t/* \t\tmark all groups as old */\n";
		ret +="\t\tt->generation++;\n";
		ret += "//\tmarking groups old should happen implicitly by advancing the generation.\n";
		ret+="//\t\t\tfor (i = 0; i < t->bitmap_size; ++i)\n";
		ret+="//\t\t\t\tt->aggr_table_bitmap[i] &= 0xAAAAAAAA;\n";
		ret+="\t\t\tt->flush_pos = 0;\n";
                ret+="\t\t\tfta_aggr_flush_old_" + node_name+"(f,t->max_aggrs);\n";
		ret+="\t\t}\n";

		ret+="\t}\n";
	}
	if(param_tbl->size() > 0){
		ret+=
"\tif(command == FTA_COMMAND_LOAD_PARAMS){\n"
"\t\tif(load_params_"+node_name+"(t, sz, value, 0))\n"
"#ifndef LFTA_IN_NIC\n"
"\t\t\tfprintf(stderr,\"WARNING: parameter passed to lfta "+node_name+" is too small, ignored.\\n\");\n"
"#else\n"
"\t\t{}\n"
"#endif\n"
"\t}\n";
	}
	ret+=
"\tif(command == FTA_COMMAND_SET_SAMPLING_RATE){\n"
"\t\tmemcpy(&t->sampling_rate, value, sizeof(gs_float_t));\n"
"\t}\n\n";


	ret += "\tif(command == FTA_COMMAND_FILE_DONE ){\n";

	if(is_aggr_query){
		ret+="\t\tif (t->n_aggrs) {\n";
		ret+="\t\t\tfta_aggr_flush_old_" + node_name+"(f,t->max_aggrs);\n";
		ret+="\t\t\t/* \t\tmark all groups as old */\n";
		ret +="\t\tt->generation++;\n";
		ret += "//\tmarking groups old should happen implicitly by advancing the generation.\n";
		ret+="//\t\t\tfor (i = 0; i < t->bitmap_size; ++i)\n";
		ret+="//\t\t\t\tt->aggr_table_bitmap[i] &= 0xAAAAAAAA;\n";
		ret+="\t\t\tt->flush_pos = 0;\n";
		ret+="\t\t\tfta_aggr_flush_old_" + node_name+"(f,t->max_aggrs);\n";
		ret+="\t\t}\n";
	}

	ret += "\t\ttuple_size = sizeof( struct "+generate_tuple_name(node_name)+");\n";
	ret += "\t\ttuple = allocate_tuple(f, tuple_size );\n";
	ret += "\t\tif( tuple == NULL)\n\t\treturn 1;\n";

	/* mark tuple as EOF_TUPLE */
	ret += "\n\t\t/* Mark tuple as eof_tuple */\n";
	ret += "\t\ttuple->tuple_type = EOF_TUPLE;\n";
	ret += "\t\tpost_tuple(tuple);\n";
	ret += "\t}\n";

	ret += "\treturn 0;\n}\n\n";

	return(ret);
}

string generate_fta_clock(string node_name, table_list *schema, unsigned time_corr, bool is_aggr_query){
	string ret="static gs_retval_t clock_fta_"+node_name+"(struct FTA *f){\n";
	ret += "\tstruct "+generate_fta_name(node_name)+" * t = (struct ";
		ret += generate_fta_name(node_name)+" *) f;\n\n";

	ret += "\t/* Create a temp status tuple */\n";
	ret += "\tstruct "+generate_tuple_name(node_name)+" *tuple;\n";
 	ret += "\tgs_int32_t tuple_size;\n";
 	ret += "\tunsigned int i;\n";
 	ret += "\ttime_t cur_time;\n";
 	ret += "\tint time_advanced;\n";
 	ret += "\tstruct fta_stat stats;\n";



	/* copy the last seen values of temporal attributes */
	col_id_set temp_cids;		//	col ids of temp attributes in select clause


	/* HACK: in order to reuse the SE generation code, we need to copy
	 * the last values of the temp attributes into new variables
	 * which have names unpack_var_XXX_XXX
	 */

	int s, g;
	col_id_set::iterator csi;

	for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if (sdt->is_temporal()) {
			gather_se_col_ids(sl_list[s],temp_cids, gb_tbl);
		}
	}

	for(csi=temp_cids.begin(); csi != temp_cids.end();++csi){
		int tblref = (*csi).tblvar_ref;
		int schref = (*csi).schema_ref;
		string field = (*csi).field;
		data_type dt(schema->get_type_name(schref,field), schema->get_modifier_list(schref,field));
		sprintf(tmpstr,"\t%s unpack_var_%s_%d;\n", dt.get_cvar_type().c_str(), field.c_str(), tblref);
		ret += tmpstr;
	}

	if (is_aggr_query) {
		for(g=0;g<gb_tbl->size();g++){
			data_type *gdt = gb_tbl->get_data_type(g);
			if(gdt->is_temporal()){
				sprintf(tmpstr,"\t%s gb_attr_%d;\n",gb_tbl->get_data_type(g)->get_cvar_type().c_str(),g);
				ret += tmpstr;
				data_type *gdt = gb_tbl->get_data_type(g);
				if(gdt->is_buffer_type()){
				sprintf(tmpstr,"\t%s gb_attr_tmp%d;\n",gb_tbl->get_data_type(g)->get_cvar_type().c_str(),g);
				ret += tmpstr;
				}
			}
		}
	}
	ret += "\n";

	ret += "\ttime_advanced = 0;\n";

	for(csi=temp_cids.begin(); csi != temp_cids.end();++csi){
		int tblref = (*csi).tblvar_ref;
		int schref = (*csi).schema_ref;
		string field = (*csi).field;
		data_type dt(schema->get_type_name(schref,field), schema->get_modifier_list(schref,field));

		// update last seen value with the value seen
		ret += "\t#ifdef PREFILTER_DEFINED\n";
		sprintf(tmpstr,"\tif (prefilter_temp_vars.unpack_var_%s_%d > t->last_%s_%d) {\n\t\tt->last_%s_%d = prefilter_temp_vars.unpack_var_%s_%d;\n",
			field.c_str(), tblref, field.c_str(), tblref, field.c_str(), tblref, field.c_str(), tblref);
		ret += tmpstr;
		ret += "\t\ttime_advanced = 1;\n\t}\n";
		ret += "\t#endif\n";

                // we need to pay special attention to time fields
                if (field == "time" || field == "timestamp"){
                        ret += "\tcur_time = time(&cur_time);\n";

                        if (field == "time") {
                        	sprintf(tmpstr,"\tif (!gscp_blocking_mode() && (t->last_time_%d < (cur_time - %d))) {\n",
                        	        tblref, time_corr);
                        	ret += tmpstr;
                                sprintf(tmpstr,"\t\tunpack_var_%s_%d = t->last_%s_%d = cur_time - %d;\n",
                                        field.c_str(), tblref, field.c_str(), tblref, time_corr);
                        } else {
	                        sprintf(tmpstr,"\tif (!gscp_blocking_mode() && ((gs_uint32_t)(t->last_%s_%d>>32) < (cur_time - %d))) {\n",
	                                field.c_str(), tblref, time_corr);
	                        ret += tmpstr;
                                sprintf(tmpstr,"\t\tunpack_var_%s_%d = t->last_%s_%d = ((gs_uint64_t)(cur_time - %d))<<32;\n",
                                        field.c_str(), tblref, field.c_str(), tblref, time_corr);
                        }
                        ret += tmpstr;

                        ret += "\t\ttime_advanced = 1;\n";
                        ret += "\t}\n";

                        sprintf(tmpstr,"\telse\n\t\tunpack_var_%s_%d = t->last_%s_%d;\n",
                                field.c_str(), tblref, field.c_str(), tblref);
                        ret += tmpstr;
		} else {
			sprintf(tmpstr,"\tunpack_var_%s_%d = t->last_%s_%d;\n",
				field.c_str(), tblref, field.c_str(), tblref);
			ret += tmpstr;
		}
	}

	// for aggregation lftas we need to check if the time was advanced beyond the current epoch
	if (is_aggr_query) {

		string change_test;
		bool first_one = true;
		for(g=0;g<gb_tbl->size();g++){
		  data_type *gdt = gb_tbl->get_data_type(g);
		  if(gdt->is_temporal()){
//					To perform the test, first need to compute the value
//					of the temporal gb attrs.
			  if(gdt->is_buffer_type()){
	//				NOTE : if the SE defining the gb is anything
	//				other than a ref to a variable, this will generate
	//				illegal code.  To be resolved with Spatch.
				sprintf(tmpstr,"\tgb_attr_tmp%d = %s;\n",
					g, generate_se_code(gb_tbl->get_def(g),schema).c_str() );
				ret+=tmpstr;
				sprintf(tmpstr,"\t%s(f, &gb_attr_%d, &gb_attr_tmp%d);\n",
					gdt->get_buffer_assign_copy().c_str(), g, g);
			  }else{
				sprintf(tmpstr,"\tgb_attr_%d = %s;\n",g,generate_se_code(gb_tbl->get_def(g),schema).c_str());
			  }
			  ret += tmpstr;

			  sprintf(tmpstr,"t->last_gb_%d",g);   string lhs_op = tmpstr;
			  sprintf(tmpstr,"gb_attr_%d",g);   string rhs_op = tmpstr;
			  if(first_one){first_one = false;} else {change_test.append(") && (");}
			  change_test.append(generate_equality_test(lhs_op, rhs_op, gdt));
		  }
		}

		ret += "\n\tif( time_advanced && !( (";
		ret += change_test;
		ret += ") ) ){\n";

		ret += "\n/*\t\tFlush the aggregates if the temporal gb attrs have changed.\t*/\n";
		ret += "\t\tif(t->flush_pos<t->max_aggrs) \n";
		ret += "\t\t\tfta_aggr_flush_old_"+node_name+"(f,t->max_aggrs);\n";

		ret += "\t\t/* \t\tmark all groups as old */\n";
		ret +="\t\tt->generation++;\n";
		ret += "//\tmarking groups old should happen implicitly by advancing the generation.\n";
		ret += "//\t\tfor (i = 0; i < t->bitmap_size; ++i)\n";
		ret += "//\t\t\tt->aggr_table_bitmap[i] &= 0xAAAAAAAA;\n";
		ret += "\t\tt->flush_pos = 0;\n";

		for(g=0;g<gb_tbl->size();g++){
                     data_type *gdt = gb_tbl->get_data_type(g);
                     if(gdt->is_temporal()){
                     	sprintf(tmpstr,"\t\tt->flush_start_gb_%d = gb_attr_%d;\n",g,g);			ret += tmpstr;
                     	sprintf(tmpstr,"\t\tt->last_gb_%d = gb_attr_%d;\n",g,g);			ret += tmpstr;
                     }
                }
		ret += "\t}\n\n";

	}

	ret += "\ttuple_size = sizeof( struct "+generate_tuple_name(node_name)+") + sizeof(gs_uint64_t) + sizeof(struct fta_stat);\n";
	ret += "\ttuple = allocate_tuple(f, tuple_size );\n";
	ret += "\tif( tuple == NULL)\n\t\treturn 1;\n";


	for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if(sdt->is_temporal()){

			if (sl_list[s]->is_gb()) {
				sprintf(tmpstr,"\tt->last_flushed_gb_%d = (t->n_aggrs) ? t->last_flushed_gb_%d : %s;\n",sl_list[s]->get_gb_ref(), sl_list[s]->get_gb_ref(), generate_se_code(sl_list[s],schema).c_str());
				ret += tmpstr;
			}

			sprintf(tmpstr,"\ttuple->tuple_var%d = ",s);
  			ret += tmpstr;
//  			if(sdt->needs_hn_translation())
//  				ret += sdt->hton_translation() +"( ";
  			if (sl_list[s]->is_gb()) {
				sprintf(tmpstr, "t->last_flushed_gb_%d",sl_list[s]->get_gb_ref());
				ret += tmpstr;
			} else{
  				ret += generate_se_code(sl_list[s],schema);
			}
//  			if(sdt->needs_hn_translation())
//  				ret += " )";
  			ret += ";\n";
		}
	}

	/* mark tuple as temporal */
	ret += "\n\t/* Mark tuple as temporal */\n";
	ret += "\ttuple->tuple_type = TEMPORAL_TUPLE;\n";

	ret += "\n\t/* Copy trace id */\n";
	ret += "\tmemcpy((gs_sp_t)tuple+sizeof( struct "+generate_tuple_name(node_name)+"), &t->trace_id, sizeof(gs_uint64_t));\n\n";

	ret += "\n\t/* Populate runtime stats */\n";
	ret += "\tstats.ftaid = f->ftaid;\n";
	ret += "\tstats.in_tuple_cnt = t->in_tuple_cnt;\n";
	ret += "\tstats.out_tuple_cnt = t->out_tuple_cnt;\n";
	ret += "\tstats.out_tuple_sz = t->out_tuple_sz;\n";
	ret += "\tstats.accepted_tuple_cnt = t->accepted_tuple_cnt;\n";
	ret += "\tstats.cycle_cnt = t->cycle_cnt;\n";
	ret += "\tstats.collision_cnt = t->collision_cnt;\n";
	ret += "\tstats.eviction_cnt = t->eviction_cnt;\n";
	ret += "\tstats.sampling_rate	 = t->sampling_rate;\n";

	ret += "\n#ifdef LFTA_PROFILE\n";
	ret += "\n\t/* Print stats */\n";
	ret += "\tfprintf(stderr, \"STATS " + node_name + " \");\n";
	ret += "\tfprintf(stderr, \"in_tuple_cnt= %u \", t->in_tuple_cnt);\n";
	ret += "\tfprintf(stderr, \"out_tuple_cnt= %u \", t->out_tuple_cnt);\n";
	ret += "\tfprintf(stderr, \"out_tuple_sz= %u \", t->out_tuple_sz);\n";
	ret += "\tfprintf(stderr, \"accepted_tuple_cnt= %u \", t->accepted_tuple_cnt);\n";
	ret += "\tfprintf(stderr, \"cycle_cnt= %llu \", t->cycle_cnt);\n";
	ret += "\tfprintf(stderr, \"cycle_per_in_tuple= %lf \", ((double)t->cycle_cnt)/((double)t->in_tuple_cnt));\n";
	ret += "\tfprintf(stderr, \"collision_cnt= %d\\n\", t->collision_cnt);\n\n";
	ret += "\tfprintf(stderr, \"eviction_cnt= %d\\n\", t->eviction_cnt);\n\n";
	ret += "\n#endif\n";


	ret += "\n\t/* Copy stats */\n";
	ret += "\tmemcpy((gs_sp_t)tuple+sizeof( struct "+generate_tuple_name(node_name)+") + sizeof(gs_uint64_t), &stats, sizeof(fta_stat));\n\n";
	ret+="\tpost_tuple(tuple);\n";

	ret += "\n\t/* Send a heartbeat message to clearinghouse */\n";
	ret += "\tfta_heartbeat(f->ftaid, t->trace_id++, 1, &stats);\n";

	ret += "\n\t/* Reset runtime stats */\n";
	ret += "\tt->in_tuple_cnt = 0;\n";
	ret += "\tt->out_tuple_cnt = 0;\n";
	ret += "\tt->out_tuple_sz = 0;\n";
	ret += "\tt->accepted_tuple_cnt = 0;\n";
	ret += "\tt->cycle_cnt = 0;\n";
	ret += "\tt->collision_cnt = 0;\n";
	ret += "\tt->eviction_cnt = 0;\n";

	ret += "\treturn 0;\n}\n\n";

	return(ret);
}


//		accept processing before the where clause,
//		do flush processwing.
string generate_aggr_accept_prelim(qp_node *fs, string node_name, table_list *schema,   col_id_set &unpacked_cids, string &temporal_flush){
	int s;

//		Slow flush
  string ret="\n/*\tslow flush\t*/\n";
  string slow_flush_str = fs->get_val_of_def("slow_flush");
  int n_slow_flush = atoi(slow_flush_str.c_str());
  if(n_slow_flush <= 0) n_slow_flush = 2;
  if(n_slow_flush > 1){
	ret += "\tt->flush_ctr++;\n";
	ret += "\tif(t->flush_ctr >= "+int_to_string(n_slow_flush)+"){\n";
	ret += "\t\tt->flush_ctr = 0;\n";
    ret+="\t\tif(t->flush_pos<t->max_aggrs) fta_aggr_flush_old_"+node_name+"(f,1);\n";
	ret += "\t}\n\n";
  }else{
    ret+="\tif(t->flush_pos<t->max_aggrs) fta_aggr_flush_old_"+node_name+"(f,1);\n\n";
  }


	string change_test;
	bool first_one = true;
	int g;
    col_id_set flush_cids;		//	col ids accessed when computing flush variables.
							//  unpack them at temporal flush test time.
    temporal_flush = "";


	for(g=0;g<gb_tbl->size();g++){
		  data_type *gdt = gb_tbl->get_data_type(g);
		  if(gdt->is_temporal()){
		  	  gather_se_col_ids(gb_tbl->get_def(g), flush_cids, gb_tbl);

//					To perform the test, first need to compute the value
//					of the temporal gb attrs.
			  if(gdt->is_buffer_type()){
	//				NOTE : if the SE defining the gb is anything
	//				other than a ref to a variable, this will generate
	//				illegal code.  To be resolved with Spatch.
				sprintf(tmpstr,"\tgb_attr_tmp%d = %s;\n",
					g, generate_se_code(gb_tbl->get_def(g),schema).c_str() );
				temporal_flush += tmpstr;
				sprintf(tmpstr,"\t%s(f, &gb_attr_%d, &gb_attr_tmp%d);\n",
					gdt->get_buffer_assign_copy().c_str(), g, g);
			  }else{
				sprintf(tmpstr,"\tgb_attr_%d = %s;\n",g,generate_se_code(gb_tbl->get_def(g),schema).c_str());
			  }
			  temporal_flush += tmpstr;
//					END computing the value of the temporal GB attr.


			  sprintf(tmpstr,"t->last_gb_%d",g);   string lhs_op = tmpstr;
			  sprintf(tmpstr,"gb_attr_%d",g);   string rhs_op = tmpstr;
			  if(first_one){first_one = false;} else {change_test.append(") && (");}
			  change_test += generate_equality_test(lhs_op, rhs_op, gdt);
		  }
	}
	if(!first_one){		// will be false iff. there is a temporal GB attribute
		  temporal_flush += "\n/*\t\tFlush the aggregates if the temporal gb attrs have changed.\t*/\n";
		  temporal_flush += "\tif( !( (";
		  temporal_flush += change_test;
		  temporal_flush += ") ) ){\n";

//		  temporal_flush+="\t\tif(t->flush_pos<t->max_aggrs) fta_aggr_flush_old_"+node_name+"(f,t->max_aggrs);\n";
		  temporal_flush+="\t\tif(t->flush_pos<t->max_aggrs){ \n";
		  temporal_flush+="\t\t\tfta_aggr_flush_old_"+node_name+"(f,t->max_aggrs);\n";
		  temporal_flush+="\t\t}\n";
		  temporal_flush+="\t\t/* \t\tmark all groups as old */\n";
		  temporal_flush+="\t\tt->generation++;\n";
		  temporal_flush+="\t\tt->flush_pos = 0;\n";


//				Now set the saved temporal value of the gb to the
//				current value of the gb.  Only for simple types,
//				not for buffer types -- but the strings are not
//				temporal in any case.

			for(g=0;g<gb_tbl->size();g++){
			  data_type *gdt = gb_tbl->get_data_type(g);
	 	  	  if(gdt->is_temporal()){
				  if(gdt->is_buffer_type()){

					fprintf(stderr,"ERROR : can't handle temporal buffer types, ignoring in buffer flush control.\n");
				  }else{
					sprintf(tmpstr,"\t\tt->flush_start_gb_%d = gb_attr_%d;\n",g,g);
					temporal_flush += tmpstr;
					sprintf(tmpstr,"\t\tt->last_gb_%d = gb_attr_%d;\n",g,g);
					temporal_flush += tmpstr;
				  }
				}
			}
		  temporal_flush += "\t}\n\n";
	}

// Unpack all the temporal attributes referenced in select clause
// and update the last value of the attribute
	col_id_set temp_cids;		//	col ids of temp attributes in select clause
	col_id_set::iterator csi;

	for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if (sdt->is_temporal()) {
			gather_se_col_ids(sl_list[s],temp_cids, gb_tbl);
		}
	}

	for(csi=temp_cids.begin(); csi != temp_cids.end();++csi){
		if(unpacked_cids.count((*csi)) == 0){
   			int tblref = (*csi).tblvar_ref;
   			int schref = (*csi).schema_ref;
   			string field = (*csi).field;
			ret += generate_unpack_code(tblref,schref,field,schema,node_name);
/*
   			data_type dt(schema->get_type_name(schref,field), schema->get_modifier_list(schref,field));
			sprintf(tmpstr,"\tretval =  %s(p, &unpack_var_%s_%d);\n",
   				schema->get_fcn(schref,field).c_str(), field.c_str(), tblref);
   			ret += tmpstr;
   			ret += "\tif(retval) return 1;\n";
*/
			sprintf(tmpstr,"\tt->last_%s_%d = unpack_var_%s_%d;\n", field.c_str(), tblref, field.c_str(), tblref);
			ret += tmpstr;

			unpacked_cids.insert( (*csi) );
		}
	}


//				Do the flush here if this is a real_time query
	string rt_level = fs->get_val_of_def("real_time");
	if(rt_level != "" && temporal_flush != ""){
		for(csi=flush_cids.begin(); csi != flush_cids.end();++csi){
			if(unpacked_cids.count((*csi)) == 0){
    			int tblref = (*csi).tblvar_ref;
    			int schref = (*csi).schema_ref;
    			string field = (*csi).field;
				ret += generate_unpack_code(tblref,schref,field,schema,node_name);
/*
  				sprintf(tmpstr,"\tretval =  %s(p, &unpack_var_%s_%d);\n",
    				schema->get_fcn(schref,field).c_str(), field.c_str(), tblref);
    			ret += tmpstr;
    			ret += "\tif(retval) return 1;\n";
*/
				unpacked_cids.insert( (*csi) );
			}
		}
		ret += temporal_flush;
	}

	return ret;
 }

string generate_sel_accept_body(qp_node *fs, string node_name, table_list *schema){

int p,s;
string ret;

///////////////			Processing for filter-only query

//			test passed : create the tuple, then assign to it.
	  ret += "/*\t\tCreate and post the tuple\t*/\n";

//			Unpack partial fcns ref'd by the select clause.
//			Its a kind of a WHERE clause ...
  for(p=sl_fcns_start;p<sl_fcns_end;p++){
	if(fcn_ref_cnt[p] > 1){
		ret += "\tif(fcn_ref_cnt_"+int_to_string(p)+"==0){\n";
	}
	if(is_partial_fcn[p]){
		ret += unpack_partial_fcn(partial_fcns[p], p, schema);
		ret += "\tif(retval) goto end;\n";
	}
	if(fcn_ref_cnt[p] > 1){
		if(!is_partial_fcn[p]){
			ret += "\t\tpartial_fcn_result_"+int_to_string(p)+"="+generate_cached_fcn(partial_fcns[p],schema)+";\n";
		}
		ret += "\t\tfcn_ref_cnt_"+int_to_string(p)+"=1;\n";
		ret += "\t}\n";
	}
  }

  // increment the counter of accepted tuples
  ret += "\n\t#ifdef LFTA_STATS\n";
  ret += "\n\tt->accepted_tuple_cnt++;\n\n";
  ret += "\t#endif\n\n";

//			First, compute the size of the tuple.

//			Unpack any BUFFER type selections into temporaries
//			so that I can compute their size and not have
//			to recompute their value during tuple packing.
//			I can use regular assignment here because
//			these temporaries are non-persistent.

	  for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if(sdt->is_buffer_type()){
			sprintf(tmpstr,"\tselvar_%d = ",s);
			ret += tmpstr;
  			ret += generate_se_code(sl_list[s],schema);
  			ret += ";\n";
		}
	  }


//		The size of the tuple is the size of the tuple struct plus the
//		size of the buffers to be copied in.

	  ret+="\ttuple_size = sizeof( struct "+generate_tuple_name(node_name)+")";
	  for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if(sdt->is_buffer_type()){
			sprintf(tmpstr," + %s(&selvar_%d)", sdt->get_buffer_size().c_str(),s);
			ret += tmpstr;
		}
	  }
	  ret += ";\n";


	  ret += "\ttuple = allocate_tuple(f, tuple_size );\n";
	  ret += "\tif( tuple == NULL)\n\t\tgoto end;\n";

//			Test passed, make assignments to the tuple.

	  ret += "\ttuple_pos = sizeof( struct "+generate_tuple_name(node_name)+");\n";

//			Mark tuple as REGULAR_TUPLE
	  ret += "\ttuple->tuple_type = REGULAR_TUPLE;\n";


	  for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if(sdt->is_buffer_type()){
			sprintf(tmpstr,"\t%s(&(tuple->tuple_var%d), &selvar_%d, (char *)tuple, ((char *)tuple)+tuple_pos);\n", sdt->get_buffer_tuple_copy().c_str(),s, s);
			ret += tmpstr;
			sprintf(tmpstr,"\ttuple_pos += %s(&selvar_%d);\n", sdt->get_buffer_size().c_str(), s);
			ret += tmpstr;
		}else{
  			sprintf(tmpstr,"\ttuple->tuple_var%d = ",s);
  			ret += tmpstr;
//  			if(sdt->needs_hn_translation())
//  				ret += sdt->hton_translation() +"( ";
  			ret += generate_se_code(sl_list[s],schema);
//  			if(sdt->needs_hn_translation())
//  				ret += ") ";
  			ret += ";\n";
		}
	  }

//		Generate output.

	  ret += "\tpost_tuple(tuple);\n";

//		Increment the counter of posted tuples
  ret += "\n\t#ifdef LFTA_STATS\n";
  ret += "\tt->out_tuple_cnt++;\n";
  ret+="\tt->out_tuple_sz+=tuple_size;\n";
  ret += "\t#endif\n\n";



	return ret;
}

string generate_fj_accept_body(filter_join_qpn *fs, string node_name,col_id_set &unpacked_cids,ext_fcn_list *Ext_fcns, table_list *schema){

int p,s,w;
string ret;

//			Get parameters
	unsigned int window_len = fs->temporal_range;
	unsigned int n_bloom = 11;
	string n_bloom_str = fs->get_val_of_def("num_bloom");
	int tmp_n_bloom = atoi(n_bloom_str.c_str());
	if(tmp_n_bloom>0)
		n_bloom = tmp_n_bloom+1;
	float bloom_width = (window_len+1.0)/(1.0*n_bloom-1);
	sprintf(tmpstr,"%f",bloom_width);
	string bloom_width_str = tmpstr;

	if(window_len < n_bloom){
		n_bloom = window_len+1;
		bloom_width_str = "1";
	}


//		Grab the current window time
	scalarexp_t winvar(fs->temporal_var);
	ret += "\tcurr_fj_ts = "+generate_se_code(&winvar,schema)+";\n";

	int bf_exp_size = 12;  // base-2 log of number of bits
	string bloom_len_str = fs->get_val_of_def("bloom_size");
	int tmp_bf_exp_size = atoi(bloom_len_str.c_str());
	if(tmp_bf_exp_size > 3 && tmp_bf_exp_size < 32){
		bf_exp_size = tmp_bf_exp_size;
	}
	int bf_bit_size = 1 << bf_exp_size;
	int bf_byte_size = bf_bit_size / (8*sizeof(char));

	unsigned int ht_size = 4096;
	string ht_size_s = fs->get_val_of_def("aggregate_slots");
	int tmp_ht_size = atoi(ht_size_s.c_str());
	if(tmp_ht_size > 1024){
		unsigned int hs = 1;		// make it power of 2
		while(tmp_ht_size){
			hs =hs << 1;
			tmp_ht_size = tmp_ht_size >> 1;
		}
		ht_size = hs;
	}

	int i, bf_mask = 0;
	if(fs->use_bloom){
		for(i=0;i<bf_exp_size;i++)
			bf_mask = (bf_mask << 1) | 1;
	}else{
		for(i=ht_size;i>1;i=i>>1)
			bf_mask = (bf_mask << 1) | 1;
	}

/*
printf("n_bloom=%d, window_len=%d, bloom_width=%s, bf_exp_size=%d, bf_bit_size=%d, bf_byte_size=%d, ht_size=%d, ht_size_s=%s, bf_mask=%d\n",
	n_bloom,
	window_len,
	bloom_width_str.c_str(),
	bf_exp_size,
	bf_bit_size,
	bf_byte_size,
	ht_size,
	ht_size_s.c_str(),
	bf_mask);
*/




//		If this is a bloom-filter fj, first test if the
//		bloom filter needs to be advanced.
//		SET_BF_EMPTY(table,number of bloom filters,bloom filter index,bit index)
//		t->bf_size : number of bits in bloom filter
	if(fs->use_bloom){
		ret +=
"//			Clean out old bloom filters if needed.\n"
"	if(t->first_exec){\n"
"		t->first_exec = 0;\n"
"		t->last_bin = (long long int)(curr_fj_ts/"+bloom_width_str+");\n"
"		t->last_bloom_pos = t->last_bin % "+int_to_string(n_bloom)+";\n"
"	}else{\n"
"		curr_bin = (long long int)(curr_fj_ts/"+bloom_width_str+");\n"
"		if(curr_bin != t->last_bin){\n"
"			for(the_bin=t->last_bin+1;the_bin<=curr_bin;the_bin++){\n"
"				t->last_bloom_pos++;\n"
"				if(t->last_bloom_pos >= "+int_to_string(n_bloom)+")\n"
"					t->last_bloom_pos = 0;\n"
"				tmp_i = t->last_bloom_pos;\n"
"				for(j=0;j<"+int_to_string(bf_bit_size)+";j++){\n"
"					SET_BF_EMPTY(t->bf_table, "+int_to_string(n_bloom)+", tmp_i,j);\n"
"				}\n"
"			}\n"
"		}\n"
"		t->last_bin = curr_bin;\n"
"	}\n"
;
	}


//-----------------------------------------------------------------
//		First, determine whether to do S (filter stream) processing.

	ret +=
"//		S (filtering stream) predicate, should it be processed?\n"
"\n"
;
// Sort S preds based on cost.
	vector<cnf_elem *> s_filt = fs->pred_t1;
	col_id_set::iterator csi;
  if(s_filt.size() > 0){

//			Unpack fields ref'd in the S pred
	for(w=0;w<s_filt.size();++w){
		col_id_set this_pred_cids;
		gather_pr_col_ids(s_filt[w]->pr, this_pred_cids, gb_tbl);
		for(csi=this_pred_cids.begin();csi!=this_pred_cids.end();++csi){
			if(unpacked_cids.count( (*csi) ) == 0){
				int tblref = (*csi).tblvar_ref;
				int schref = (*csi).schema_ref;
				string field = (*csi).field;
				ret += generate_unpack_code(tblref,schref,field,schema,node_name,"end_s");
				unpacked_cids.insert( (*csi) );
			}
		}
	}


//		Sort by evaluation cost.
//		First, estimate evaluation costs
//		Eliminate predicates covered by the prefilter (those in s_pids).
//		I need to do it before the sort becuase the indices refer
//		to the position in the unsorted list.
	vector<cnf_elem *> tmp_wh;
	for(w=0;w<s_filt.size();++w){
		compute_cnf_cost(s_filt[w],Ext_fcns);
		tmp_wh.push_back(s_filt[w]);
	}
	s_filt = tmp_wh;

	sort(s_filt.begin(), s_filt.end(), compare_cnf_cost());

//		Now generate the predicates.
	for(w=0;w<s_filt.size();++w){
		sprintf(tmpstr,"//\t\tPredicate clause %d.(cost %d)\n",w,s_filt[w]->cost);
		ret += tmpstr;

//			Find partial fcns ref'd in this cnf element
		set<int> pfcn_refs;
		collect_partial_fcns_pr(s_filt[w]->pr, pfcn_refs);
//			Since set<..> is a "Sorted Associative Container",
//			we can walk through it in sorted order by walking from
//			begin() to end().  (and the partial fcns must be
//			evaluated in this order).
		set<int>::iterator si;
		string pf_preds;
		for(si=pfcn_refs.begin();si!=pfcn_refs.end();++si){
			if(fcn_ref_cnt[(*si)] > 1){
				ret += "\tif(fcn_ref_cnt_"+int_to_string((*si))+"==0){\n";
			}
			if(is_partial_fcn[(*si)]){
				ret += "\t"+unpack_partial_fcn(partial_fcns[(*si)], (*si), schema);
				ret += "\t\tif(retval) goto end_s;\n";
			}
			if(fcn_ref_cnt[(*si)] > 1){
				if(!is_partial_fcn[(*si)]){
					ret += "\t\tpartial_fcn_result_"+int_to_string((*si))+"="+generate_cached_fcn(partial_fcns[(*si)],schema)+";\n";
//		Testing for S is a side branch.
//		I don't want a cacheable partial function to be
//		marked as evaluated.  Therefore I mark the function
//		as evalauted ONLY IF it is not partial.
					ret += "\t\tfcn_ref_cnt_"+int_to_string((*si))+"=1;\n";
				}
				ret += "\t}\n";
			}
		}

		ret += "\tif( !("+generate_predicate_code(s_filt[w]->pr,schema)+
				") ) goto end_s;\n";
	}
  }else{
	  ret += "\n\n/*\t\t (no predicate to test)\t*/\n\n";
  }

	for(p=0;p<fs->hash_eq.size();++p)
		ret += "\t\ts_equijoin_"+int_to_string(p)+" = "+generate_se_code(fs->hash_eq[p]->pr->get_right_se(),schema)+";\n";

	if(fs->use_bloom){
//			First, generate the S scalar expressions in the hash_eq

//			Iterate over the bloom filters
		for(i=0;i<3;i++){
			ret += "\t\tbucket=0;\n";
			for(p=0;p<fs->hash_eq.size();++p){
				ret +=
"		bucket ^= (("+hash_nums[(i*fs->hash_eq.size()+p)%NRANDS]+" * lfta_"+
	fs->hash_eq[p]->pr->get_right_se()->get_data_type()->get_type_str()+
	+"_to_hash(s_equijoin_"+int_to_string(p)+"))>>32);\n";
			}
//		SET_BF_BIT(table,number of bloom filters,bloom filter index,bit index)
				ret +=
"		bucket &= "+int_to_string(bf_mask)+";\n"
"		SET_BF_BIT(t->bf_table,"+int_to_string(n_bloom)+",t->last_bloom_pos,bucket);\n"
"\n"
;
		}
	}else{
		ret += "\t\tbucket=0;\n";
		for(p=0;p<fs->hash_eq.size();++p){
			ret +=
"		bucket ^= (("+hash_nums[(i*fs->hash_eq.size()+p)%NRANDS]+" * lfta_"+
	fs->hash_eq[p]->pr->get_right_se()->get_data_type()->get_type_str()+
	+"_to_hash(s_equijoin_"+int_to_string(p)+"))>>32);\n";
		}
		ret +=
"		bucket &= "+int_to_string(bf_mask)+";\n"
"		bucket1 = (bucket + 1) & "+int_to_string(bf_mask)+";\n"
;
//			Try the first bucket
		ret += "\t\tif(";
		for(p=0;p<fs->hash_eq.size();++p){
			if(p>0) ret += " && ";
//			ret += "t->join_table[bucket].key_var"+int_to_string(p)+
//					" == s_equijoin_"+int_to_string(p);
			data_type *hdt = fs->hash_eq[p]->pr->get_right_se()->get_data_type();
			string lhs_op = "t->join_table[bucket].key_var"+int_to_string(p);
			string rhs_op = "s_equijoin_"+int_to_string(p);
			ret += generate_equality_test(lhs_op,rhs_op,hdt);
		}
		ret += "){\n\t\t\tthe_bucket = bucket;\n";
		ret += "\t\t}else {if(";
		for(p=0;p<fs->hash_eq.size();++p){
			if(p>0) ret += " && ";
//			ret += "t->join_table[bucket1].key_var"+int_to_string(p)+
//					" == s_equijoin_"+int_to_string(p);
			data_type *hdt = fs->hash_eq[p]->pr->get_right_se()->get_data_type();
			string lhs_op = "t->join_table[bucket].key_var"+int_to_string(p);
			string rhs_op = "s_equijoin_"+int_to_string(p);
			ret += generate_equality_test(lhs_op,rhs_op,hdt);
		}
		ret +=  "){\n\t\t\tthe_bucket = bucket1;\n";
		ret += "\t\t}else{ if(t->join_table[bucket].ts <= t->join_table[bucket1].ts)\n";
		ret+="\t\t\tthe_bucket = bucket;\n\t\t\telse the_bucket=bucket1;\n";
		ret += "\t\t}}\n";
		for(p=0;p<fs->hash_eq.size();++p){
			data_type *hdt = fs->hash_eq[p]->pr->get_right_se()->get_data_type();
			if(hdt->is_buffer_type()){
				sprintf(tmpstr,"\t\t%s(f, &(t->join_table[the_bucket].key_var%d), &s_equijoin_%d);\n", hdt->get_buffer_assign_copy().c_str(), p, p);
				ret += tmpstr;
			}else{
				ret += "\t\tt->join_table[the_bucket].key_var"+int_to_string(p)+
					" = s_equijoin_"+int_to_string(p)+";\n";
			}
		}
		ret+="\t\tt->join_table[the_bucket].ts =  curr_fj_ts;\n";
	}
  ret += "\tend_s:\n";

//	------------------------------------------------------------
//		Next, determine if the R record should be processed.


	ret +=
"//		R (main stream) cheap predicate\n"
"\n"
;

//		Unpack r_filt fields
	vector<cnf_elem *> r_filt = fs->pred_t0;
	for(w=0;w<r_filt.size();++w){
		col_id_set this_pred_cids;
		gather_pr_col_ids(r_filt[w]->pr, this_pred_cids, gb_tbl);
		for(csi=this_pred_cids.begin();csi!=this_pred_cids.end();++csi){
			if(unpacked_cids.count( (*csi) ) == 0){
				int tblref = (*csi).tblvar_ref;
				int schref = (*csi).schema_ref;
				string field = (*csi).field;
				ret += generate_unpack_code(tblref,schref,field,schema,node_name);
				unpacked_cids.insert( (*csi) );
			}
		}
	}

// Sort S preds based on cost.

	vector<cnf_elem *> tmp_wh;
	for(w=0;w<r_filt.size();++w){
		compute_cnf_cost(r_filt[w],Ext_fcns);
		tmp_wh.push_back(r_filt[w]);
	}
	r_filt = tmp_wh;

	sort(r_filt.begin(), r_filt.end(), compare_cnf_cost());

//		WARNING! the constant 20 below is a wild-ass guess.
	int cheap_rpos;
	for(cheap_rpos=0;cheap_rpos<r_filt.size() && r_filt[cheap_rpos]->cost <= 20;cheap_rpos++)

//		Test the cheap filters on R.
  if(cheap_rpos >0){

//		Now generate the predicates.
	for(w=0;w<cheap_rpos;++w){
		sprintf(tmpstr,"//\t\tPredicate clause %d.(cost %d)\n",w,r_filt[w]->cost);
		ret += tmpstr;

//			Find partial fcns ref'd in this cnf element
		set<int> pfcn_refs;
		collect_partial_fcns_pr(r_filt[w]->pr, pfcn_refs);
//			Since set<..> is a "Sorted Associative Container",
//			we can walk through it in sorted order by walking from
//			begin() to end().  (and the partial fcns must be
//			evaluated in this order).
		set<int>::iterator si;
		for(si=pfcn_refs.begin();si!=pfcn_refs.end();++si){
			if(fcn_ref_cnt[(*si)] > 1){
				ret += "\tif(fcn_ref_cnt_"+int_to_string((*si))+"==0){\n";
			}
			if(is_partial_fcn[(*si)]){
				ret += "\t"+unpack_partial_fcn(partial_fcns[(*si)], (*si), schema);
				ret += "\t\tif(retval) goto end;\n";
			}
			if(fcn_ref_cnt[(*si)] > 1){
				if(!is_partial_fcn[(*si)]){
					ret += "\t\tpartial_fcn_result_"+int_to_string((*si))+"="+generate_cached_fcn(partial_fcns[(*si)],schema)+";\n";
				}
				ret += "\t\tfcn_ref_cnt_"+int_to_string((*si))+"=1;\n";
				ret += "\t}\n";
			}
		}

		ret += "\tif( !("+generate_predicate_code(r_filt[w]->pr,schema)+
				") ) goto end;\n";
	}
  }else{
	  ret += "\n\n/*\t\t (no predicate to test)\t*/\n\n";
  }

	ret += "\n//	Do the join\n\n";
	for(p=0;p<fs->hash_eq.size();++p)
		ret += "\t\tr_equijoin_"+int_to_string(p)+" = "+generate_se_code(fs->hash_eq[p]->pr->get_left_se(),schema)+";\n";


//			Passed the cheap pred, now test the join with S.
	if(fs->use_bloom){
		for(i=0;i<3;i++){
			ret += "\t\tbucket"+int_to_string(i)+"=0;\n";
			for(p=0;p<fs->hash_eq.size();++p){
				ret +=
"	bucket"+int_to_string(i)+
	" ^= (("+hash_nums[(i*fs->hash_eq.size()+p)%NRANDS]+" * lfta_"+
	fs->hash_eq[p]->pr->get_right_se()->get_data_type()->get_type_str()+
	+"_to_hash(r_equijoin_"+int_to_string(p)+"))>>32);\n";
			}
				ret +=
"	bucket"+int_to_string(i)+" &= "+int_to_string(bf_mask)+";\n";
		}
		ret += "\tfound = 0;\n";
		ret += "\tfor(b=0;b<"+int_to_string(n_bloom)+" && !found; b++){\n";
		ret +=
"\t\tif(IS_BF_SET(t->bf_table,"+int_to_string(n_bloom)+",t->last_bloom_pos,bucket0) && "
"IS_BF_SET(t->bf_table,"+int_to_string(n_bloom)+",t->last_bloom_pos,bucket1) && "
"IS_BF_SET(t->bf_table,"+int_to_string(n_bloom)+",t->last_bloom_pos,bucket2))\n "
"\t\t\tfound=1;\n"
"\t}\n"
;
		ret +=
"	if(!found)\n"
"		goto end;\n"
;
	}else{
		ret += "\tfound = 0;\n";
		ret += "\t\tbucket=0;\n";
		for(p=0;p<fs->hash_eq.size();++p){
			ret +=
"		bucket ^= (("+hash_nums[(i*fs->hash_eq.size()+p)%NRANDS]+" * lfta_"+
	fs->hash_eq[p]->pr->get_right_se()->get_data_type()->get_type_str()+
	+"_to_hash(r_equijoin_"+int_to_string(p)+"))>>32);\n";
		}
		ret +=
"		bucket &= "+int_to_string(bf_mask)+";\n"
"		bucket1 = (bucket + 1) & "+int_to_string(bf_mask)+";\n"
;
//			Try the first bucket
		ret += "\t\tif(";
		for(p=0;p<fs->hash_eq.size();++p){
			if(p>0) ret += " && ";
//			ret += "t->join_table[bucket].key_var"+int_to_string(p)+
//					" == r_equijoin_"+int_to_string(p);
			data_type *hdt = fs->hash_eq[p]->pr->get_right_se()->get_data_type();
			string lhs_op = "t->join_table[bucket].key_var"+int_to_string(p);
			string rhs_op = "s_equijoin_"+int_to_string(p);
			ret += generate_equality_test(lhs_op,rhs_op,hdt);
		}
		if(p>0) ret += " && ";
		ret += "t->join_table[bucket].ts+"+int_to_string(fs->temporal_range)+" <=  curr_fj_ts";
		ret += "){\n\t\t\tfound = 1;\n";
		ret += "\t\t}else {if(";
		for(p=0;p<fs->hash_eq.size();++p){
			if(p>0) ret += " && ";
//			ret += "t->join_table[bucket1].key_var"+int_to_string(p)+
//					" == r_equijoin_"+int_to_string(p);
			data_type *hdt = fs->hash_eq[p]->pr->get_right_se()->get_data_type();
			string lhs_op = "t->join_table[bucket].key_var"+int_to_string(p);
			string rhs_op = "s_equijoin_"+int_to_string(p);
			ret += generate_equality_test(lhs_op,rhs_op,hdt);
		}
		if(p>0) ret += " && ";
		ret += "t->join_table[bucket1].ts+"+int_to_string(fs->temporal_range)+" <=  curr_fj_ts";
		ret +=  ")\n\t\t\tfound=1;\n";
		ret+="\t\t}\n";
		ret +=
"	if(!found)\n"
"		goto end;\n"
;
	}


//		Test the expensive filters on R.
  if(cheap_rpos < r_filt.size()){

//		Now generate the predicates.
	for(w=cheap_rpos;w<r_filt.size();++w){
		sprintf(tmpstr,"//\t\tPredicate clause %d.(cost %d)\n",w,r_filt[w]->cost);
		ret += tmpstr;

//			Find partial fcns ref'd in this cnf element
		set<int> pfcn_refs;
		collect_partial_fcns_pr(r_filt[w]->pr, pfcn_refs);
//			Since set<..> is a "Sorted Associative Container",
//			we can walk through it in sorted order by walking from
//			begin() to end().  (and the partial fcns must be
//			evaluated in this order).
		set<int>::iterator si;
		for(si=pfcn_refs.begin();si!=pfcn_refs.end();++si){
			if(fcn_ref_cnt[(*si)] > 1){
				ret += "\tif(fcn_ref_cnt_"+int_to_string((*si))+"==0){\n";
			}
			if(is_partial_fcn[(*si)]){
				ret += "\t"+unpack_partial_fcn(partial_fcns[(*si)], (*si), schema);
				ret += "\t\tif(retval) goto end;\n";
			}
			if(fcn_ref_cnt[(*si)] > 1){
				if(!is_partial_fcn[(*si)]){
					ret += "\t\tpartial_fcn_result_"+int_to_string((*si))+"="+generate_cached_fcn(partial_fcns[(*si)],schema)+";\n";
				}
				ret += "\t\tfcn_ref_cnt_"+int_to_string((*si))+"=1;\n";
				ret += "\t}\n";
			}
		}

		ret += "\tif( !("+generate_predicate_code(r_filt[w]->pr,schema)+
				") ) goto end;\n";
	}
  }else{
	  ret += "\n\n/*\t\t (no predicate to test)\t*/\n\n";
  }



///////////////			post the tuple

//			test passed : create the tuple, then assign to it.
	  ret += "/*\t\tCreate and post the tuple\t*/\n";

//		Unpack r_filt fields
	for(s=0;s<sl_list.size();++s){
		col_id_set this_se_cids;
		gather_se_col_ids(sl_list[s], this_se_cids, gb_tbl);
		for(csi=this_se_cids.begin();csi!=this_se_cids.end();++csi){
			if(unpacked_cids.count( (*csi) ) == 0){
				int tblref = (*csi).tblvar_ref;
				int schref = (*csi).schema_ref;
				string field = (*csi).field;
				ret += generate_unpack_code(tblref,schref,field,schema,node_name);
				unpacked_cids.insert( (*csi) );
			}
		}
	}


//			Unpack partial fcns ref'd by the select clause.
//			Its a kind of a WHERE clause ...
  for(p=sl_fcns_start;p<sl_fcns_end;p++){
	if(fcn_ref_cnt[p] > 1){
		ret += "\tif(fcn_ref_cnt_"+int_to_string(p)+"==0){\n";
	}
	if(is_partial_fcn[p]){
		ret += unpack_partial_fcn(partial_fcns[p], p, schema);
		ret += "\tif(retval) goto end;\n";
	}
	if(fcn_ref_cnt[p] > 1){
		if(!is_partial_fcn[p]){
			ret += "\t\tpartial_fcn_result_"+int_to_string(p)+"="+generate_cached_fcn(partial_fcns[p],schema)+";\n";
		}
		ret += "\t\tfcn_ref_cnt_"+int_to_string(p)+"=1;\n";
		ret += "\t}\n";
	}
  }

  // increment the counter of accepted tuples
  ret += "\n\t#ifdef LFTA_STATS\n";
  ret += "\n\tt->accepted_tuple_cnt++;\n\n";
  ret += "\t#endif\n\n";

//			First, compute the size of the tuple.

//			Unpack any BUFFER type selections into temporaries
//			so that I can compute their size and not have
//			to recompute their value during tuple packing.
//			I can use regular assignment here because
//			these temporaries are non-persistent.

	  for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if(sdt->is_buffer_type()){
			sprintf(tmpstr,"\tselvar_%d = ",s);
			ret += tmpstr;
  			ret += generate_se_code(sl_list[s],schema);
  			ret += ";\n";
		}
	  }


//		The size of the tuple is the size of the tuple struct plus the
//		size of the buffers to be copied in.

	  ret+="\ttuple_size = sizeof( struct "+generate_tuple_name(node_name)+")";
	  for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if(sdt->is_buffer_type()){
			sprintf(tmpstr," + %s(&selvar_%d)", sdt->get_buffer_size().c_str(),s);
			ret += tmpstr;
		}
	  }
	  ret += ";\n";


	  ret += "\ttuple = allocate_tuple(f, tuple_size );\n";
	  ret += "\tif( tuple == NULL)\n\t\tgoto end;\n";

//			Test passed, make assignments to the tuple.

	  ret += "\ttuple_pos = sizeof( struct "+generate_tuple_name(node_name)+");\n";

//			Mark tuple as REGULAR_TUPLE
	  ret += "\ttuple->tuple_type = REGULAR_TUPLE;\n";


	  for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if(sdt->is_buffer_type()){
			sprintf(tmpstr,"\t%s(&(tuple->tuple_var%d), &selvar_%d, (char *)tuple, ((char *)tuple)+tuple_pos);\n", sdt->get_buffer_tuple_copy().c_str(),s, s);
			ret += tmpstr;
			sprintf(tmpstr,"\ttuple_pos += %s(&selvar_%d);\n", sdt->get_buffer_size().c_str(), s);
			ret += tmpstr;
		}else{
  			sprintf(tmpstr,"\ttuple->tuple_var%d = ",s);
  			ret += tmpstr;
//  			if(sdt->needs_hn_translation())
//  				ret += sdt->hton_translation() +"( ";
  			ret += generate_se_code(sl_list[s],schema);
//  			if(sdt->needs_hn_translation())
//  				ret += ") ";
  			ret += ";\n";
		}
	  }

//		Generate output.

	  ret += "\tpost_tuple(tuple);\n";

//		Increment the counter of posted tuples
  ret += "\n\t#ifdef LFTA_STATS\n";
  ret += "\n\tt->out_tuple_cnt++;\n\n";
  ret+="\t\t\t\tt->out_tuple_sz+=tuple_size;\n";
  ret += "\t#endif\n\n";


	return ret;
}

string generate_aggr_accept_body(qp_node *fs,string node_name,table_list *schema, string &temporal_flush){
	string ret;
	int a,p,g;

//////////////		Processing for aggregtion query

//		First, search for a match.  Start by unpacking the group-by attributes.

//			One complication : if a real-time aggregate flush occurs,
//			the GB attr has already been calculated.  So don't compute
//			it again if 1) its temporal and 2) it will be computed in the
//			agggregate flush code.

//		Unpack the partial fcns ref'd by the gb's and the aggr defs.
  for(p=gb_fcns_start;p<gb_fcns_end;p++){
    if(is_partial_fcn[p]){
		ret += unpack_partial_fcn(partial_fcns[p], p, schema);
		ret += "\tif(retval) goto end;\n";
	}
  }
  for(p=ag_fcns_start;p<ag_fcns_end;p++){
    if(is_partial_fcn[p]){
		ret += unpack_partial_fcn(partial_fcns[p], p, schema);
		ret += "\tif(retval) goto end;\n";
	}
  }

  // increment the counter of accepted tuples
  ret += "\n\t#ifdef LFTA_STATS\n";
  ret += "\n\tt->accepted_tuple_cnt++;\n\n";
  ret += "\t#endif\n\n";

  ret += "/*\t\tTest if the group is in the hash table \t*/\n";
//			Compute the values of the group-by variables.
  for(g=0;g<gb_tbl->size();g++){
	  data_type *gdt = gb_tbl->get_data_type(g);
	  if((! gdt->is_temporal()) || temporal_flush == ""){

		  if(gdt->is_buffer_type()){
	//				NOTE : if the SE defining the gb is anything
	//				other than a ref to a variable, this will generate
	//				illegal code.  To be resolved with Spatch.
			sprintf(tmpstr,"\tgb_attr_tmp%d = %s;\n",
				g, generate_se_code(gb_tbl->get_def(g),schema).c_str() );
			ret += tmpstr;
			sprintf(tmpstr,"\t%s(f, &gb_attr_%d, &gb_attr_tmp%d);\n",
				gdt->get_buffer_assign_copy().c_str(), g, g);
		  }else{
			sprintf(tmpstr,"\tgb_attr_%d = %s;\n",g,generate_se_code(gb_tbl->get_def(g),schema).c_str());
		  }
		  ret += tmpstr;
	  }
  }
  ret += "\n";

//			A quick aside : if any of the GB attrs are temporal,
//			test for change and flush if any change occurred.
//			We've already computed the flush code,
//			Put it here if this is not a real time query.
//			We've already unpacked all column refs, so no need to
//			do it again here.

	string rt_level = fs->get_val_of_def("real_time");
	if(rt_level == "" && temporal_flush != ""){
		ret += temporal_flush;
	}

//			Compute the hash bucket
	if(gb_tbl->size() > 0){
		ret += "\thashval = ";\
		for(g=0;g<gb_tbl->size();g++){
		  if(g>0) ret += " ^ ";
	  	  data_type *gdt = gb_tbl->get_data_type(g);
		  if(gdt->is_buffer_type()){
			sprintf(tmpstr,"((%s * lfta_%s_to_hash(gb_attr_%d)))",hash_nums[g%NRANDS].c_str(),
				gdt->get_type_str().c_str(), g);
		  }else{
			sprintf(tmpstr,"((%s * lfta_%s_to_hash(gb_attr_%d)))",hash_nums[g%NRANDS].c_str(),
				gdt->get_type_str().c_str(), g);
		  }
		  ret += tmpstr;
		}
		ret += ";\n";
		ret += "\thash2 = ((hashval * "+hash_nums[g%NRANDS]+") >> 32) & SLOT_HASH_BITS;\n";
        ret+="\tprobe = (hashval >> 32) & (t->max_aggrs-1);\n";
	}else{
		ret+="\tprobe = 0;\n";
		ret+="\thash2 = 0;\n\n";
	}

//		Does the lfta reference a udaf?
	  bool has_udaf = false;
	  for(a=0;a<aggr_tbl->size();a++){
		  if(! aggr_tbl->is_builtin(a)) has_udaf = true;
	  }

//		Scan for a match, or alternatively the best slot.
//		Currently, hardcode 5 tests.
	ret +=
"	gen_val = t->generation & SLOT_GEN_BITS;\n"
"	match_found = 0;\n"
"	best_slot = probe;\n"
"	for(i=0;i<5 && match_found == 0;i++){\n"
"		if((t->aggr_table_hashmap[probe] & SLOT_FILLED) && (t->aggr_table_hashmap[probe] & SLOT_GEN_BITS) == gen_val && (t->aggr_table_hashmap[probe] & SLOT_HASH_BITS) == hash2 ){\n"
;
	if(gb_tbl->size()>0){
		ret+="\n\t/* \t\tcheck if the grouping variables are equal */\n";
		ret+="\t\tif(";
	  	string rhs_op, lhs_op;
	  	for(g=0;g<gb_tbl->size();g++){
		  if(g>0) ret += " && ";
		  ret += "(";
		  sprintf(tmpstr,"gb_attr_%d",g); lhs_op = tmpstr;
		  sprintf(tmpstr,"t->aggr_table[probe].gb_var%d",g); rhs_op = tmpstr;
		  ret += generate_equality_test(lhs_op,rhs_op,gb_tbl->get_data_type(g));
		  ret += ")";
	  	}
	 }
	 ret += "){\n"
"			match_found = 1;\n"
"			best_slot = probe;\n"
"		}\n"
"	}\n"
"//		Rate slots in case no match found: prefer empty, then full but old slots\n"
"	if(t->aggr_table_hashmap[best_slot] & SLOT_FILLED){\n"
"		if((t->aggr_table_hashmap[probe] & SLOT_FILLED)==0)\n"
"			best_slot = probe;\n"
"		}else{\n"
"			if((t->aggr_table_hashmap[best_slot] & SLOT_GEN_BITS) == gen_val && (t->aggr_table_hashmap[probe] & SLOT_GEN_BITS) != gen_val){\n"
"				best_slot = probe;\n"
"			}\n"
"		}\n"
"		probe++;\n"
"		if(probe >= t->max_aggrs)\n"
"			probe=0;\n"
"	}\n"
"	if(match_found){\n"
;
	ret += generate_gb_update(node_name, schema, "best_slot",has_udaf);
	ret +=
"	}else{\n"
"		if(t->aggr_table_hashmap[best_slot] & SLOT_FILLED){\n"
;
printf("sgah_qpn name is %s, disorder is %d\n",fs->node_name.c_str(),((sgah_qpn *)fs)->lfta_disorder);
	if(((sgah_qpn *)fs)->lfta_disorder <= 1){
		ret +=
"			if((t->aggr_table_hashmap[best_slot] & SLOT_GEN_BITS)==gen_val){\n"
"				if((";
		bool first_g = true;
		for(int g=0;g<gb_tbl->size();g++){
			data_type *gdt = gb_tbl->get_data_type(g);
			if(gdt->is_temporal()){
				if(first_g) first_g = false; else ret+=" + ";
				ret += "(gb_attr_"+int_to_string(g)+" - t->aggr_table[best_slot].gb_var"+int_to_string(g)+")";
			}
		}
		ret += ") == 0 ){\n";

		ret +=
"					fta_aggr_flush_old_"+ node_name+"(f,t->max_aggrs);\n"
"				}\n"
"			}\n"
;
	}

	ret += generate_tuple_from_aggr(node_name,schema,"best_slot");
  	ret +=
"\t\t\t#ifdef LFTA_STATS\n"
"\t\t\tif((t->aggr_table_hashmap[best_slot] & SLOT_GEN_BITS) == gen_val)\n"
"\t\t\t\tt->collision_cnt++;\n\n"
"\t\t\t#endif\n\n"
"\t\t}\n"
;
	ret += generate_init_group(schema,"best_slot");


	  ret += "\t}\n";

	return ret;
}



string generate_fta_accept(qp_node *fs, string node_name, table_list *schema, ext_fcn_list *Ext_fcns, bool is_aggr_query, bool is_fj, set<unsigned int> &s_pids){

	string ret="static gs_retval_t accept_packet_"+node_name+
		"(struct FTA *f, FTAID * ftaid, void *pkt, gs_int32_t sz){\n";
    ret += "\tstruct packet *p = (struct packet *)pkt;\n";

  int a;

//			Define all of the variables needed by this
//			procedure.


//			Gather all column references, need to define unpacking variables.
  int w,s;
  col_id_set cid_set;
  col_id_set::iterator csi;

//		If its a filter join, rebind all colrefs
//		to the first range var, to avoid double unpacking.

  if(is_fj){
    for(w=0;w<where.size();++w)
		reset_pr_col_ids_tblvars(where[w]->pr, gb_tbl);
    for(s=0;s<sl_list.size();s++)
		reset_se_col_ids_tblvars(sl_list[s], gb_tbl);
  }

  for(w=0;w<where.size();++w){
	if(is_fj || s_pids.count(w) == 0)
  		gather_pr_col_ids(where[w]->pr,cid_set, gb_tbl);
	}
  for(s=0;s<sl_list.size();s++){
  	gather_se_col_ids(sl_list[s],cid_set, gb_tbl);
  }

  int g;
  if(gb_tbl != NULL){
  	for(g=0;g<gb_tbl->size();g++)
	  gather_se_col_ids(gb_tbl->get_def(g),cid_set, gb_tbl);
  }

  //			Variables for unpacking attributes.
  ret += "/*\t\tVariables for unpacking attributes\t*/\n";
  for(csi=cid_set.begin(); csi!=cid_set.end();++csi){
    int schref = (*csi).schema_ref;
	int tblref = (*csi).tblvar_ref;
    string field = (*csi).field;
    data_type dt(schema->get_type_name(schref,field));
    sprintf(tmpstr,"\t%s unpack_var_%s_%d;\n",dt.get_cvar_type().c_str(),
    	field.c_str(), tblref);
    ret += tmpstr;
  }

  ret += "\n\n";

//			Variables that are always needed
  ret += "/*\t\tVariables which are always needed\t*/\n";
  ret += "\tgs_retval_t retval;\n";
  ret += "\tgs_int32_t tuple_size, tuple_pos, lfta_bailout;\n";
  ret += "\tstruct "+generate_tuple_name(node_name)+" *tuple;\n";

  ret+="\tstruct "+generate_fta_name(node_name)+" *t = (struct "+generate_fta_name(node_name)+"*) f;\n\n";


//			Variables needed for aggregation queries.
  if(is_aggr_query){
	  ret += "\n/*\t\tVariables for aggregation\t*/\n";
	  ret+="\tunsigned int i, probe;\n";
	  ret+="\tunsigned int gen_val, match_found, best_slot;\n";
	  ret+="\tgs_uint64_t hashval, hash2;\n";
//			Variables for storing group-by attribute values.
	  if(gb_tbl->size() > 0)
	  	ret += "/*\t\tGroup-by attributes\t*/\n";
	  for(g=0;g<gb_tbl->size();g++){
		sprintf(tmpstr,"\t%s gb_attr_%d;\n",gb_tbl->get_data_type(g)->get_cvar_type().c_str(),g);
		ret += tmpstr;
		data_type *gdt = gb_tbl->get_data_type(g);
		if(gdt->is_buffer_type()){
		  sprintf(tmpstr,"\t%s gb_attr_tmp%d;\n",gb_tbl->get_data_type(g)->get_cvar_type().c_str(),g);
		  ret += tmpstr;
		}
	  }
	  ret += "\n";
//			Temporaries for min/max
 	  string aggr_tmp_str = "";
	  for(a=0;a<aggr_tbl->size();a++){
		string aggr_op = aggr_tbl->get_op(a);
		if(aggr_op == "MIN" || aggr_op == "MAX"){
			sprintf(tmpstr,"\t%s aggr_tmp_%d;\n",aggr_tbl->get_data_type(a)->get_cvar_type().c_str(),a);
			aggr_tmp_str.append(tmpstr);
		}
	  }
	  if(aggr_tmp_str != ""){
		ret += "/*\t\tTemp vars for BUFFER aggregates\t*/\n";
		ret += aggr_tmp_str;
		ret += "\n";
	  }
//		Variables for udaf output temporaries
	bool no_udaf = true;
   	for(a=0;a<aggr_tbl->size();a++){
		if(! aggr_tbl->is_builtin(a)){
			if(no_udaf){
				ret+="/*\t\tUDAF output vars.\t*/\n";
				no_udaf = false;
			}
			int afcn_id = aggr_tbl->get_fcn_id(a);
			data_type *adt = Ext_fcns->get_fcn_dt(afcn_id);
			sprintf(tmpstr,"udaf_ret%d", a);
			ret+="\t"+adt->make_cvar(tmpstr)+";\n";
		}
	}
  }

//			Variables needed for a filter join query
  if(fs->node_type() == "filter_join"){
	filter_join_qpn *fjq = (filter_join_qpn *)fs;
	bool uses_bloom = fjq->use_bloom;
	ret += "/*\t\tJoin fields\t*/\n";
	for(g=0;g<fjq->hash_eq.size();g++){
		sprintf(tmpstr,"\t%s s_equijoin_%d, r_equijoin_%d;\n",fjq->hash_eq[g]->pr->get_left_se()->get_data_type()->get_cvar_type().c_str(),g,g);
		ret += tmpstr;
	  }
	if(uses_bloom){
		ret +=
"  /*		Variables for fj bloom filter	*/ \n"
"\tunsigned int i=0,j=0,k=0, b, bf_clean = 0, tmp_i, found; \n"
"\tunsigned int bucket, bucket0, bucket1, bucket2;\n"
"\tlong long int curr_fj_ts;\n"
"\tunsigned int curr_bin, the_bin;\n"
"\n"
;
	}else{
		ret +=
"  /*		Variables for fj join table	*/ \n"
"\tunsigned int i, bucket, found; \n"
"\tunsigned int bucket1, the_bucket;\n"
"	long long int curr_fj_ts;\n"
"\n"
;
	}
  }


//		Variables needed to store selected attributes of BUFFER type
//		temporarily, in order to compute their size for storage
//		in an output tuple.

  string select_var_defs = "";
  for(s=0;s<sl_list.size();s++){
	data_type *sdt = sl_list[s]->get_data_type();
	if(sdt->is_buffer_type()){
	  sprintf(tmpstr,"\t%s selvar_%d;\n",sdt->get_cvar_type().c_str(),s);
	  select_var_defs.append(tmpstr);
	}
  }
  if(select_var_defs != ""){
	ret += "/*\t\tTemporaries for computing buffer sizes.\t*/\n";
    ret += select_var_defs;
  }

//		Variables to store results of partial functions.
  int p;
  if(partial_fcns.size()>0){
	  ret += "/*\t\tVariables for storing results of partial functions. \t*/\n";
	  for(p=0;p<partial_fcns.size();++p){
		if(is_partial_fcn[p] || (!is_aggr_query && fcn_ref_cnt[p] >1)){
		  sprintf(tmpstr,"\t%s partial_fcn_result_%d;\n",
		    partial_fcns[p]->get_data_type()->get_cvar_type().c_str(), p);
		  ret += tmpstr;
		  if(!is_aggr_query && fcn_ref_cnt[p] >1){
			ret += "\tint fcn_ref_cnt_"+int_to_string(p)+" = 0;\n";
		  }
		}
	  }

	  if(is_aggr_query) ret += "\tint unpack_failed = 0;\n";
	  ret += "\n";
  }

//		variable to hold packet struct	//
	if(packed_return){
		ret += "\t struct "+node_name+"_input_struct *"+node_name+"_input_struct_var;\n";
	}


  ret += "\t#ifdef LFTA_STATS\n";
// variable to store counter of cpu cycles spend in accept_tuple
	ret += "\tgs_uint64_t start_cycle = rdtsc();\n";
// increment counter of received tuples
	ret += "\tt->in_tuple_cnt++;\n";
  ret += "\t#endif\n";


//	-------------------------------------------------
//		If the packet is "packet", test if its for this lfta,
//		and if so load it into its struct

	if(packed_return){
		ret+="\n/*  packed tuple : test and load. \t*/\n";
		ret+="\t"+node_name+"_input_struct_var = (struct "+node_name+"_input_struct *) pkt;\n";
		ret+="\tif("+node_name+"_input_struct_var->__lfta_id_fm_nic__ != "+int_to_string(global_id) + ")\n";
		ret+="\t\tgoto end;\n\n";
	}



  col_id_set unpacked_cids;	//	Keep track of the cols that have been unpacked.

  string temporal_flush;
  if(is_aggr_query)
	ret += generate_aggr_accept_prelim(fs, node_name, schema, unpacked_cids, temporal_flush);
  else {	// non-aggregation operators

// Unpack all the temporal attributes referenced in select clause
// and update the last value of the attribute
	col_id_set temp_cids;		//	col ids of temp attributes in select clause

	for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if (sdt->is_temporal()) {
			gather_se_col_ids(sl_list[s],temp_cids, gb_tbl);
		}
	}
//			If this is a filter join,
//			ensure that the temporal range field is unpacked.
	if(is_fj){
		col_id window_var_cid(((filter_join_qpn *)fs)->temporal_var);
		if(temp_cids.count(window_var_cid)==0)
			temp_cids.insert(window_var_cid);
	}

	for(csi=temp_cids.begin(); csi != temp_cids.end();++csi){
		if(unpacked_cids.count((*csi)) == 0){
   			int tblref = (*csi).tblvar_ref;
   			int schref = (*csi).schema_ref;
   			string field = (*csi).field;
			ret += generate_unpack_code(tblref,schref,field,schema,node_name);
			sprintf(tmpstr,"\tt->last_%s_%d = unpack_var_%s_%d;\n", field.c_str(), tblref, field.c_str(), tblref);
			ret += tmpstr;

			unpacked_cids.insert( (*csi) );
		}
	}

  }

  vector<cnf_elem *> filter = fs->get_filter_clause();
//		Test the filter predicate (some query types have additional preds).
  if(filter.size() > 0){

//		Sort by evaluation cost.
//		First, estimate evaluation costs
//		Eliminate predicates covered by the prefilter (those in s_pids).
//		I need to do it before the sort becuase the indices refer
//		to the position in the unsorted list./
	vector<cnf_elem *> tmp_wh;
	for(w=0;w<filter.size();++w){
		if(s_pids.count(w) == 0){
			compute_cnf_cost(filter[w],Ext_fcns);
			tmp_wh.push_back(filter[w]);
		}
	}
	filter = tmp_wh;

	sort(filter.begin(), filter.end(), compare_cnf_cost());

//		Now generate the predicates.
	for(w=0;w<filter.size();++w){
		sprintf(tmpstr,"//\t\tPredicate clause %d.(cost %d)\n",w,filter[w]->cost);
		ret += tmpstr;
//			Find the set of	variables accessed in this CNF elem,
//			but in no previous element.
		col_id_set this_pred_cids;
		gather_pr_col_ids(filter[w]->pr, this_pred_cids, gb_tbl);
		for(csi=this_pred_cids.begin();csi!=this_pred_cids.end();++csi){
			if(unpacked_cids.count( (*csi) ) == 0){
    			int tblref = (*csi).tblvar_ref;
    			int schref = (*csi).schema_ref;
    			string field = (*csi).field;
				ret += generate_unpack_code(tblref,schref,field,schema,node_name);
				unpacked_cids.insert( (*csi) );
			}
		}
//			Find partial fcns ref'd in this cnf element
		set<int> pfcn_refs;
		collect_partial_fcns_pr(filter[w]->pr, pfcn_refs);
//			Since set<..> is a "Sorted Associative Container",
//			we can walk through it in sorted order by walking from
//			begin() to end().  (and the partial fcns must be
//			evaluated in this order).
		set<int>::iterator si;
		for(si=pfcn_refs.begin();si!=pfcn_refs.end();++si){
			if(fcn_ref_cnt[(*si)] > 1){
				ret += "\tif(fcn_ref_cnt_"+int_to_string((*si))+"==0){\n";
			}
			if(is_partial_fcn[(*si)]){
				ret += "\t"+unpack_partial_fcn(partial_fcns[(*si)], (*si), schema);
				ret += "\t\tif(retval) goto end;\n";
			}
			if(fcn_ref_cnt[(*si)] > 1){
				if(!is_partial_fcn[(*si)]){
					ret += "\t\tpartial_fcn_result_"+int_to_string((*si))+"="+generate_cached_fcn(partial_fcns[(*si)],schema)+";\n";
				}
				ret += "\t\tfcn_ref_cnt_"+int_to_string((*si))+"=1;\n";
				ret += "\t}\n";
			}
		}

		ret += "\tif( !("+generate_predicate_code(filter[w]->pr,schema)+
				") ) goto end;\n";
	}
  }else{
	  ret += "\n\n/*\t\t (no predicate to test)\t*/\n\n";
  }


//			We've passed the WHERE clause,
//			unpack the remainder of the accessed fields.
  if(is_fj){
	ret += "\n/*\tPassed the WHERE clause, unpack the hash fields. */\n";
	vector<cnf_elem *> h_eq = ((filter_join_qpn *)fs)-> hash_eq;
		for(w=0;w<h_eq.size();++w){
		col_id_set this_pred_cids;
		gather_pr_col_ids(h_eq[w]->pr, this_pred_cids, gb_tbl);
		for(csi=this_pred_cids.begin();csi!=this_pred_cids.end();++csi){
			if(unpacked_cids.count( (*csi) ) == 0){
				int tblref = (*csi).tblvar_ref;
				int schref = (*csi).schema_ref;
				string field = (*csi).field;
				ret += generate_unpack_code(tblref,schref,field,schema,node_name);
				unpacked_cids.insert( (*csi) );
			}
		}
	}
  }else{
	  ret += "\n/*\tPassed the WHERE clause, unpack the rest of the accessed fields. */\n";

	  for(csi=cid_set.begin();csi!=cid_set.end();++csi){
		if(unpacked_cids.count( (*csi) ) == 0){
			int schref = (*csi).schema_ref;
			int tblref = (*csi).tblvar_ref;
			string field = (*csi).field;
			ret += generate_unpack_code(tblref,schref,field,schema,node_name);
			unpacked_cids.insert( (*csi) );
		}
	  }
  }


//////////////////
//////////////////	After this, the query types
//////////////////	are processed differently.

  if(!is_aggr_query && !is_fj)
	ret += generate_sel_accept_body(fs, node_name, schema);
  else if(is_aggr_query)
	ret += generate_aggr_accept_body(fs, node_name, schema, temporal_flush);
  else
	ret += generate_fj_accept_body((filter_join_qpn *)fs, node_name, unpacked_cids, Ext_fcns, schema);


//		Finish up.

   ret += "\n\tend:\n";
  ret += "\t#ifdef LFTA_STATS\n";
	ret+= "\tt->cycle_cnt += rdtsc() - start_cycle;\n";
  ret += "\t#endif\n";
   ret += "\n\treturn 1;\n}\n\n";

	return(ret);
}


string generate_fta_alloc(qp_node *fs, string node_name, table_list *schema, bool is_aggr_query, bool is_fj, bool uses_bloom){
	int g, cl;

	string ret = "struct FTA * "+generate_alloc_name(node_name) +
	   "(struct FTAID ftaid, gs_uint32_t reusable, gs_int32_t command,  gs_int32_t sz, void *value){\n";

	ret+="\tstruct "+generate_fta_name(node_name)+"* f;\n";
	ret+="\tint i;\n";
	ret += "\n";
	ret+="\tif((f=fta_alloc(0,sizeof(struct "+generate_fta_name(node_name)+")))==0){\n\t\treturn(0);\n\t}\n";

//				assign a streamid to fta instance
	ret+="\t/* assign a streamid */\n";
	ret+="\tf->f.ftaid = ftaid;\n";
	ret+="\tf->f.ftaid.streamid = (gs_p_t)f;\n";
	ret+="\tgslog(LOG_INFO,\"Lfta "+node_name+" has FTAID {ip=%u,port=%u,index=%u,streamid=%u}\\n\",f->f.ftaid.ip,f->f.ftaid.port,f->f.ftaid.index,f->f.ftaid.streamid);\n";

	if(is_aggr_query){
		ret += "\tf->n_aggrs = 0;\n";

		ret += "\tf->max_aggrs = ";

//				Computing the number of aggregate blocks is a little
//				tricky.  If there are no GB attrs, or if all GB attrs
//				are temporal, then use a single aggregate block, else
//				use a default value (10).  A user specification overrides
//				this logic.
		bool single_group = true;
		for(g=0;g<gb_tbl->size();g++){
			data_type *gdt = gb_tbl->get_data_type(g);
			if(! gdt->is_temporal() ){
				single_group = false;
			}
		}
		string max_aggr_str = fs->get_val_of_def("aggregate_slots");
		int max_aggr_i = atoi(max_aggr_str.c_str());
		if(max_aggr_i <= 0){
			if(single_group)
				ret += "2";
			else
				ret += int_to_string(DEFAULT_LFTA_HASH_TABLE_SIZE);
		}else{
			unsigned int naggrs = 1;		// make it power of 2
			unsigned int nones = 0;
			while(max_aggr_i){
				if(max_aggr_i&1)
					nones++;
				naggrs = naggrs << 1;
				max_aggr_i = max_aggr_i >> 1;
			}
			if(nones==1)		// in case it was already a power of 2.
				naggrs/=2;
			ret += int_to_string(naggrs);
		}
		ret += ";\n";

		ret+="\tif ((f->aggr_table = sp_fta_alloc((struct FTA *)f,sizeof(struct "+generate_aggr_struct_name(node_name)+") * f->max_aggrs))==0) {\n";
		ret+="\t\treturn(0);\n";
		ret+="\t}\n\n";
//		ret+="/* compute how many integers we need to store the hashmap */\n";
//		ret+="\tf->bitmap_size = (f->max_aggrs % (sizeof(gs_uint32_t) * 4)) ? (f->max_aggrs / (sizeof(gs_uint32_t) * 4) + 1) : (f->max_aggrs / (sizeof(gs_uint32_t) * 4));\n\n";
		ret+="\tif ((f->aggr_table_hashmap = sp_fta_alloc((struct FTA *)f,sizeof(gs_uint32_t) * f->max_aggrs))==0) {\n";
		ret+="\t\treturn(0);\n";
		ret+="\t}\n";
		ret+="/*\t\tfill bitmap with zero \t*/\n";
		ret+="\tfor (i = 0; i < f->max_aggrs; ++i)\n";
		ret+="\t\tf->aggr_table_hashmap[i] = 0;\n";
		ret+="\tf->generation=0;\n";
		ret+="\tf->flush_pos = f->max_aggrs;\n";

		ret += "\tf->flush_ctr = 0;\n";

	}

	if(is_fj){
		if(uses_bloom){
			ret+="\tf->first_exec = 1;\n";
			unsigned int n_bloom = 11;
			string n_bloom_str = fs->get_val_of_def("num_bloom");
			int tmp_n_bloom = atoi(n_bloom_str.c_str());
			if(tmp_n_bloom>0)
				n_bloom = tmp_n_bloom+1;

			unsigned int window_len = ((filter_join_qpn *)fs)->temporal_range;
			if(window_len < n_bloom){
				n_bloom = window_len+1;
			}

			int bf_exp_size = 12;  // base-2 log of number of bits
			string bloom_len_str = fs->get_val_of_def("bloom_size");
			int tmp_bf_exp_size = atoi(bloom_len_str.c_str());
			if(tmp_bf_exp_size > 3 && tmp_bf_exp_size < 32){
				bf_exp_size = tmp_bf_exp_size;
			}
			int bf_bit_size = 1 << 12;
			int bf_byte_size = bf_bit_size / (8*sizeof(char));

			int bf_tot = n_bloom*bf_byte_size;
			ret+="\tif ((f->bf_table = sp_fta_alloc((struct FTA *)f,"+int_to_string(bf_tot)+"))==0) {\n";
			ret+="\t\treturn(0);\n";
			ret+="\t}\n";
			ret +=
"	for(i=0;i<"+int_to_string(bf_tot)+";i++)\n"
"		f->bf_table[i] = 0;\n"
;
		}else{
			unsigned int ht_size = 4096;
			string ht_size_s = fs->get_val_of_def("aggregate_slots");
			int tmp_ht_size = atoi(ht_size_s.c_str());
			if(tmp_ht_size > 1024){
				unsigned int hs = 1;		// make it power of 2
				while(tmp_ht_size){
					hs =hs << 1;
					tmp_ht_size = tmp_ht_size >> 1;
				}
				ht_size = hs;
			}
			ret+="\tif ((f->join_table = sp_fta_alloc((struct FTA *)f,sizeof(struct "+generate_fj_struct_name(node_name)+") * "+int_to_string(ht_size)+"))==0) {\n";
			ret+="\t\treturn(0);\n";
			ret+="\t}\n\n";
			ret +=
"	for(i=0;i<"+int_to_string(ht_size)+";i++)\n"
"		f->join_table[i].ts = 0;\n"
;
		}
	}

//			Initialize the complex literals (which might be handles).

	for(cl=0;cl<complex_literals->size();cl++){
		literal_t *l = complex_literals->get_literal(cl);
//		sprintf(tmpstr,"\tf->complex_literal_%d = ",cl);
//		ret += tmpstr + l->to_C_code() + ";\n";
		sprintf(tmpstr,"&(f->complex_literal_%d)",cl);
		ret += "\t" + l->to_C_code(tmpstr) + ";\n";
	}

	ret += "\n";

//			Initialize the last seen values of temporal attributes to min(max) value of
//			their respective type
//			Create places to hold the last values of temporal attributes referenced in select clause


	col_id_set temp_cids;		//	col ids of temp attributes in select clause

	int s;
	col_id_set::iterator csi;

	for(s=0;s<sl_list.size();s++){
		data_type *sdt = sl_list[s]->get_data_type();
		if (sdt->is_temporal()) {
			gather_se_col_ids(sl_list[s],temp_cids, gb_tbl);
		}
	}

	for(csi=temp_cids.begin(); csi != temp_cids.end();++csi){
		int tblref = (*csi).tblvar_ref;
		int schref = (*csi).schema_ref;
		string field = (*csi).field;
		data_type dt(schema->get_type_name(schref,field), schema->get_modifier_list(schref,field));
		if (dt.is_increasing()) {
			sprintf(tmpstr,"\tf->last_%s_%d = %s;\n", field.c_str(), tblref, dt.get_min_literal().c_str());
			ret += tmpstr;
		} else if (dt.is_decreasing()) {
			sprintf(tmpstr,"\tf->last_%s_%d = %s;\n", field.c_str(), tblref, dt.get_max_literal().c_str());
			ret += tmpstr;
		}
	}

//	initialize last seen values of temporal groubpy variables
	if(is_aggr_query){
		for(g=0;g<gb_tbl->size();g++){
                        data_type *dt = gb_tbl->get_data_type(g);
                        if(dt->is_temporal()){
/*
                                fprintf(stderr,"group by attribute %s is temporal, ",
                                                gb_tbl->get_name(g).c_str());
*/
                                if(dt->is_increasing()){
					sprintf(tmpstr,"\tf->last_gb_%d = f->last_flushed_gb_%d = %s;\n",g, g, dt->get_min_literal().c_str());
                                }else{
					sprintf(tmpstr,"\tf->last_gb_%d = f->last_flushed_gb_%d = %s;\n",g, g, dt->get_max_literal().c_str());
                                }
				ret += tmpstr;
                        }
		}
	}

	ret += "\tf->f.alloc_fta="+generate_alloc_name(node_name)+";\n";
	ret+="\tf->f.free_fta=free_fta_"+node_name+";\n";
	ret+="\tf->f.control_fta=control_fta_"+node_name+";\n";
	ret+="\tf->f.accept_packet=accept_packet_"+node_name+";\n";
	ret+="\tf->f.clock_fta=clock_fta_"+node_name+";\n\n";

//			Initialize runtime stats
	ret+="\tf->in_tuple_cnt = 0;\n";
	ret+="\tf->out_tuple_cnt = 0;\n";
	ret+="\tf->out_tuple_sz = 0;\n";
	ret+="\tf->accepted_tuple_cnt = 0;\n";
	ret+="\tf->cycle_cnt = 0;\n";
	ret+="\tf->collision_cnt = 0;\n";
	ret+="\tf->eviction_cnt = 0;\n";
	ret+="\tf->sampling_rate = 1.0;\n";

	ret+="\tf->trace_id = 0;\n\n";
    if(param_tbl->size() > 0){
    	ret+=
"\tif(load_params_"+node_name+"(f, sz, value, 1)){\n"
"#ifndef LFTA_IN_NIC\n"
"\t\t\tfprintf(stderr,\"WARNING: parameter passed to lfta "+node_name+" is too small. This query does not have valid parameters, bailing out.\\n\");\n"
"#else\n"
"\t\t}\n"
"#endif\n"
"\t\t\treturn 0;\n"
"\t\t}\n";
	}

//			Register the pass-by-handle parameters
    int ph;
    for(ph=0;ph<param_handle_table.size();++ph){
		data_type pdt(param_handle_table[ph]->type_name);
		sprintf(tmpstr,"\tf->handle_param_%d = %s((struct FTA *)f,",ph,param_handle_table[ph]->lfta_registration_fcn().c_str());
		switch(param_handle_table[ph]->val_type){
		case cplx_lit_e:
			ret += tmpstr;
			if(pdt.is_buffer_type()) ret += "&(";
			sprintf(tmpstr,"f->complex_literal_%d",param_handle_table[ph]->complex_literal_idx);
			ret += tmpstr ;
			if(pdt.is_buffer_type()) ret += ")";
			ret +=  ");\n";
			break;
		case litval_e:
//				not complex, no constructor
			ret += tmpstr;
			ret += param_handle_table[ph]->litval->to_C_code("") + ");\n";
			break;
		case param_e:
//				query parameter handles are regstered/deregistered in the
//				load_params function.
//			ret += "t->param_"+param_handle_table[ph]->param_name;
			break;
		default:
			fprintf(stderr, "INTERNAL ERROR unknown case found when processing pass-by-handle parameter table.\n");
			exit(1);
		}
	}

	ret += "\treturn (struct FTA *) f;\n";
	ret += "}\n\n";

	return(ret);
}




//////////////////////////////////////////////////////////////////

string generate_lfta_block(qp_node *fs, table_list *schema, int gid,
//		map<string,string> &int_fcn_defs,
		ext_fcn_list *Ext_fcns, string &schema_embed_str, ifq_t *ifdb, nic_property *nicp, set<unsigned int> &s_pids){
	bool is_aggr_query;
	int s,p,g;
	string retval;

/////////////////////////////////////////////////////////////
///		Do operator-generic processing, such as
///		gathering the set of referenced columns,
///		generating structures, etc.

//		Initialize globals to empty.
	gb_tbl = NULL; aggr_tbl = NULL;
	global_id = -1; nicprop = NULL;
	param_tbl = fs->get_param_tbl();
	sl_list.clear(); where.clear();
	partial_fcns.clear();
	fcn_ref_cnt.clear(); is_partial_fcn.clear();
	pred_class.clear(); pred_pos.clear();
	sl_fcns_start=sl_fcns_end=wh_fcns_start=wh_fcns_end=0;
	gb_fcns_start=gb_fcns_end=ag_fcns_start=ag_fcns_end=0;


//		Does the lfta read packed results from the NIC?
	nicprop = nicp;			// load into global
	global_id = gid;
    packed_return = false;
 	if(nicp && nicp->option_exists("Return")){
		if(nicp->option_value("Return") == "Packed"){
			packed_return = true;
		}else{
			fprintf(stderr,"Warning, nic option value of Return=%s is not recognized, ignoring\n",nicp->option_value("Return").c_str());
		}
	}


//			Extract data which defines the query.
//				complex literals gathered now.
	complex_literals = fs->get_cplx_lit_tbl(Ext_fcns);
	param_handle_table = fs->get_handle_param_tbl(Ext_fcns);
	string node_name = fs->get_node_name();
    bool is_fj = false, uses_bloom = false;


	if(fs->node_type() == "spx_qpn"){
		is_aggr_query = false;
		spx_qpn *spx_node = (spx_qpn *)fs;
		sl_list = spx_node->get_select_se_list();
		where = spx_node->get_where_clause();
		gb_tbl = NULL;
		aggr_tbl = NULL;
	} else
	if(fs->node_type() == "sgah_qpn"){
		is_aggr_query = true;
		sgah_qpn *sgah_node = (sgah_qpn *)fs;
		sl_list = sgah_node->get_select_se_list();
		where = sgah_node->get_where_clause();
		gb_tbl = sgah_node->get_gb_tbl();
		aggr_tbl = sgah_node->get_aggr_tbl();

		if((sgah_node->get_having_clause()).size() > 0){
			fprintf(stderr,"Warning in LFTA %s: HAVING clause will be ignored.\n", fs->get_node_name().c_str());
		}
	} else
	if(fs->node_type() == "filter_join"){
		is_aggr_query = false;
        is_fj = true;
		filter_join_qpn *fj_node = (filter_join_qpn *)fs;
		sl_list = fj_node->get_select_se_list();
		where = fj_node->get_where_clause();
		uses_bloom = fj_node->use_bloom;
		gb_tbl = NULL;
		aggr_tbl = NULL;
	} else {
		fprintf(stderr,"INTERNAL ERROR, unrecognized node type %s in generate_lfta_block\n", fs->node_type().c_str());
		exit(1);
	}

//			Build list of "partial functions", by clause.
//			NOTE : partial fcns are not handles well.
//			The act of searching for them associates the fcn call
//			in the SE with an index to an array.  Refs to the
//			fcn value are replaced with refs to the variable they are
//			unpacked into.  A more general tagging mechanism would be better.

	int i;
	vector<bool> *pfunc_ptr = NULL;
	vector<int> *ref_cnt_ptr = NULL;
	if(!is_aggr_query){		// don't collect cacheable fcns on aggr query.
		ref_cnt_ptr = &fcn_ref_cnt;
		pfunc_ptr = &is_partial_fcn;
	}

	sl_fcns_start = 0;
	for(i=0;i<sl_list.size();i++){
		find_partial_fcns(sl_list[i], &partial_fcns, ref_cnt_ptr, pfunc_ptr, Ext_fcns);
	}
	wh_fcns_start = sl_fcns_end = partial_fcns.size();
	for(i=0;i<where.size();i++){
		find_partial_fcns_pr(where[i]->pr, &partial_fcns, ref_cnt_ptr, pfunc_ptr, Ext_fcns);
	}
	gb_fcns_start = wh_fcns_end = partial_fcns.size();
	if(gb_tbl != NULL){
		for(i=0;i<gb_tbl->size();i++){
			find_partial_fcns(gb_tbl->get_def(i), &partial_fcns, NULL, NULL, Ext_fcns);
		}
	}
	ag_fcns_start = gb_fcns_end = partial_fcns.size();
	if(aggr_tbl != NULL){
		for(i=0;i<aggr_tbl->size();i++){
			find_partial_fcns(aggr_tbl->get_aggr_se(i), NULL, NULL, &is_partial_fcn, Ext_fcns);
		}
	}
	ag_fcns_end = partial_fcns.size();

//		Fill up the is_partial_fcn and fcn_ref_cnt arrays.
	if(is_aggr_query){
		for(i=0; i<partial_fcns.size();i++){
			fcn_ref_cnt.push_back(1);
			is_partial_fcn.push_back(true);
		}
	}

//		Unmark non-partial expensive functions referenced only once.
	for(i=0; i<partial_fcns.size();i++){
		if(!is_partial_fcn[i] && fcn_ref_cnt[i] <= 1){
			partial_fcns[i]->set_partial_ref(-1);
		}
	}

	node_name = normalize_name(node_name);

	retval += generate_preamble(schema, /*int_fcn_defs,*/ node_name, schema_embed_str);

	if(packed_return){		// generate unpack struct
		vector<tablevar_t *> input_tbls = fs->get_input_tbls();
		int schref = input_tbls[0]->get_schema_ref();
		vector<string> refd_cols;
		for(s=0;s<sl_list.size();++s){
			gather_nicsafe_cols(sl_list[s],refd_cols, nicp, gb_tbl);
		}
		for(p=0;p<where.size();++p){
//				I'm not disabling these preds ...
			gather_nicsafe_cols(where[p]->pr,refd_cols, nicp, gb_tbl);
		}
		if(gb_tbl){
			for(g=0;g<gb_tbl->size();++g){
			  gather_nicsafe_cols(gb_tbl->get_def(g),refd_cols, nicp, gb_tbl);
			}
		}
		sort(refd_cols.begin(), refd_cols.end());
		retval += "struct "+node_name+"_input_struct{\n";
		retval += "\tint __lfta_id_fm_nic__;\n";
		int vsi;
		for(vsi=0;vsi<refd_cols.size();++vsi){
    		data_type dt(schema->get_type_name(schref,refd_cols[vsi]));
			retval+="\t"+dt.get_cvar_type()+" unpack_var_"+refd_cols[vsi]+";\n";
		}
		retval+="};\n\n";
	}


/////////////////////////////////////////////////////
//			Common stuff unpacked, do some generation

  	if(is_aggr_query)
	  retval += generate_aggr_struct(node_name, gb_tbl, aggr_tbl);
	if(is_fj)
		retval += generate_fj_struct((filter_join_qpn *)fs, node_name);

	retval += generate_fta_struct(node_name, gb_tbl, aggr_tbl, param_tbl, complex_literals, param_handle_table, is_aggr_query, is_fj, uses_bloom, schema);
  	retval += generate_tuple_struct(node_name, sl_list) ;

	if(is_aggr_query)
		retval += generate_fta_flush(node_name, schema, Ext_fcns) ;
	if(param_tbl->size() > 0)
		retval += generate_fta_load_params(node_name) ;
	retval += generate_fta_free(node_name, is_aggr_query) ;
	retval +=  generate_fta_control(node_name, schema, is_aggr_query) ;
	retval +=  generate_fta_accept(fs, node_name, schema, Ext_fcns, is_aggr_query, is_fj, s_pids) ;


	/* extract the value of Time_Correlation from interface definition */
	int e,v;
	string es;
	unsigned time_corr;
	vector<tablevar_t *> tvec =  fs->get_input_tbls();
	vector<string> time_corr_vec = ifdb->get_iface_vals(tvec[0]->get_machine(), tvec[0]->get_interface(),"Time_Correlation",e,es);
	if (time_corr_vec.empty())
		time_corr = DEFAULT_TIME_CORR;
	else
		time_corr = atoi(time_corr_vec[0].c_str());

	retval.append( generate_fta_clock(node_name, schema, time_corr, is_aggr_query) );
	retval.append( generate_fta_alloc(fs, node_name, schema, is_aggr_query, is_fj, uses_bloom) );

  return(retval);
}



int compute_snap_len(qp_node *fs, table_list *schema){

//		Initialize global vars
	gb_tbl = NULL;
	sl_list.clear(); where.clear();

	if(fs->node_type() == "spx_qpn"){
		spx_qpn *spx_node = (spx_qpn *)fs;
		sl_list = spx_node->get_select_se_list();
		where = spx_node->get_where_clause();
	}
	else if(fs->node_type() == "sgah_qpn"){
		sgah_qpn *sgah_node = (sgah_qpn *)fs;
		sl_list = sgah_node->get_select_se_list();
		where = sgah_node->get_where_clause();
		gb_tbl = sgah_node->get_gb_tbl();
	}
	else if(fs->node_type() == "filter_join"){
		filter_join_qpn *fj_node = (filter_join_qpn *)fs;
		sl_list = fj_node->get_select_se_list();
		where = fj_node->get_where_clause();
	} else{
		fprintf(stderr,"INTERNAL ERROR, node type %s not recognized in compute_snap_len\n",fs->node_type().c_str());
		exit(1);
	}

//			Gather all column references, need to define unpacking variables.
  int w,s;
  col_id_set cid_set;
  col_id_set::iterator csi;

  for(w=0;w<where.size();++w)
  	gather_pr_col_ids(where[w]->pr,cid_set, gb_tbl);
  for(s=0;s<sl_list.size();s++){
  	gather_se_col_ids(sl_list[s],cid_set, gb_tbl);
  }

  int g;
  if(gb_tbl != NULL){
  	for(g=0;g<gb_tbl->size();g++)
	  gather_se_col_ids(gb_tbl->get_def(g),cid_set, gb_tbl);
  }

  //			compute snap length
  int snap_len = -1;
  int n_snap=0;
  for(csi=cid_set.begin(); csi!=cid_set.end();++csi){
    int schref = (*csi).schema_ref;
	int tblref = (*csi).tblvar_ref;
    string field = (*csi).field;

	param_list *field_params = schema->get_modifier_list(schref, field);
	if(field_params->contains_key("snap_len")){
		string fld_snap_str = field_params->val_of("snap_len");
		int fld_snap;
		if(sscanf(fld_snap_str.c_str(),"%d",&fld_snap)>0){
			if(fld_snap > snap_len) snap_len = fld_snap;
			n_snap++;
		}else{
			fprintf(stderr,"CONFIGURATION ERROR: field %s has a non-numeric snap length (%s), ignoring\n",field.c_str(), fld_snap_str.c_str() );
		}
	}
  }

  if(n_snap == cid_set.size()){
  	return (snap_len);
  }else{
	return -1;
  }


}

//		Function which computes an optimal
//		set of unpacking functions.

void find_optimal_unpack_fcns(col_id_set &upref_cids, table_list *Schema, map<col_id, string,lt_col_id> &ucol_fcn_map){
	map<string, int> pfcn_count;
	map<string, int>::iterator msii;
	col_id_set::iterator cisi;
	set<string>::iterator ssi;
	string best_fcn;

	while(ucol_fcn_map.size() < upref_cids.size()){

//			Gather unpack functions referenced by unaccounted-for
//			columns, and increment their reference count.
		pfcn_count.clear();
		for(cisi=upref_cids.begin();cisi!=upref_cids.end();++cisi){
			if(ucol_fcn_map.count((*cisi)) == 0){
				set<string> ufcns = Schema->get_field((*cisi).schema_ref, (*cisi).field)->get_unpack_fcns();
				for(ssi=ufcns.begin();ssi!=ufcns.end();++ssi)
					pfcn_count[(*ssi)]++;
			}
		}

//		Get the lowest cost per field function.
		float min_cost = 0.0;
		string best_fcn = "";
		for(msii=pfcn_count.begin();msii!=pfcn_count.end();++msii){
			int fcost = Schema->get_ufcn_cost((*msii).first);
			if(fcost < 0){
				fprintf(stderr,"CONFIGURATION ERROR, unpack function %s either has negative cost or is not defined.\n",(*msii).first.c_str());
				exit(1);
			}
			float this_cost = (1.0*fcost)/(*msii).second;
			if(msii == pfcn_count.begin() || this_cost < min_cost){
				min_cost = this_cost;
				best_fcn = (*msii).first;
			}
		}
		if(best_fcn == ""){
			fprintf(stderr,"ERROR, could not find a best field unpqacking function.\n");
			exit(1);
		}

//		Assign this function to the unassigned fcns which use it.
		for(cisi=upref_cids.begin();cisi!=upref_cids.end();++cisi){
			if(ucol_fcn_map.count((*cisi)) == 0){
				set<string> ufcns = Schema->get_field((*cisi).schema_ref, (*cisi).field)->get_unpack_fcns();
				if(ufcns.count(best_fcn)>0)
					ucol_fcn_map[(*cisi)] = best_fcn;
			}
		}
	}
}



//		Generate an initial test test for the lfta
//		Assume that the predicate references no external functions,
//		and especially no partial functions,
//		aggregates, internal functions.
string generate_lfta_prefilter(vector<cnf_set *> &pred_list,
		col_id_set &temp_cids, table_list *Schema, ext_fcn_list *Ext_fcns,
		vector<col_id_set> &lfta_cols, vector<long long int> &lfta_sigs,
		vector<int> &lfta_snap_lens){
  col_id_set tmp_cid_set, cid_set,upref_cids,upall_cids;
  col_id_set::iterator csi;
	int o,p,q;
	string ret;

//		Gather complex literals in the prefilter.
	cplx_lit_table *complex_literals = new cplx_lit_table();
	for(p=0;p<pred_list.size();++p){
		find_complex_literal_pr(pred_list[p]->pr,Ext_fcns, complex_literals);
	}


//		Find the combinable predicates
	vector<predicate_t *> pr_list;
	for(p=0;p<pred_list.size();++p){
    	find_combinable_preds(pred_list[p]->pr,&pr_list, Schema, Ext_fcns);
	}

//		Analyze the combinable predicates to find the predicate classes.
	pred_class.clear();		// idx to equiv pred in equiv_list
	pred_pos.clear();		// idx to returned bitmask.
	vector<predicate_t *> equiv_list;
	vector<int> num_equiv;


	for(p=0;p<pr_list.size();++p){
		for(q=0;q<equiv_list.size();++q){
			if(is_equivalent_class_pred_base(equiv_list[q],pr_list[p],Schema,Ext_fcns))
				break;
		}
		if(q == equiv_list.size()){		// no equiv : create new
			pred_class.push_back(equiv_list.size());
			equiv_list.push_back(pr_list[p]);
			pred_pos.push_back(0);
			num_equiv.push_back(1);

		}else{			// pr_list[p] is equivalent to pred q
			pred_class.push_back(q);
			pred_pos.push_back(num_equiv[q]);
			num_equiv[q]++;
		}
	}

//		Generate the variables which hold the common pred handles
	ret += "/*\t\tprefilter global vars.\t*/\n";
	for(q=0;q<equiv_list.size();++q){
		for(p=0;p<=(num_equiv[q]/32);++p){
			ret += "void *pref_common_pred_hdl_"+int_to_string(q)+"_"+int_to_string(p)+";\n";
		}
	}

//		Struct to hold prefilter complex literals
	ret += "struct prefilter_complex_lit_struct {\n";
	if(complex_literals->size() == 0)
		ret += "\tint no_variable;\n";
	int cl;
	for(cl=0;cl<complex_literals->size();cl++){
		literal_t *l = complex_literals->get_literal(cl);
		data_type *dtl = new data_type( l->get_type() );
		sprintf(tmpstr,"\t%s complex_literal_%d;\n",dtl->get_cvar_type().c_str(),cl);
		ret += tmpstr;
	}
	ret += "} prefilter_complex_lits;\n\n";


//		Generate the prefilter initialziation code
	ret += "void init_lfta_prefilter(){\n";

//		First initialize complex literals, if any.
	ret += "\tstruct prefilter_complex_lit_struct *t = &prefilter_complex_lits;\n";
	for(cl=0;cl<complex_literals->size();cl++){
		literal_t *l = complex_literals->get_literal(cl);
		sprintf(tmpstr,"&(t->complex_literal_%d)",cl);
		ret += "\t" + l->to_C_code(tmpstr) + ";\n";
	}


	set<int> epred_seen;
	for(p=0;p<pr_list.size();++p){
		int q = pred_class[p];
//printf("\tq=%d\n",q);
		if(epred_seen.count(q)>0){
			ret += "\tregister_commonpred_handles_"+equiv_list[q]->get_op()+"(";
  			vector<bool> cl_op = Ext_fcns->get_class_indicators(equiv_list[q]->get_fcn_id());
  			vector<scalarexp_t *> op_list = pr_list[p]->get_op_list();
			for(o=0;o<op_list.size();++o){
				if(! cl_op[o]){
					ret += generate_se_code(op_list[o],Schema)+", ";
				}
			}
			ret += "pref_common_pred_hdl_"+int_to_string(q)+"_"+int_to_string(pred_pos[p]/32)+","+int_to_string(pred_pos[p]%32)+");\n";
			epred_seen.insert(q);
		}else{
			ret += "\tpref_common_pred_hdl_"+int_to_string(q)+"_"+int_to_string(pred_pos[p]/32)+" = (void *)register_commonpred_handles_"+equiv_list[q]->get_op()+"(";
  			vector<bool> cl_op = Ext_fcns->get_class_indicators(equiv_list[q]->get_fcn_id());
  			vector<scalarexp_t *> op_list = pr_list[p]->get_op_list();
			for(o=0;o<op_list.size();++o){
				if(! cl_op[o]){
					ret += generate_se_code(op_list[o],Schema)+", ";
				}
			}
			ret += "NULL,"+int_to_string(pred_pos[p]%32)+");\n";
			epred_seen.insert(q);
		}
	}
	ret += "}\n\n";



//		Start on main body code generation
  ret+="gs_uint64_t lfta_prefilter(void *pkt){\n";


///--------------------------------------------------------------
///		Generate and store the prefilter body,
///		reuse it for the snap length calculator
///-------------------------------------------------------------
	string body;

    body += "\tstruct packet *p = (struct packet *)pkt;\n";



//		Gather the colids to store unpacked variables.
	for(p=0;p<pred_list.size();++p){
    	gather_pr_col_ids(pred_list[p]->pr,tmp_cid_set, gb_tbl);
	}

//		make the col_ids refer to the base tables, and
//		grab the col_ids with at least one unpacking function.
	for(csi=tmp_cid_set.begin();csi!=tmp_cid_set.end();++csi){
		string c_tbl = Schema->get_basetbl_name((*csi).schema_ref,(*csi).field);
		col_id tmp_col_id;
		tmp_col_id.field = (*csi).field;
		tmp_col_id.tblvar_ref = (*csi).tblvar_ref;
		tmp_col_id.schema_ref = Schema->get_table_ref(c_tbl);
		cid_set.insert(tmp_col_id);
		field_entry *fe = Schema->get_field(tmp_col_id.schema_ref, tmp_col_id.field);
		if(fe->get_unpack_fcns().size()>0)
			upref_cids.insert(tmp_col_id);


	}

//		Find the set of unpacking programs needed for the
//		prefilter fields.
	map<col_id, string,lt_col_id>  ucol_fcn_map;
	find_optimal_unpack_fcns(upref_cids, Schema, ucol_fcn_map);
	set<string> pref_ufcns;
	map<col_id, string,lt_col_id>::iterator mcis;
	for(mcis=ucol_fcn_map.begin(); mcis!=ucol_fcn_map.end(); mcis++){
		pref_ufcns.insert((*mcis).second);
	}



//			Variables for unpacking attributes.
    body += "/*\t\tVariables for unpacking attributes\t*/\n";
    for(csi=cid_set.begin(); csi!=cid_set.end();++csi){
      int schref = (*csi).schema_ref;
	  int tblref = (*csi).tblvar_ref;
      string field = (*csi).field;
      data_type dt(Schema->get_type_name(schref,field));
      sprintf(tmpstr,"\t%s unpack_var_%s_%d;\n",dt.get_cvar_type().c_str(),
    	field.c_str(), tblref);
      body += tmpstr;
      sprintf(tmpstr,"\tgs_retval_t ret_%s_%d;\n", field.c_str(),tblref);
      body += tmpstr;
    }
//			Variables for unpacking temporal attributes.
    body += "/*\t\tVariables for unpacking temporal attributes\t*/\n";
    for(csi=temp_cids.begin(); csi!=temp_cids.end();++csi){
	  if (cid_set.count(*csi) == 0) {
      	int schref = (*csi).schema_ref;
		int tblref = (*csi).tblvar_ref;
        string field = (*csi).field;
        data_type dt(Schema->get_type_name(schref,field));
        sprintf(tmpstr,"\t%s unpack_var_%s_%d;\n",dt.get_cvar_type().c_str(),
    	  field.c_str(), tblref);
        body += tmpstr;

	  }
    }
    body += "\n\n";

//		Variables for combinable predicate evaluation
	body += "/*\t\tVariables for common prdicate evaluation\t*/\n";
	for(q=0;q<equiv_list.size();++q){
		for(p=0;p<=(num_equiv[q]/32);++p){
			body += "unsigned long int pref_common_pred_val_"+int_to_string(q)+"_"+int_to_string(p)+" = 0;\n";
		}
	}


//			Variables that are always needed
    body += "/*\t\tVariables which are always needed\t*/\n";
	body += "\tgs_uint64_t retval=0, bitpos=1;\n";
	body += "\tstruct prefilter_complex_lit_struct *t = &prefilter_complex_lits;\n";

//		Call the unpacking functions for the prefilter fields
	if(pref_ufcns.size() > 0)
		body += "\n/*\t\tcall field unpacking functions\t*/\n";
	set<string>::iterator ssi;
	for(ssi=pref_ufcns.begin(); ssi!=pref_ufcns.end(); ++ssi){
		body += "\t"+Schema->get_ufcn_fcn((*ssi))+"(p);\n";
	}


//		Unpack the accessed attributes
	body += "\n/*\t\tUnpack the accessed attributes.\t*/\n";
    for(csi=cid_set.begin();csi!=cid_set.end();++csi){
 	  int tblref = (*csi).tblvar_ref;
      int schref = (*csi).schema_ref;
   	  string field = (*csi).field;
  	  sprintf(tmpstr,"\tret_%s_%d =  (%s(p, &unpack_var_%s_%d) == 0);\n",
   		field.c_str(),tblref,Schema->get_fcn(schref,field).c_str(), field.c_str(), tblref);
   	  body += tmpstr;
    }

//		next unpack the temporal attributes and ignore the errors
//		We are assuming here that failed unpack of temporal attributes
//		is not going to overwrite the last stored value
//		Failed upacks are ignored
    for(csi=temp_cids.begin();csi!=temp_cids.end();++csi){
 	  int tblref = (*csi).tblvar_ref;
      int schref = (*csi).schema_ref;
   	  string field = (*csi).field;
  	  sprintf(tmpstr,"\t%s(p, &prefilter_temp_vars.unpack_var_%s_%d);\n",
   		 Schema->get_fcn(schref,field).c_str(), field.c_str(), tblref);
   	  body += tmpstr;
    }

//		Evaluate the combinable predicates
	if(equiv_list.size()>0)
		body += "/*\t\tEvaluate the combinable predicates.\t*/\n";
	for(q=0;q<equiv_list.size();++q){
		for(p=0;p<=(num_equiv[q]/32);++p){

//		Only call the common eval fcn if all ref'd fields present.
  			col_id_set pred_cids;
  			col_id_set::iterator cpi;
		   	gather_pr_col_ids(equiv_list[q], pred_cids, gb_tbl);
			if(pred_cids.size()>0){
			body += "\tif(";
				for(cpi=pred_cids.begin();cpi!=pred_cids.end();++cpi){
					if(cpi != pred_cids.begin())
						body += " && ";
      				string field = (*cpi).field;
 	  				int tblref = (*cpi).tblvar_ref;
					body += "ret_"+field+"_"+int_to_string(tblref);
				}
				body+=")\n";
			}

			body += "\t\tpref_common_pred_val_"+int_to_string(q)+"_"+int_to_string(p)+" = eval_commonpred_"+equiv_list[q]->get_op()+"(pref_common_pred_hdl_"+int_to_string(q)+"_"+int_to_string(p);
  			vector<scalarexp_t *> op_list = equiv_list[q]->get_op_list();
  			vector<bool> cl_op = Ext_fcns->get_class_indicators(equiv_list[q]->get_fcn_id());
			for(o=0;o<op_list.size();++o){
				if(cl_op[o]){
					body += ","+generate_se_code(op_list[o],Schema);
				}
			}
			body += ");\n";
		}
	}


	for(p=0;p<pred_list.size();++p){
  		col_id_set pred_cids;
  		col_id_set::iterator cpi;
	   	gather_pr_col_ids(pred_list[p]->pr,pred_cids, gb_tbl);
		if(pred_cids.size()>0){
			body += "\tif(";
			for(cpi=pred_cids.begin();cpi!=pred_cids.end();++cpi){
				if(cpi != pred_cids.begin())
					body += " && ";
      			string field = (*cpi).field;
 	  			int tblref = (*cpi).tblvar_ref;
				body += "ret_"+field+"_"+int_to_string(tblref);
			}
			body+=")\n";
		}
    	body += "\t\tif("+generate_predicate_code(pred_list[p]->pr,Schema)+")\n\t\t\tretval |= bitpos;\n";
		body+="\tbitpos = bitpos << 1;\n";
	}

// ---------------------------------------------------------------
//		Finished with the body of the prefilter
// --------------------------------------------------------------

	ret += body;

//			Collect fields referenced by an lfta but not
//			already unpacked for the prefilter.

//printf("upref_cids is:\n");
//for(csi=upref_cids.begin();csi!=upref_cids.end();csi++)
//printf("\t%s %d\n",(*csi).field.c_str(), (*csi).schema_ref);
//printf("pref_ufcns is:\n");
//for(ssi=pref_ufcns.begin();ssi!=pref_ufcns.end();++ssi)
//printf("\t%s\n",(*ssi).c_str());

	int l;
	for(l=0;l<lfta_cols.size();++l){
		for(csi=lfta_cols[l].begin();csi!=lfta_cols[l].end();++csi){
			string c_tbl = Schema->get_basetbl_name((*csi).schema_ref,(*csi).field);
			col_id tmp_col_id;
			tmp_col_id.field = (*csi).field;
			tmp_col_id.tblvar_ref = (*csi).tblvar_ref;
			tmp_col_id.schema_ref = Schema->get_table_ref(c_tbl);
			field_entry *fe = Schema->get_field(tmp_col_id.schema_ref, tmp_col_id.field);
			set<string> fld_ufcns = fe->get_unpack_fcns();
//printf("tmpcol is (%s, %d), ufcns size is %d, upref_cids cnt is %d\n",tmp_col_id.field.c_str(),tmp_col_id.schema_ref,fld_ufcns.size(),  upref_cids.count(tmp_col_id));
			if(fld_ufcns.size()>0 && upref_cids.count(tmp_col_id) == 0){
//		Ensure that this field not already unpacked.
				bool found = false;
				for(ssi=fld_ufcns.begin();ssi!=fld_ufcns.end();++ssi){
//printf("\tField has unpacking fcn %s\n",(*ssi).c_str());
					if(pref_ufcns.count((*ssi))){
//printf("Field already unpacked.\n");
						found = true;;
					}
				}
				if(! found){
//printf("\tadding to unpack list\n");
					upall_cids.insert(tmp_col_id);
				}
			}
		}
	}

//printf("upall_cids is:\n");
//for(csi=upall_cids.begin();csi!=upall_cids.end();csi++)
//printf("\t%s %d\n",(*csi).field.c_str(), (*csi).schema_ref);

//		Get the set of unpacking programs for these.
	map<col_id, string,lt_col_id>  uall_fcn_map;
	find_optimal_unpack_fcns(upall_cids, Schema, uall_fcn_map);
	set<string> pall_ufcns;
	for(mcis=uall_fcn_map.begin(); mcis!=uall_fcn_map.end(); mcis++){
//printf("uall_fcn_map[%s %d] = %s\n",(*mcis).first.field.c_str(),(*mcis).first.schema_ref,(*mcis).second.c_str());
		pall_ufcns.insert((*mcis).second);
	}

//		Iterate through the remaining set of unpacking function
	if(pall_ufcns.size() > 0)
		ret += "//\t\tcall all remaining field unpacking functions.\n";
	for(ssi=pall_ufcns.begin(); ssi!=pall_ufcns.end(); ++ssi){
//		gather the set of columns unpacked by this ufcn
		col_id_set fcol_set;
		for(csi=upall_cids.begin();csi!=upall_cids.end();++csi){
			if(uall_fcn_map[(*csi)] == (*ssi))
				fcol_set.insert((*csi));
		}

//		gather the set of lftas which access a field unpacked by the fcn
		set<long long int> clfta;
		for(l=0;l<lfta_cols.size();l++){
			for(csi=fcol_set.begin();csi!=fcol_set.end();++csi){
				if(lfta_cols[l].count((*csi)) > 0)
					break;
			}
			if(csi != fcol_set.end())
				clfta.insert(lfta_sigs[l]);
		}

//		generate the unpacking code
		ret += "\tif(";
		set<long long int>::iterator sii;
		for(sii=clfta.begin();sii!=clfta.end();++sii){
			if(sii!=clfta.begin())
				ret += " || ";
			sprintf(tmpstr,"((retval & %lluull) == %lluull)",(*sii),(*sii));
			ret += tmpstr;
		}
		ret += ")\n\t\t"+Schema->get_ufcn_fcn((*ssi))+"(p);\n";
	}


    ret += "\treturn(retval);\n\n";
  ret += "}\n\n";


// --------------------------------------------------------
//		reuse prefilter body for snaplen calculator

  ret+="gs_uint32_t lfta_pkt_snaplen(void *pkt){\n";

	ret += body;

	int i;
	vector<int> s_snaps = lfta_snap_lens;
	sort(s_snaps.begin(), s_snaps.end());

	if(s_snaps[0] == -1){
		set<unsigned long long int> sigset;
		for(i=0;i<lfta_snap_lens.size();++i){
			if(lfta_snap_lens[i] == -1){
				sigset.insert(lfta_sigs[i]);
			}
		}
		ret += "\tif( ";
		set<unsigned long long int>::iterator sulli;
		for(sulli=sigset.begin();sulli!=sigset.end();++sulli){
			if(sulli!=sigset.begin())
				ret += " || ";
			sprintf(tmpstr,"((retval & %lluull) == %lluull)",(*sulli),(*sulli));
			ret += tmpstr;
		}
		ret += ") return -1;\n";
	}

	int nextpos = lfta_snap_lens.size()-1;
	int nextval = lfta_snap_lens[nextpos];
	while(nextval >= 0){
		set<unsigned long long int> sigset;
		for(i=0;i<lfta_snap_lens.size();++i){
			if(lfta_snap_lens[i] == nextval){
				sigset.insert(lfta_sigs[i]);
			}
		}
		ret += "\tif( ";
		set<unsigned long long int>::iterator sulli;
		for(sulli=sigset.begin();sulli!=sigset.end();++sulli){
			if(sulli!=sigset.begin())
				ret += " || ";
			sprintf(tmpstr,"((retval & %lluull) == %lluull)",(*sulli),(*sulli));
			ret += tmpstr;
		}
		ret += ") return "+int_to_string(nextval)+";\n";

		for(nextpos--;nextpos>0 && lfta_snap_lens[nextpos] == nextval;nextpos--);
		if(nextpos>0)
			nextval = lfta_snap_lens[nextpos];
		else
			nextval = -1;
	}
	ret += "\treturn 0;\n";
	ret += "}\n\n";


  return(ret);
}




//		Generate the struct which will store the the values of
// 		temporal attributesunpacked by prefilter
string generate_lfta_prefilter_struct(col_id_set &cid_set, table_list *Schema) {

  col_id_set::iterator csi;

// printf("generate_lfta_prefilter_struct : %d vars\n",cid_set.size());

  string ret="struct prefilter_unpacked_temp_vars {\n";
  ret += "\t/*\tVariables for unpacking temporal attributes\t*/\n";

  string init_code;

  for(csi=cid_set.begin(); csi!=cid_set.end();++csi){
    int schref = (*csi).schema_ref;
    int tblref = (*csi).tblvar_ref;
    string field = (*csi).field;
   	data_type dt(Schema->get_type_name(schref,field), Schema->get_modifier_list(schref,field));
    sprintf(tmpstr,"\t%s unpack_var_%s_%d;\n",dt.get_cvar_type().c_str(),
    	field.c_str(), tblref);
    ret += tmpstr;

	if (init_code != "")
		init_code += ", ";
	if (dt.is_increasing())
		init_code += dt.get_min_literal();
	else
		init_code += dt.get_max_literal();

  }
  ret += "};\n\n";

  ret += "struct prefilter_unpacked_temp_vars prefilter_temp_vars = {" + init_code + "};\n\n";

  return(ret);
}
