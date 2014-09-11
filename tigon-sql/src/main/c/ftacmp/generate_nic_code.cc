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
#include <algorithm>

#include "parse_fta.h"
#include "parse_schema.h"
#include "analyze_fta.h"
#include "generate_utils.h"
#include "query_plan.h"
#include "generate_nic_code.h"
#include"analyze_fta.h"

#define LEFT 1
#define RIGHT 2

using namespace std;

static const char *fcn_for_op[] = {
	"+", "plus",
	"-", "minus",
	"|", "bitwise_or",
	"*", "multiply",
	"/", "divide",
	"&", "bitwise_and",
	"%", "modulo",
	">>", "shift_right",
	"<<", "shift_left",
	"AND", "AND",
	"OR", "OR",
	"=", "equal",
	">", "greater",
	"<", "less",
	">=", "greater_equal",
	"<=", "less_equal",
	"<>", "not_equal",
	"END"
};

static const char *fcn_for_unary_op[]= {
	"+", "unary_plus",
	"-", "unary_minus",
	"!", "integer_not",
	"~", "bitwise_not",
	"NOT", "logical_not",
	"END"
};

static string nic_fcn(string op){
int i = 0;
bool done = false;

  while(!done){
	if(string(fcn_for_op[i]) == string("END")){
		done = true;
		break;
	}
	if(string(fcn_for_op[i]) == op){
		return string(fcn_for_op[i+1]);
	}
	i++;
  }
  fprintf(stderr,"INTERNAL ERROR, operator %s not found in nic_fcn\n",op.c_str());
  return string("ERROR");
}


static string unary_nic_fcn(string op){
int i = 0;
bool done = false;

  while(!done){
	if(string(fcn_for_unary_op[i]) == string("END")){
		done = true;
		break;
	}
	if(string(fcn_for_unary_op[i]) == op){
		return string(fcn_for_unary_op[i+1]);
	}
	i++;
  }
  fprintf(stderr,"INTERNAL ERROR, operator %s not found in unary_nic_fcn\n",op.c_str());
  return string("ERROR");
}




//			Generate code that makes reference
//			to the tuple, and not to any aggregates.
static string generate_nic_se_code(scalarexp_t *se,
		map<string, string> &constants,
		int &copy_var_cnt,
		set<string> &no_copy_vars,
		string *retvar, int lr,
		gb_table *gbt  ){

	string ret = "";
	string l,cid;
    data_type *ldt, *rdt;
	int o;
	vector<scalarexp_t *> operands;
	string lvar, rvar;
	int gbref;

	string op = se->get_op();
//printf("op_type=%d op=%s\n",se->get_operator_type(),op.c_str());

	switch(se->get_operator_type()){
	case SE_LITERAL:
		l = se->get_literal()->to_query_string();
		if(constants.count(l)){
			cid = constants[l];
		}else{
			cid = "constant"+int_to_string(constants.size());
			constants[cid] = l;
		}
		*retvar = cid;
		if(lr == LEFT){
			*retvar = "copyvar_"+int_to_string(copy_var_cnt);
			copy_var_cnt++;
			ret += "load_with_copy("+*retvar+","+cid+")\n";
		}
		return(ret);
	case SE_PARAM:
		fprintf(stderr,"INTERNAL ERROR, param ref'd in generate_nic_se_code\n");
		*retvar = "error";
		return("ERROR in generate_nic_se_code");
	case SE_UNARY_OP:
		ret += generate_nic_se_code(se->get_left_se(),
			constants, copy_var_cnt, no_copy_vars, &lvar,LEFT,gbt);
		ret += unary_nic_fcn(op)+"("+lvar+")\n";
		*retvar = lvar;
		return(ret);
	case SE_BINARY_OP:
		ret += generate_nic_se_code(se->get_left_se(),
			constants, copy_var_cnt, no_copy_vars, &lvar,LEFT,gbt);
		ret += generate_nic_se_code(se->get_right_se(),
			constants, copy_var_cnt, no_copy_vars, &rvar,RIGHT,gbt);
		ret += nic_fcn(op)+"("+lvar+","+rvar+")\n";
		*retvar = lvar;
//printf("op %s; lvar=%s, rvar=%s, retvar=%s\n",op.c_str(), lvar.c_str(), rvar.c_str(), retvar->c_str());
		return(ret);
	case SE_COLREF:
		if(se->is_gb()){
			gbref = se->get_gb_ref();
/*
			if(gbt->get_reftype(gbref) != GBVAR_COLREF){
				fprintf(stderr,"INTERNAL ERROR, non-colref gbvar in generate_nic_se_code\n");
				exit(1);
			}
*/
			ret = generate_nic_se_code(gbt->get_def(gbref),constants,copy_var_cnt, no_copy_vars, &lvar,LEFT,gbt);
			*retvar = lvar;
			return ret;
		}
		if(lr == LEFT){
			*retvar = "copyvar_"+int_to_string(copy_var_cnt);
			copy_var_cnt++;
			ret += "load_with_copy("+*retvar+","+se->get_colref()->get_field()+")\n";
		}else{
			*retvar = "nocopyvar_"+se->get_colref()->get_field();
			if(no_copy_vars.count(*retvar) == 0){
				no_copy_vars.insert(*retvar);
				ret += "load("+*retvar+","+se->get_colref()->get_field()+")\n";
			}
		}
		return ret;

	case SE_FUNC:
		fprintf(stderr,"CONFIG ERROR, fcn reference in generate_nic_se_code, not yet implemented.\n");
		*retvar = "error";
		return(ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in generate_nic_se_code (lfta), line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		*retvar = "error";
		return("ERROR in generate_nic_se_code");
	}
}


static string generate_nic_predicate_code(predicate_t *pr,
		map<string, string> &constants,
		int &copy_var_cnt,
		set<string> &no_copy_vars,
		string *retvar, gb_table *gbt  ){
	string ret;
	vector<literal_t *>  litv;
	int i;
    data_type *ldt, *rdt;
	vector<scalarexp_t *> op_list;
	int o;
	string lvar, rvar;
	string l,cid;
	string op = pr->get_op();

	switch(pr->get_operator_type()){
	case PRED_IN:
		fprintf(stderr,"CONFIG ERROR: IN not supported in generate_nic_predicate_code\n");
		*retvar = "error";
		return("ERROR in generate_nic_predicate_code");

	case PRED_COMPARE:
		ret += generate_nic_se_code(pr->get_left_se(),
			constants, copy_var_cnt, no_copy_vars, &lvar,LEFT,gbt);
		ret += generate_nic_se_code(pr->get_right_se(),
			constants, copy_var_cnt, no_copy_vars, &rvar,RIGHT,gbt);
		ret += nic_fcn(op)+"("+lvar+","+rvar+")\n";
		*retvar = lvar;
		return(ret);
	case PRED_UNARY_OP:
		ret += generate_nic_predicate_code(pr->get_left_pr(),
			constants, copy_var_cnt, no_copy_vars, &lvar,gbt);
		ret += unary_nic_fcn(op)+"("+lvar+")\n";
		*retvar = lvar;
		return(ret);
	case PRED_BINARY_OP:
		ret += generate_nic_predicate_code(pr->get_left_pr(),
			constants, copy_var_cnt, no_copy_vars, &lvar,gbt);
		ret += generate_nic_predicate_code(pr->get_right_pr(),
			constants, copy_var_cnt, no_copy_vars, &rvar,gbt);
		ret += nic_fcn(op)+"("+lvar+","+rvar+")\n";
		*retvar = lvar;
		return(ret);
	case PRED_FUNC:
		fprintf(stderr,"CONFIG ERROR, fcn reference in generate_nic_pred_code, not yet implemented.\n");
		*retvar = "error";
		return(ret);
		ret += " )";
		return(ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in generate_predicate_nic_code, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		return("ERROR in generate_nic_predicate_code");
	}
}


bool se_is_nic_safe(scalarexp_t *se, nic_property *nicp, gb_table *gbt){
  string dts, colname;
  bool rs, ls;
  int gbref;

	switch(se->get_operator_type()){
	case SE_LITERAL:
		dts = se->get_data_type()->get_type_str();
		if(nicp->legal_type(dts))
			return true;
		return false;
		break;
	case SE_PARAM:
		return false;
		break;
	case SE_UNARY_OP:
		if(!se_is_nic_safe(se->get_left_se(), nicp, gbt)) return false;
		return(nicp->legal_unary_op(se->get_op()));
		break;
	case SE_BINARY_OP:
		if(!se_is_nic_safe(se->get_left_se(), nicp, gbt)) return false;
		if(!se_is_nic_safe(se->get_right_se(), nicp, gbt)) return false;
		return(nicp->legal_binary_op(se->get_op()));
		break;
	case SE_COLREF:
		if(se->is_gb()){
			gbref = se->get_gb_ref();
/*
			if(gbt->get_reftype(gbref) != GBVAR_COLREF){
				return false;
			}
*/
			return se_is_nic_safe(gbt->get_def(gbref),nicp,gbt);
		}
		dts = se->get_data_type()->get_type_str();
		if(! nicp->legal_type(dts))
			return false;
		colname = se->get_colref()->get_field();
		if(! nicp->illegal_field(colname))
			return true;
		return false;
		break;
	case SE_FUNC:
		return false;
    default:
		return false;
	}

	return false;
}


bool pr_is_nic_safe(predicate_t *pr, nic_property *nicp, gb_table *gbt){
  string dts, colname;
  bool rs, ls;
	switch(pr->get_operator_type()){
	case PRED_IN:
		return false;
	case PRED_COMPARE:
		if(!se_is_nic_safe(pr->get_left_se(), nicp, gbt)) return false;
		if(!se_is_nic_safe(pr->get_right_se(), nicp, gbt)) return false;
		return(nicp->legal_binary_op(pr->get_op()));
	case PRED_UNARY_OP:
		if(!pr_is_nic_safe(pr->get_left_pr(),nicp, gbt)) return false;
		return(nicp->legal_unary_op(pr->get_op()));
	case PRED_BINARY_OP:
		if(!pr_is_nic_safe(pr->get_left_pr(),nicp, gbt)) return false;
		if(!pr_is_nic_safe(pr->get_right_pr(),nicp, gbt)) return false;
		return(nicp->legal_binary_op(pr->get_op()));
	case PRED_FUNC:
		return false;
	default:
		return false;
	}

	return false;
}


void gather_nicsafe_cols(scalarexp_t *se, vector<string> &cols, nic_property *nicp, gb_table *gbt){
	int i, gbref;
	string colname;
	vector<scalarexp_t *> operands;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
		return;
	case SE_UNARY_OP:
		gather_nicsafe_cols(se->get_left_se(),cols,nicp, gbt);
		return;
	case SE_BINARY_OP:
		gather_nicsafe_cols(se->get_left_se(),cols,nicp, gbt);
		gather_nicsafe_cols(se->get_right_se(),cols,nicp, gbt);
		return;
	case SE_COLREF:
		if(se->is_gb()){
			gbref = se->get_gb_ref();
			return gather_nicsafe_cols(gbt->get_def(gbref),cols,nicp, gbt);
		}
		colname = se->get_colref()->get_field();
		if(! nicp->illegal_field(colname)){
		  for(i=0;i<cols.size();++i){
			if(colname == cols[i])
				break;
		  }
		  if(i==cols.size())
			cols.push_back(colname);
		}
		return;
	case SE_FUNC:
		operands = se->get_operands();
		for(i=0;i<operands.size();++i)
			gather_nicsafe_cols(operands[i],cols,nicp,gbt);
		return;
	default:
		return;
	}
	return;
}

void gather_nicsafe_cols(predicate_t *pr, vector<string> &cols, nic_property *nicp, gb_table *gbt){
  string dts, colname;
  bool rs, ls;
	vector<scalarexp_t *> operands;
	int i;

	switch(pr->get_operator_type()){
	case PRED_IN:
		gather_nicsafe_cols(pr->get_left_se(), cols, nicp, gbt);
		return ;
	case PRED_COMPARE:
		gather_nicsafe_cols(pr->get_left_se(), cols, nicp, gbt);
		gather_nicsafe_cols(pr->get_right_se(), cols, nicp, gbt);
		return;
	case PRED_UNARY_OP:
		gather_nicsafe_cols(pr->get_left_pr(),cols, nicp, gbt);
		return;
	case PRED_BINARY_OP:
		gather_nicsafe_cols(pr->get_left_pr(),cols, nicp, gbt);
		gather_nicsafe_cols(pr->get_right_pr(),cols, nicp, gbt);
		return;
	case PRED_FUNC:
		operands = pr->get_op_list();
		for(i=0;i<operands.size();++i)
			gather_nicsafe_cols(operands[i],cols,nicp, gbt);
		return ;
	default:
		return ;
	}

	return ;
}


string generate_nic_code(vector<stream_query *> lftas, nic_property *nicp){
  int i,p,s,g;
  string ret = "";
  string vars = "";
  gb_table *gbtbl;

  bool packed_return = false;

 	if(nicp->option_exists("Return")){
		if(nicp->option_value("Return") == "Packed"){
//printf("val of Return is %s\n",nicp->option_value("Return").c_str());
			packed_return = true;
		}else{
			fprintf(stderr,"Warning, nic option value of Return=%s is not recognized, ignoring\n",nicp->option_value("Return").c_str());
		}
	}

	vector<vector<predicate_t *> > nicsafe_preds;
	vector<predicate_t *> empty_list;
	for(i=0;i<lftas.size();i++){
		qp_node *qpn = lftas[i]->get_query_head();
		if(qpn->node_type() == "sgah_qpn"){
			gbtbl = ((sgah_qpn *)(qpn))->get_gb_tbl();
		}else{
			gbtbl = NULL;
		}
		nicsafe_preds.push_back(empty_list);
		vector<cnf_elem *> lfta_cnfs = qpn->get_filter_clause();
		for(p=0;p<lfta_cnfs.size();++p){
			if(pr_is_nic_safe(lfta_cnfs[p]->pr,nicp,gbtbl)){
				nicsafe_preds[i].push_back(lfta_cnfs[p]->pr);
			}
		}
	}

	map<string, string> consts;
	int n_copyvar = 0;
	set<string> nocopy_vars;
	string retvar;

	if(packed_return){
//printf("dping packed return\n");
		for(i=0;i<lftas.size();++i){
			qp_node *qpn = lftas[i]->get_query_head();
			ret += "comment( lfta "+qpn->get_node_name() + " )\n";
			if(qpn->node_type() == "sgah_qpn"){
				gbtbl = ((sgah_qpn *)(qpn))->get_gb_tbl();
			}else{
				gbtbl = NULL;
			}
			ret += "\nlabel(lfta"+int_to_string(i)+")\n";
			for(p=0;p<nicsafe_preds[i].size();++p){
				ret += generate_nic_predicate_code(nicsafe_preds[i][p],
						consts, n_copyvar, nocopy_vars, &retvar, gbtbl);
				ret += "jump_if_zero("+retvar+",lfta"+int_to_string(i+1)+")\n";
			}
			vector<scalarexp_t *> se_list;
			if(qpn->node_type() == "sgah_qpn"){
				se_list =((sgah_qpn *)(qpn))->get_select_se_list();
			}else{
				se_list =((spx_qpn *)(qpn))->get_select_se_list();
			}

			vector<string> refd_cols;
			for(s=0;s<se_list.size();++s){
				gather_nicsafe_cols(se_list[s],refd_cols, nicp, gbtbl);
			}
			vector<cnf_elem *> lfta_cnfs = qpn->get_where_clause();
			for(p=0;p<lfta_cnfs.size();++p){
				gather_nicsafe_cols(lfta_cnfs[p]->pr,refd_cols, nicp, gbtbl);
			}
			if(gbtbl){
				for(g=0;g<gbtbl->size();++g){
//printf("gathering gbvar %d\n",g);
				  gather_nicsafe_cols(gbtbl->get_def(g),refd_cols, nicp, gbtbl);
				}
			}
			sort(refd_cols.begin(), refd_cols.end());

			ret += "pack_and_send(const_id"+int_to_string(i);
			consts["const_id"+int_to_string(i)] = int_to_string(lftas[i]->get_gid());
			for(s=0;s<refd_cols.size();++s){
				ret += ",";
				ret += refd_cols[s];
			}
			ret += ")\n\n";
		}
		ret += "\nlabel(lfta"+int_to_string(i)+")\n";
	}else{
		for(i=0;i<lftas.size();++i){
			qp_node *qpn = lftas[i]->get_query_head();
			ret += "comment( lfta "+qpn->get_node_name() + " )\n";
			if(qpn->node_type() == "sgah_qpn"){
				gbtbl = ((sgah_qpn *)(qpn))->get_gb_tbl();
			}else{
				gbtbl = NULL;
			}
			ret += "\nlabel(lfta"+int_to_string(i)+")\n";
			for(p=0;p<nicsafe_preds[i].size();++p){
				ret += generate_nic_predicate_code(nicsafe_preds[i][p],
						consts, n_copyvar, nocopy_vars, &retvar, gbtbl);
				if(p<nicsafe_preds[i].size()-1){
					ret += "jump_if_zero("+retvar+",lfta"+int_to_string(i+1)+")\n\n";
				}else{
					ret += "jump_if_notzero("+retvar+",pass)\n\n";
				}
			}
		}
		ret += "label(pass)\n";
		ret += "send_packet()\n";
		ret += "\nlabel(lfta"+int_to_string(i)+")\n";
	}
    ret += "label(exit)\n";
	ret += "exit()\n";

	map<string, string>::iterator mssi;
	for(mssi=consts.begin();mssi!=consts.end();++mssi){
		vars += "constant( "+(*mssi).first+" , "+(*mssi).second+")\n";
	}
	for(i=0;i<n_copyvar;++i){
		vars += "variable( copyvar_"+int_to_string(i)+" )\n";
	}
	set<string>::iterator ssi;
	for(ssi=nocopy_vars.begin(); ssi!=nocopy_vars.end(); ++ssi){
		vars += "variable( "+(*ssi)+" )\n";
	}

	ret = "BEGIN\n"+vars + "\n"+ret + "END\n";

	return ret;

}


