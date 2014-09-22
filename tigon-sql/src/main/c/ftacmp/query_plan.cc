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

//		Create, manipulate, and dump query plans.

#include "query_plan.h"
#include "analyze_fta.h"
#include "generate_utils.h"

#include<vector>

using namespace std;

extern string hash_nums[NRANDS]; 	// for fast hashing


char tmpstr[1000];

void untaboo(string &s){
	int c;
	for(c=0;c<s.size();++c){
		if(s[c] == '.'){
			s[c] = '_';
		}
	}
}

//			mrg_qpn constructor, define here to avoid
//			circular references in the .h file
mrg_qpn::mrg_qpn(filter_join_qpn *spx, std::string n_name, std::vector<std::string> &sources, std::vector<std::pair<std::string, std::string> > &ifaces, ifq_t *ifdb){
		param_tbl = spx->param_tbl;
		int i;
		node_name = n_name;
		field_entry_list *fel = new field_entry_list();
		merge_fieldpos = -1;

		disorder = 1;

		for(i=0;i<spx->select_list.size();++i){
			data_type *dt = spx->select_list[i]->se->get_data_type()->duplicate();
			if(dt->is_temporal()){
				if(merge_fieldpos < 0){
					merge_fieldpos = i;
				}else{
					fprintf(stderr,"Warning: Merge subquery %s found two temporal fields (%s, %s), using %s\n", n_name.c_str(), spx->select_list[merge_fieldpos]->name.c_str(), spx->select_list[i]->name.c_str(), spx->select_list[merge_fieldpos]->name.c_str() );
					dt->reset_temporal();
				}
			}

			field_entry *fe = dt->make_field_entry(spx->select_list[i]->name);
			fel->append_field(fe);
			delete dt;
		}
		if(merge_fieldpos<0){
			fprintf(stderr,"ERROR, no temporal attribute for merge subquery %s\n",n_name.c_str());
				exit(1);
		}
		table_layout = new table_def( n_name.c_str(), NULL, NULL, fel, STREAM_SCHEMA);

//				NEED TO HANDLE USER_SPECIFIED SLACK
		this->resolve_slack(spx->select_list[merge_fieldpos]->se,
				spx->select_list[merge_fieldpos]->name, ifaces, ifdb,NULL);
//	if(this->slack == NULL)
//		fprintf(stderr,"Zero slack.\n");
//	else
//		fprintf(stderr,"slack is %s\n",slack->to_string().c_str());

		for(i=0;i<sources.size();i++){
			std::string rvar = "_m"+int_to_string(i);
			mvars.push_back(new colref_t(rvar.c_str(), spx->select_list[merge_fieldpos]->name.c_str()));
			mvars[i]->set_tablevar_ref(i);
			fm.push_back(new tablevar_t(sources[i].c_str()));
			fm[i]->set_range_var(rvar);
		}

		param_tbl = new param_table();
		std::vector<std::string> param_names = spx->param_tbl->get_param_names();
		int pi;
		for(pi=0;pi<param_names.size();pi++){
			data_type *dt = spx->param_tbl->get_data_type(param_names[pi]);
			param_tbl->add_param(param_names[pi],dt->duplicate(),
							spx->param_tbl->handle_access(param_names[pi]));
		}
		definitions = spx->definitions;

}



//		This function translates an analyzed parse tree
//		into one or more query nodes (qp_node).
//		Currently only one node is created, but some query
//		fragments might create more than one query node,
//		e.g. aggregation over a joim, or nested subqueries
//		in the FROM clause (unless this is handles at parse tree
//		analysis time).  At this stage, they will be linked
//		by the names in the FROM clause.
//		INVARIANT : if mroe than one query node is returned,
//		the last one represents the output of the query.
vector<qp_node *> create_query_nodes(query_summary_class *qs,table_list *Schema){

//		Classify the query.

	vector <qp_node *> local_plan;
	qp_node *plan_root;

//			TODO
//			I should probably move a lot of this code
//			into the qp_node constructors,
//			and have this code focus on building the query plan tree.

//		MERGE node
	if(qs->query_type == MERGE_QUERY){
		mrg_qpn *merge_node = new mrg_qpn(qs,Schema);

//			Done
		plan_root = merge_node;
		local_plan.push_back(merge_node);

		/*
		Do not split sources until we are done with optimizations
		vector<mrg_qpn *> split_merge = merge_node->split_sources();
		local_plan.insert(local_plan.begin(), split_merge.begin(), split_merge.end());
		*/
//			If children are created, add them to the schema.
/*
		int i;
printf("split_merge size is %d\n",split_merge.size());
		for(i=1;i<split_merge.size();++i){
			Schema->add_table(split_merge[i]->get_fields());
printf("Adding split merge table %d\n",i);
		}
*/

/*
printf("Did split sources on %s:\n",qs->query_name.c_str());
int ss;
for(ss=0;ss<local_plan.size();ss++){
printf("node %d, name=%s, sources=",ss,local_plan[ss]->get_node_name().c_str());
vector<tablevar_t *> inv = local_plan[ss]->get_input_tbls();
int nn;
for(nn=0;nn<inv.size();nn++){
printf("%s ",inv[nn]->to_string().c_str());
}
printf("\n");
}
*/


	} else{

//		Select / Aggregation / Join
	  if(qs->gb_tbl->size() == 0 && qs->aggr_tbl->size() == 0){

		if(qs->fta_tree->get_from()->size() == 1){
			spx_qpn *spx_node = new spx_qpn(qs,Schema);

			plan_root = spx_node;
			local_plan.push_back(spx_node);
		}else{
			if(qs->fta_tree->get_from()->get_properties() == FILTER_JOIN_PROPERTY){
				filter_join_qpn *join_node = new filter_join_qpn(qs,Schema);
				plan_root = join_node;
				local_plan.push_back(join_node);
			}else{
				join_eq_hash_qpn *join_node = new join_eq_hash_qpn(qs,Schema);
				plan_root = join_node;
				local_plan.push_back(join_node);
			}
		}
	  }else{
//			aggregation

		if(qs->states_refd.size() || qs->sg_tbl.size() || qs->cb_cnf.size()){
			sgahcwcb_qpn *sgahcwcb_node = new sgahcwcb_qpn(qs,Schema);
			plan_root = sgahcwcb_node;
			local_plan.push_back(sgahcwcb_node);
	  	}else{
			if(qs->closew_cnf.size()){
				rsgah_qpn *rsgah_node = new rsgah_qpn(qs,Schema);
				plan_root = rsgah_node;
				local_plan.push_back(rsgah_node);
			}else{
				sgah_qpn *sgah_node = new sgah_qpn(qs,Schema);
				plan_root = sgah_node;
				local_plan.push_back(sgah_node);
			}
		}
	  }
	}


//		Get the query name and other definitions.
	plan_root->set_node_name( qs->query_name);
	plan_root->set_definitions( qs->definitions) ;


//	return(plan_root);
	return(local_plan);

}


string se_to_query_string(scalarexp_t *se, aggregate_table *aggr_tbl){
  string l_str;
  string r_str;
  string ret;
  int p;
  vector<scalarexp_t *> operand_list;
  string su_ind = "";

  if(se->is_superaggr())
	su_ind = "$";

  switch(se->get_operator_type()){
    case SE_LITERAL:
		l_str = se->get_literal()->to_query_string();
		return l_str;
    case SE_PARAM:
		l_str = "$" + se->get_op();
		return l_str;
    case SE_COLREF:
		l_str =  se->get_colref()->to_query_string() ;
		return l_str;
    case SE_UNARY_OP:
		 l_str = se_to_query_string(se->get_left_se(),aggr_tbl);

		return se->get_op()+"( "+l_str+" )";;
    case SE_BINARY_OP:
		l_str = se_to_query_string(se->get_left_se(),aggr_tbl);
		r_str = se_to_query_string(se->get_right_se(),aggr_tbl);
		return( "("+l_str+")"+se->get_op()+"("+r_str+")" );
    case SE_AGGR_STAR:
		return( se->get_op() + su_ind + "(*)");
    case SE_AGGR_SE:
		l_str = se_to_query_string(aggr_tbl->get_aggr_se(se->get_aggr_ref()),aggr_tbl);
		return( se->get_op() + su_ind + "(" + l_str + ")" );
	case SE_FUNC:
		if(se->get_aggr_ref() >= 0)
			operand_list = aggr_tbl->get_operand_list(se->get_aggr_ref());
		else
			operand_list = se->get_operands();

		ret = se->get_op() + su_ind + "(";
		for(p=0;p<operand_list.size();p++){
			l_str = se_to_query_string(operand_list[p],aggr_tbl);
			if(p>0) ret += ", ";
			ret += l_str;
		}
		ret += ")";
		return(ret);
	break;
  }
  return "ERROR SE op type not recognized in se_to_query_string.\n";
}


string pred_to_query_str(predicate_t *pr, aggregate_table *aggr_tbl){
  string l_str;
  string r_str;
  string ret;
  int o,l;
  vector<literal_t *> llist;
  vector<scalarexp_t *> op_list;

	switch(pr->get_operator_type()){
	case PRED_IN:
		l_str = se_to_query_string(pr->get_left_se(),aggr_tbl);
		ret = l_str + " IN [";
		llist = pr->get_lit_vec();
		for(l=0;l<llist.size();l++){
			if(l>0) ret += ", ";
			ret += llist[l]->to_query_string();
		}
		ret += "]";

		return(ret);
	case PRED_COMPARE:
		l_str = se_to_query_string(pr->get_left_se(),aggr_tbl);
		r_str = se_to_query_string(pr->get_right_se(),aggr_tbl);
		return( l_str + " " + pr->get_op() + " " + r_str );
	case PRED_UNARY_OP:
		l_str = pred_to_query_str(pr->get_left_pr(),aggr_tbl);
		return(pr->get_op() + "( " + l_str + " )");
	case PRED_BINARY_OP:
		l_str = pred_to_query_str(pr->get_left_pr(),aggr_tbl);
		r_str = pred_to_query_str(pr->get_right_pr(),aggr_tbl);
		return("( " + r_str + " )" + pr->get_op() + "( " + l_str + " )");
	case PRED_FUNC:
		ret = pr->get_op()+"[";
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			if(o>0) ret += ", ";
			ret += se_to_query_string(op_list[o],aggr_tbl);
		}
		ret += "]";
		return(ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in pred_to_query_str, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return(0);
}



//			Build a selection list,
//			but avoid adding duplicate SEs.


int add_select_list_nodup(vector<select_element *> &lfta_select_list, scalarexp_t *se,
				bool &new_element){
	new_element = false;
	int s;
	for(s=0;s<lfta_select_list.size();s++){
		if(is_equivalent_se(lfta_select_list[s]->se, se)){
			return(s);
		}
	}
	new_element = true;
	lfta_select_list.push_back(new select_element(se,"NoNameIn:add_select_list_nodup"));
	return(lfta_select_list.size()-1);
}



//		TODO: The generated colref should be tied to the tablevar
//		representing the lfta output.  For now, always 0.

scalarexp_t *make_fta_se_ref(vector<select_element *> &lfta_select_list, scalarexp_t *se, int h_tvref){
	bool new_element;
	int fta_se_nbr = add_select_list_nodup(lfta_select_list, se, new_element);
	string colname;
	if(!new_element){
		colname = lfta_select_list[fta_se_nbr]->name;
	}else{
		colname = impute_colname(lfta_select_list, se);
		lfta_select_list[fta_se_nbr]->name = colname;
	}
//
//		TODO: fill in the tablevar and schema of the colref here.
	colref_t *new_cr = new colref_t(colname.c_str());
	new_cr->set_tablevar_ref(h_tvref);


	scalarexp_t *new_se= new scalarexp_t(new_cr);
	new_se->use_decorations_of(se);

	return(new_se);
}


//			Build a selection list,
//			but avoid adding duplicate SEs.


int add_select_list_nodup(vector<select_element *> *lfta_select_list, scalarexp_t *se,
				bool &new_element){
	new_element = false;
	int s;
	for(s=0;s<lfta_select_list->size();s++){
		if(is_equivalent_se((*lfta_select_list)[s]->se, se)){
			return(s);
		}
	}
	new_element = true;
	lfta_select_list->push_back(new select_element(se,"NoNameIn:add_select_list_nodup"));
	return(lfta_select_list->size()-1);
}



//		TODO: The generated colref should be tied to the tablevar
//		representing the lfta output.  For now, always 0.

scalarexp_t *make_fta_se_ref(vector<vector<select_element *> *> &lfta_select_list, scalarexp_t *se, int h_tvref){
	bool new_element;
    vector<select_element *> *the_sel_list = lfta_select_list[h_tvref];
	int fta_se_nbr = add_select_list_nodup(the_sel_list, se, new_element);
	string colname;
	if(!new_element){
		colname = (*the_sel_list)[fta_se_nbr]->name;
	}else{
		colname = impute_colname(*the_sel_list, se);
		(*the_sel_list)[fta_se_nbr]->name = colname;
	}
//
//		TODO: fill in the tablevar and schema of the colref here.
	colref_t *new_cr = new colref_t(colname.c_str());
	new_cr->set_tablevar_ref(h_tvref);


	scalarexp_t *new_se= new scalarexp_t(new_cr);
	new_se->use_decorations_of(se);

	return(new_se);
}




//
//			Test if a se can be evaluated at the fta.
//			check forbidden types (e.g. float), forbidden operations
//			between types (e.g. divide a long long), forbidden operations
//			(too expensive, not implemented).
//
//			Return true if not forbidden, false if forbidden
//
//			TODO: the parameter aggr_tbl is not used, delete it.

bool check_fta_forbidden_se(scalarexp_t *se,
						 aggregate_table *aggr_tbl,
						 ext_fcn_list *Ext_fcns
						 ){

  int p, fcn_id;
  vector<scalarexp_t *> operand_list;
  vector<data_type *> dt_signature;
  data_type *dt = se->get_data_type();



  switch(se->get_operator_type()){
    case SE_LITERAL:
    case SE_PARAM:
    case SE_COLREF:
		return( se->get_data_type()->fta_legal_type() );
	case SE_IFACE_PARAM:
		return true;
    case SE_UNARY_OP:
		if(!check_fta_forbidden_se(se->get_left_se(), aggr_tbl, Ext_fcns))
			 return(false);
		return(
		   dt->fta_legal_operation(se->get_left_se()->get_data_type(), se->get_op())
		);
    case SE_BINARY_OP:
		 if(!check_fta_forbidden_se(se->get_left_se(),aggr_tbl, Ext_fcns))
			 return(false);
		 if(!check_fta_forbidden_se(se->get_right_se(),aggr_tbl, Ext_fcns))
			 return(false);
		 return(dt->fta_legal_operation(se->get_left_se()->get_data_type(),
									se->get_right_se()->get_data_type(),
									se->get_op()
									)
		);

//			return true, aggregate fta-safeness is determined elsewhere.
    case SE_AGGR_STAR:
		return(true);
    case SE_AGGR_SE:
		return(true);

	case SE_FUNC:
		if(se->get_aggr_ref() >= 0) return true;

		operand_list = se->get_operands();
		for(p=0;p<operand_list.size();p++){
			if(!check_fta_forbidden_se(operand_list[p],aggr_tbl, Ext_fcns))
				return(false);
			dt_signature.push_back(operand_list[p]->get_data_type() );
		}
		fcn_id = Ext_fcns->lookup_fcn(se->get_op(), dt_signature);
		if( fcn_id < 0 ){
			fprintf(stderr,"ERROR, no external function %s(",se->get_op().c_str());
			int o;
			for(o=0;o<operand_list.size();o++){
				if(o>0) fprintf(stderr,", ");
				fprintf(stderr,"%s",operand_list[o]->get_data_type()->to_string().c_str());
			}
			fprintf(stderr,") is defined, line %d, char %d\n", se->get_lineno(), se->get_charno() );
			if(fcn_id == -2) fprintf(stderr,"(multiple subsuming functions found)\n");
			return(false);
		}

		return(Ext_fcns->fta_legal(fcn_id) );
	default:
		printf("INTERNAL ERROR in check_fta_forbidden_se: operator type %d\n",se->get_operator_type());
		exit(1);
	break;
  }
  return(false);

}


//		test if a pr can be executed at the fta.
//
//			Return true if not forbidden, false if forbidden

bool check_fta_forbidden_pr(predicate_t *pr,
						 aggregate_table *aggr_tbl,
						 ext_fcn_list *Ext_fcns
						 ){

  vector<literal_t *> llist;
  data_type *dt;
  int l,o, fcn_id;
  vector<scalarexp_t *> op_list;
  vector<data_type *> dt_signature;



	switch(pr->get_operator_type()){
	case PRED_IN:
		if(! check_fta_forbidden_se(pr->get_left_se(), aggr_tbl, Ext_fcns) )
			return(false);
		llist = pr->get_lit_vec();
		for(l=0;l<llist.size();l++){
			dt = new data_type(llist[l]->get_type());
			if(! dt->fta_legal_type()){
				delete dt;
				return(false);
			}
			delete dt;
		}
		return(true);
	case PRED_COMPARE:
		if(! check_fta_forbidden_se(pr->get_left_se(), aggr_tbl, Ext_fcns))
			return(false);
		if(! check_fta_forbidden_se(pr->get_right_se(), aggr_tbl, Ext_fcns))
			return(false);
		return(true);
	case PRED_UNARY_OP:
		return( check_fta_forbidden_pr(pr->get_left_pr(), aggr_tbl, Ext_fcns) );
	case PRED_BINARY_OP:
		if(! check_fta_forbidden_pr(pr->get_left_pr(), aggr_tbl, Ext_fcns))
			return(false);
		if(! check_fta_forbidden_pr(pr->get_right_pr(), aggr_tbl, Ext_fcns))
			return(false);
		return(true);
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();o++){
			if(!check_fta_forbidden_se(op_list[o],aggr_tbl, Ext_fcns))
				return(false);
			dt_signature.push_back(op_list[o]->get_data_type() );
		}
		fcn_id = Ext_fcns->lookup_pred(pr->get_op(), dt_signature);
		if( fcn_id < 0 ){
			fprintf(stderr,"ERROR, no external predicate %s(",pr->get_op().c_str());
			int o;
			for(o=0;o<op_list.size();o++){
				if(o>0) fprintf(stderr,", ");
				fprintf(stderr,"%s",op_list[o]->get_data_type()->to_string().c_str());
			}
			fprintf(stderr,") is defined, line %d, char %d\n", pr->get_lineno(), pr->get_charno() );
			if(fcn_id == -2) fprintf(stderr,"(multiple subsuming predicates found)\n");
			return(false);
		}

		return(Ext_fcns->fta_legal(fcn_id) );
	default:
		fprintf(stderr,"INTERNAL ERROR in check_fta_forbidden_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return(0);

}


//		Split the aggregates in orig_aggr_tbl, into superaggregates and
//		subaggregates.
//		(the value of the HFTA aggregate might be a SE of several LFTA
//		 subaggregates, e.g. avg : sum / count )
//		Register the superaggregates in hfta_aggr_tbl, and the
//		subaggregates in lfta_aggr_tbl.
//		Insert references to the subaggregates into lfta_select_list.
//		(and record their names in the currnames list)
//		Create a SE for the superaggregate, put it in hfta_aggr_se,
//		keyed on agr_id.

void split_fta_aggr(aggregate_table *orig_aggr_tbl, int agr_id,
					aggregate_table *hfta_aggr_tbl,
					aggregate_table *lfta_aggr_tbl,
					vector<select_element *> &lfta_select_list,
					map<int,scalarexp_t *> &hfta_aggr_se,
				    ext_fcn_list *Ext_fcns
					){
	bool new_element;
	scalarexp_t *subaggr_se;
	int fta_se_nbr;
	string colname;
	int ano;
	colref_t *new_cr;
	scalarexp_t *new_se, *l_se;
	vector<scalarexp_t *> subaggr_ref_se;

//		UDAF processing
	if(! orig_aggr_tbl->is_builtin(agr_id)){
//			Construct the subaggregate
		int fcn_id = orig_aggr_tbl->get_fcn_id(agr_id);
		vector<scalarexp_t *> opl = orig_aggr_tbl->get_operand_list(agr_id);
		vector<scalarexp_t *> subopl;
		int o;
		for(o=0;o<opl.size();++o){
			subopl.push_back(dup_se(opl[o], NULL));
		}
		int sub_id = Ext_fcns->get_subaggr_id(fcn_id);
		subaggr_se = new scalarexp_t(Ext_fcns->get_fcn_name(sub_id).c_str(), subopl);
		subaggr_se->set_fcn_id(sub_id);
		subaggr_se->set_data_type(Ext_fcns->get_fcn_dt(sub_id));
//			Add it to the lfta select list.
		fta_se_nbr = add_select_list_nodup(lfta_select_list, subaggr_se,new_element);
		if(!new_element){
			colname = lfta_select_list[fta_se_nbr]->name;
		}else{
			colname = impute_colname(lfta_select_list, subaggr_se);
			lfta_select_list[fta_se_nbr]->name = colname;
			ano = lfta_aggr_tbl->add_aggr(Ext_fcns->get_fcn_name(sub_id), sub_id, subopl,Ext_fcns->get_storage_dt(sub_id), false, false,Ext_fcns->has_lfta_bailout(sub_id));
			subaggr_se->set_aggr_id(ano);
		}

//			Construct a reference to the subaggregate
		new_cr = new colref_t(colname.c_str());
		new_se = new scalarexp_t(new_cr);
//				I'm not certain what the types should be ....
//				This will need to be filled in by later analysis.
//				NOTE: this might not capture all the meaning of data_type ...
		new_se->set_data_type(Ext_fcns->get_fcn_dt(sub_id));
		subaggr_ref_se.push_back(new_se);

//			Construct the superaggregate
		int super_id = Ext_fcns->get_superaggr_id(fcn_id);
		scalarexp_t *ret_se = new scalarexp_t(Ext_fcns->get_fcn_name(super_id).c_str(), subaggr_ref_se);
		ret_se->set_fcn_id(super_id);
		ret_se->set_data_type(Ext_fcns->get_fcn_dt(super_id));
//			Register it in the hfta aggregate table
		ano = hfta_aggr_tbl->add_aggr(ret_se->get_op(), super_id, subaggr_ref_se,Ext_fcns->get_storage_dt(super_id), false, Ext_fcns->is_running_aggr(sub_id),false);
		ret_se->set_aggr_id(ano);
		hfta_aggr_se[agr_id] = ret_se;

		return;
	}


//		builtin aggregate processing
	bool l_forbid;

	vector<bool> use_se;
	vector<string> subaggr_names = orig_aggr_tbl->get_subaggr_fcns(agr_id, use_se);
	vector<data_type *> subaggr_dt = orig_aggr_tbl->get_subaggr_dt(agr_id);
	int sa;

	if(orig_aggr_tbl->is_star_aggr(agr_id)){
	  for(sa=0;sa<subaggr_names.size();sa++){
		subaggr_se = scalarexp_t::make_star_aggr(subaggr_names[sa].c_str());
		subaggr_se->set_data_type(subaggr_dt[sa]);

//			The following sequence is similar to the code in make_fta_se_ref,
//			but there is special processing for the aggregate tables.
		int fta_se_nbr = add_select_list_nodup(lfta_select_list, subaggr_se,new_element);
		if(!new_element){
			colname = lfta_select_list[fta_se_nbr]->name;
		}else{
			colname = impute_colname(lfta_select_list, subaggr_se);
			lfta_select_list[fta_se_nbr]->name = colname;
			ano = lfta_aggr_tbl->add_aggr(subaggr_names[sa].c_str(),NULL, false);
			subaggr_se->set_aggr_id(ano);
		}
		new_cr = new colref_t(colname.c_str());
		new_cr->set_tablevar_ref(0);
		new_se = new scalarexp_t(new_cr);

//					I'm not certain what the types should be ....
//					This will need to be filled in by later analysis.
//						Actually, this is causing a problem.
//						I will assume a UINT data type. / change to INT
//						(consistent with assign_data_types in analyze_fta.cc)
//			TODO: why can't I use subaggr_dt, as I do in the other IF branch?
		data_type *ndt = new data_type("Int");	// used to be Uint
		new_se->set_data_type(ndt);

		subaggr_ref_se.push_back(new_se);
	  }
	}else{
	  for(sa=0;sa<subaggr_names.size();sa++){
		if(use_se[sa]){
			scalarexp_t *aggr_operand = orig_aggr_tbl->get_aggr_se(agr_id);
			l_se = dup_se(aggr_operand,  NULL);
			subaggr_se = scalarexp_t::make_se_aggr(subaggr_names[sa].c_str(),l_se);
		}else{
			subaggr_se = scalarexp_t::make_star_aggr(subaggr_names[sa].c_str());
		}
		subaggr_se->set_data_type(subaggr_dt[sa]);

//			again, similar to make_fta_se_ref.
		fta_se_nbr = add_select_list_nodup(lfta_select_list, subaggr_se,new_element);
		if(!new_element){
			colname = lfta_select_list[fta_se_nbr]->name;
		}else{
			colname = impute_colname(lfta_select_list, subaggr_se);
			lfta_select_list[fta_se_nbr]->name = colname;
			if(use_se[sa])
				ano = lfta_aggr_tbl->add_aggr(subaggr_names[sa].c_str(),l_se, false);
			else
				ano = lfta_aggr_tbl->add_aggr(subaggr_names[sa].c_str(),NULL,false);
			subaggr_se->set_aggr_id(ano);
		}
		new_cr = new colref_t(colname.c_str());
		new_se = new scalarexp_t(new_cr);
//				I'm not certain what the types should be ....
//				This will need to be filled in by later analysis.
//				NOTE: this might not capture all the meaning of data_type ...
		new_se->set_data_type(subaggr_dt[sa]);
		subaggr_ref_se.push_back(new_se);
	  }
	}
	scalarexp_t *ret_se = orig_aggr_tbl->make_superaggr_se(agr_id, subaggr_ref_se);
	ret_se->set_data_type(orig_aggr_tbl->get_data_type(agr_id));
	ano = hfta_aggr_tbl->add_aggr(ret_se->get_op(), ret_se->get_left_se(), false );
	ret_se->set_aggr_id(ano);
	hfta_aggr_se[agr_id] = ret_se;

}


//		Split the aggregates in orig_aggr_tbl, into hfta_superaggregates and
//		hfta_subaggregates.
//		Register the superaggregates in hi_aggr_tbl, and the
//		subaggregates in loq_aggr_tbl.
//		Insert references to the subaggregates into low_select_list.
//		(and record their names in the currnames list)
//		Create a SE for the superaggregate, put it in hfta_aggr_se,
//		keyed on agr_id.

void split_hfta_aggr(aggregate_table *orig_aggr_tbl, int agr_id,
					aggregate_table *hi_aggr_tbl,
					aggregate_table *low_aggr_tbl,
					vector<select_element *> &low_select_list,
					map<int,scalarexp_t *> &hi_aggr_se,
				    ext_fcn_list *Ext_fcns
					){
	bool new_element;
	scalarexp_t *subaggr_se;
	int fta_se_nbr;
	string colname;
	int ano;
	colref_t *new_cr;
	scalarexp_t *new_se, *l_se;
	vector<scalarexp_t *> subaggr_ref_se;

//		UDAF processing
	if(! orig_aggr_tbl->is_builtin(agr_id)){
//			Construct the subaggregate
		int fcn_id = orig_aggr_tbl->get_fcn_id(agr_id);
		vector<scalarexp_t *> opl = orig_aggr_tbl->get_operand_list(agr_id);
		vector<scalarexp_t *> subopl;
		int o;
		for(o=0;o<opl.size();++o){
			subopl.push_back(dup_se(opl[o], NULL));
		}
		int sub_id = Ext_fcns->get_hfta_subaggr_id(fcn_id);
		subaggr_se = new scalarexp_t(Ext_fcns->get_fcn_name(sub_id).c_str(), subopl);
		subaggr_se->set_fcn_id(sub_id);
		subaggr_se->set_data_type(Ext_fcns->get_fcn_dt(sub_id));
//			Add it to the low select list.
		fta_se_nbr = add_select_list_nodup(low_select_list, subaggr_se,new_element);
		if(!new_element){
			colname = low_select_list[fta_se_nbr]->name;
		}else{
			colname = impute_colname(low_select_list, subaggr_se);
			low_select_list[fta_se_nbr]->name = colname;
			ano = low_aggr_tbl->add_aggr(Ext_fcns->get_fcn_name(sub_id), sub_id, subopl,Ext_fcns->get_storage_dt(sub_id), false, false,false);
			subaggr_se->set_aggr_id(ano);
		}

//			Construct a reference to the subaggregate
		new_cr = new colref_t(colname.c_str());
		new_se = new scalarexp_t(new_cr);
//				I'm not certain what the types should be ....
//				This will need to be filled in by later analysis.
//				NOTE: this might not capture all the meaning of data_type ...
		new_se->set_data_type(Ext_fcns->get_fcn_dt(sub_id));
		subaggr_ref_se.push_back(new_se);

//			Construct the superaggregate
		int super_id = Ext_fcns->get_hfta_superaggr_id(fcn_id);
		scalarexp_t *ret_se = new scalarexp_t(Ext_fcns->get_fcn_name(super_id).c_str(), subaggr_ref_se);
		ret_se->set_fcn_id(super_id);
		ret_se->set_data_type(Ext_fcns->get_fcn_dt(super_id));
//			Register it in the high aggregate table
		ano = hi_aggr_tbl->add_aggr(ret_se->get_op(), super_id, subaggr_ref_se,Ext_fcns->get_storage_dt(super_id), false, false,false);
		ret_se->set_aggr_id(ano);
		hi_aggr_se[agr_id] = ret_se;

		return;
	}


//		builtin aggregate processing
	bool l_forbid;

	vector<bool> use_se;
	vector<string> subaggr_names = orig_aggr_tbl->get_subaggr_fcns(agr_id, use_se);
	vector<data_type *> subaggr_dt = orig_aggr_tbl->get_subaggr_dt(agr_id);
	int sa;

	if(orig_aggr_tbl->is_star_aggr(agr_id)){
	  for(sa=0;sa<subaggr_names.size();sa++){
		subaggr_se = scalarexp_t::make_star_aggr(subaggr_names[sa].c_str());
		subaggr_se->set_data_type(subaggr_dt[sa]);

//			The following sequence is similar to the code in make_fta_se_ref,
//			but there is special processing for the aggregate tables.
		int fta_se_nbr = add_select_list_nodup(low_select_list, subaggr_se,new_element);
		if(!new_element){
			colname = low_select_list[fta_se_nbr]->name;
		}else{
			colname = impute_colname(low_select_list, subaggr_se);
			low_select_list[fta_se_nbr]->name = colname;
			ano = low_aggr_tbl->add_aggr(subaggr_names[sa].c_str(),NULL, false);
			subaggr_se->set_aggr_id(ano);
		}
		new_cr = new colref_t(colname.c_str());
		new_cr->set_tablevar_ref(0);
		new_se = new scalarexp_t(new_cr);

//					I'm not certain what the types should be ....
//					This will need to be filled in by later analysis.
//						Actually, this is causing a problem.
//						I will assume a UINT data type.
//						(consistent with assign_data_types in analyze_fta.cc)
//			TODO: why can't I use subaggr_dt, as I do in the other IF branch?
		data_type *ndt = new data_type("Int");	// was Uint
		new_se->set_data_type(ndt);

		subaggr_ref_se.push_back(new_se);
	  }
	}else{
	  for(sa=0;sa<subaggr_names.size();sa++){
		if(use_se[sa]){
			scalarexp_t *aggr_operand = orig_aggr_tbl->get_aggr_se(agr_id);
			l_se = dup_se(aggr_operand,  NULL);
			subaggr_se = scalarexp_t::make_se_aggr(subaggr_names[sa].c_str(),l_se);
		}else{
			subaggr_se = scalarexp_t::make_star_aggr(subaggr_names[sa].c_str());
		}
		subaggr_se->set_data_type(subaggr_dt[sa]);

//			again, similar to make_fta_se_ref.
		fta_se_nbr = add_select_list_nodup(low_select_list, subaggr_se,new_element);
		if(!new_element){
			colname = low_select_list[fta_se_nbr]->name;
		}else{
			colname = impute_colname(low_select_list, subaggr_se);
			low_select_list[fta_se_nbr]->name = colname;
			if(use_se[sa])
				ano = low_aggr_tbl->add_aggr(subaggr_names[sa].c_str(),l_se, false);
			else
				ano = low_aggr_tbl->add_aggr(subaggr_names[sa].c_str(),NULL,false);
			subaggr_se->set_aggr_id(ano);
		}
		new_cr = new colref_t(colname.c_str());
		new_se = new scalarexp_t(new_cr);
//				I'm not certain what the types should be ....
//				This will need to be filled in by later analysis.
//				NOTE: this might not capture all the meaning of data_type ...
		new_se->set_data_type(subaggr_dt[sa]);
		subaggr_ref_se.push_back(new_se);
	  }
	}
	scalarexp_t *ret_se = orig_aggr_tbl->make_superaggr_se(agr_id, subaggr_ref_se);
	ret_se->set_data_type(orig_aggr_tbl->get_data_type(agr_id));
	ano = hi_aggr_tbl->add_aggr(ret_se->get_op(), ret_se->get_left_se(), false );
	ret_se->set_aggr_id(ano);
	hi_aggr_se[agr_id] = ret_se;

}





//		Split a scalar expression into one part which executes
//		at the stream and another set of parts which execute
//		at the FTA.
//		Because I'm actually modifying the SEs, I will make
//		copies.  But I will assume that literals, params, and
//		colrefs are immutable at this point.
//		(if there is ever a need to change one, must make a
//		 new value).
//		NOTE : if se is constant (only refrences literals),
//			avoid making the fta compute it.
//
//		NOTE : This will need to be generalized to
//		handle join expressions, namely to handle a vector
//		of lftas.
//
//		Return value is the HFTA se.
//		Add lftas select_elements to the fta_select_list.
//		set fta_forbidden if this node or any child cannot
//		execute at the lfta.

/*

scalarexp_t *split_fta_se(scalarexp_t *se,
				  bool &fta_forbidden,
				  vector<select_element *> &lfta_select_list,
				  ext_fcn_list *Ext_fcns
				 ){

  int p, fcn_id;
  vector<scalarexp_t *> operand_list;
  vector<data_type *> dt_signature;
  scalarexp_t *ret_se, *l_se, *r_se;
  bool l_forbid, r_forbid, this_forbid;
  colref_t *new_cr;
  scalarexp_t *new_se;
  data_type *dt = se->get_data_type();

  switch(se->get_operator_type()){
    case SE_LITERAL:
		fta_forbidden = ! se->get_data_type()->fta_legal_type();
		ret_se = new scalarexp_t(se->get_literal());
		ret_se->use_decorations_of(se);
		return(ret_se);

    case SE_PARAM:
		fta_forbidden = ! se->get_data_type()->fta_legal_type();
		ret_se = scalarexp_t::make_param_reference(se->get_op().c_str());
		ret_se->use_decorations_of(se);
		return(ret_se);

    case SE_COLREF:
//			No colref should be forbidden,
//			the schema is wrong, the fta_legal_type() fcn is wrong,
//			or the source table is actually a stream.
//			Issue a warning, but proceed with processing.
//			Also, should not be a ref to a gbvar.
//			(a gbvar ref only occurs in an aggregation node,
//			and these SEs are rehomed, not split.
		fta_forbidden = ! se->get_data_type()->fta_legal_type();

		if(fta_forbidden){
			fprintf(stderr,"WARNING, a colref is a forbidden data type in split_fta_se,"
							" colref is %s,"
							" type is %s, line=%d, col=%d\n",
							se->get_colref()->to_string().c_str(),
							se->get_data_type()->get_type_str().c_str(),
							se->lineno, se->charno
					);
		}

		if(se->is_gb()){
			fprintf(stderr,"INTERNAL ERROR, a colref is a gbvar ref in split_fta_se,"
							" type is %s, line=%d, col=%d\n",
							se->get_data_type()->get_type_str().c_str(),
							se->lineno, se->charno
					);
			exit(1);
		}

		ret_se = new scalarexp_t(se->get_colref());
		ret_se->use_decorations_of(se);
		return(ret_se);

    case SE_UNARY_OP:
		 l_se = split_fta_se(se->get_left_se(), l_forbid, lfta_select_list, Ext_fcns);

		 this_forbid = ! dt->fta_legal_operation(l_se->get_data_type(), se->get_op());

//			If this operation is forbidden but the child SE is not,
//			put the child se on the lfta_select_list, create a colref
//			which accesses this se, and make it the child of this op.
//			Exception : the child se is constant (only literal refs).
		 if(this_forbid && !l_forbid){
			 if(!is_literal_or_param_only(l_se)){
				 new_se = make_fta_se_ref(lfta_select_list, l_se,0);
				 ret_se = new scalarexp_t(se->get_op().c_str(), new_se);
			 }
		 }else{
			 ret_se = new scalarexp_t(se->get_op().c_str(), l_se);
		 }
 		 ret_se->use_decorations_of(se);
		 fta_forbidden = this_forbid | l_forbid;
		 return(ret_se);

    case SE_BINARY_OP:
		 l_se = split_fta_se(se->get_left_se(), l_forbid, lfta_select_list, Ext_fcns);
		 r_se = split_fta_se(se->get_right_se(), r_forbid, lfta_select_list, Ext_fcns);

		 this_forbid = ! dt->fta_legal_operation(l_se->get_data_type(), r_se->get_data_type(), se->get_op());

//		  	Replace the left se if it is not forbidden, but something else is.
		 if((this_forbid || r_forbid) & !l_forbid){
			 if(!is_literal_or_param_only(l_se)){
				 new_se = make_fta_se_ref(lfta_select_list, l_se,0);
				 l_se = new_se;
			 }
		 }

//			Replace the right se if it is not forbidden, but something else is.
		 if((this_forbid || l_forbid) & !r_forbid){
			 if(!is_literal_or_param_only(r_se)){
				 new_se = make_fta_se_ref(lfta_select_list, r_se,0);
				 r_se = new_se;
			 }
		 }

		 ret_se = new scalarexp_t(se->get_op().c_str(), l_se, r_se);
 		 ret_se->use_decorations_of(se);
		 fta_forbidden = this_forbid || r_forbid || l_forbid;

		 return(ret_se);

    case SE_AGGR_STAR:
    case SE_AGGR_SE:

		fprintf(stderr,"INTERNAL ERROR, aggregate ref (%s) in split_fta_se."
						" line=%d, col=%d\n",
						se->get_op().c_str(),
						se->lineno, se->charno
				);
		exit(1);
		break;

	case SE_FUNC:
		{
			fta_forbidden = false;
			operand_list = se->get_operands();
			vector<scalarexp_t *> new_operands;
			vector<bool> forbidden_op;
			for(p=0;p<operand_list.size();p++){
				l_se = split_fta_se(operand_list[p], l_forbid, lfta_select_list, Ext_fcns);

				fta_forbidden |= l_forbid;
				new_operands.push_back(l_se);
				forbidden_op.push_back(l_forbid);
				dt_signature.push_back(operand_list[p]->get_data_type() );
			}

			fcn_id = Ext_fcns->lookup_fcn(se->get_op(), dt_signature);
			if( fcn_id < 0 ){
				fprintf(stderr,"ERROR, no external function %s(",se->get_op().c_str());
				int o;
				for(o=0;o<operand_list.size();o++){
					if(o>0) fprintf(stderr,", ");
					fprintf(stderr,"%s",operand_list[o]->get_data_type()->get_type_str().c_str());
				}
				fprintf(stderr,") is defined, line %d, char %d\n", se->get_lineno(), se->get_charno() );
			if(fcn_id == -2) fprintf(stderr,"(multiple subsuming functions found)\n");
				return(false);
			}

			fta_forbidden |= (! Ext_fcns->fta_legal(fcn_id));

//				Replace the non-forbidden operands.
//				the forbidden ones are already replaced.
			if(fta_forbidden){
				for(p=0;p<new_operands.size();p++){
					if(! forbidden_op[p]){
//					  if(new_operands[p]->get_data_type()->get_temporal() != constant_t){
			 			if(!is_literal_or_param_only(new_operands[p])){
						new_se = make_fta_se_ref(lfta_select_list, new_operands[p],0);
						new_operands[p] = new_se;
					  }
					}
				}
			}

			ret_se = new scalarexp_t(se->get_op().c_str(), new_operands);
			ret_se->use_decorations_of(se);

			return(ret_se);

		}
	default:
		printf("INTERNAL ERROR in check_fta_forbidden_se: operator type %d\n",se->get_operator_type());
		exit(1);
	break;
  }
  return(false);

}

*/


//		The predicates have already been
//		broken into conjunctions.
//		If any part of a conjunction is fta-forbidden,
//		it must be executed in the stream operator.
//		Else it is executed in the FTA.
//		A pre-analysis should determine whether this
//		predicate is fta-safe.  This procedure will
//		assume that it is fta-forbidden and will
//		prepare it for execution in the stream.

/*

predicate_t *split_fta_pr(predicate_t *pr,
						 vector<select_element *> &lfta_select_list,
						 ext_fcn_list *Ext_fcns
						 ){

  vector<literal_t *> llist;
  scalarexp_t *se_l, *se_r;
  bool l_forbid, r_forbid;
  predicate_t *ret_pr, *pr_l, *pr_r;
  vector<scalarexp_t *> op_list, new_op_list;
  int o;
  vector<data_type *> dt_signature;


	switch(pr->get_operator_type()){
	case PRED_IN:
		se_l = split_fta_se(pr->get_left_se(), l_forbid, lfta_select_list, Ext_fcns);

		if(!l_forbid){
		  if(!is_literal_or_param_only(se_l)){
			scalarexp_t *new_se = make_fta_se_ref(lfta_select_list, se_l,0);
			se_l = new_se;
		  }
		}
		ret_pr = new predicate_t(se_l, pr->get_lit_vec());

		return(ret_pr);

	case PRED_COMPARE:
		se_l = split_fta_se(pr->get_left_se(), l_forbid, lfta_select_list, Ext_fcns);
		if(!l_forbid){
		  if(!is_literal_or_param_only(se_l)){
			scalarexp_t *new_se = make_fta_se_ref(lfta_select_list, se_l,0);
			se_l = new_se;
		  }
		}

		se_r = split_fta_se(pr->get_right_se(), r_forbid, lfta_select_list, Ext_fcns);
		if(!r_forbid){
		  if(!is_literal_or_param_only(se_r)){
			scalarexp_t *new_se = make_fta_se_ref(lfta_select_list, se_r,0);
			se_r = new_se;
		  }
		}

		ret_pr = new predicate_t(se_l, pr->get_op().c_str(), se_r);
		return(ret_pr);

	case PRED_UNARY_OP:
		pr_l = split_fta_pr(pr->get_left_pr(), lfta_select_list, Ext_fcns);
		ret_pr = new predicate_t(pr->get_op().c_str(), pr_l);
		return(ret_pr);

	case PRED_BINARY_OP:
		pr_l = split_fta_pr(pr->get_left_pr(), lfta_select_list, Ext_fcns);
		pr_r = split_fta_pr(pr->get_right_pr(), lfta_select_list, Ext_fcns);
		ret_pr = new predicate_t(pr->get_op().c_str(), pr_l, pr_r);
		return(ret_pr);

	case PRED_FUNC:
//			I can't push the predicate into the lfta, except by
//			returning a bool value, and that is not worth the trouble,
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			se_l = split_fta_se(op_list[o],l_forbid,lfta_select_list,Ext_fcns);
			if(!l_forbid){
		  	  if(!is_literal_or_param_only(se_l)){
				scalarexp_t *new_se = make_fta_se_ref(lfta_select_list, se_l,0);
				se_l = new_se;
		  	  }
			}
			new_op_list.push_back(se_l);
		}

		ret_pr =  new predicate_t(pr->get_op().c_str(), new_op_list);
		ret_pr->set_fcn_id(pr->get_fcn_id());
		return(ret_pr);
	default:
		fprintf(stderr,"INTERNAL ERROR in split_fta_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return(0);

}

*/


//--------------------------------------------------------------------



//		Split a scalar expression into one part which executes
//		at the stream and another set of parts which execute
//		at the FTA.
//		Because I'm actually modifying the SEs, I will make
//		copies.  But I will assume that literals, params, and
//		colrefs are immutable at this point.
//		(if there is ever a need to change one, must make a
//		 new value).
//		NOTE : if se is constant (only refrences literals),
//			avoid making the fta compute it.
//
//		NOTE : This will need to be generalized to
//		handle join expressions, namely to handle a vector
//		of lftas.
//
//		Return value is the HFTA se.
//		Add lftas select_elements to the fta_select_list.
//		set fta_forbidden if this node or any child cannot
//		execute at the lfta.

#define SPLIT_FTAVEC_NOTBLVAR -1
#define SPLIT_FTAVEC_MIXED -2

bool is_PROTOCOL_source(int colref_source,
			vector< vector<select_element *> *> &lfta_select_list){
	if(colref_source>=0 && lfta_select_list[colref_source]!=NULL) return true;
	return false;
}

int combine_colref_source(int s1, int s2){
	if(s1==s2) return(s1);
	if(s1==SPLIT_FTAVEC_NOTBLVAR) return s2;
	if(s2==SPLIT_FTAVEC_NOTBLVAR) return s1;
	return SPLIT_FTAVEC_MIXED;
}

scalarexp_t *split_ftavec_se(
				  scalarexp_t *se,	// the SE to split
				  bool &fta_forbidden,	// return true if some part of se
										// is fta-unsafe
				  int &colref_source,	// the tblvar which sources the
										// colref, or NOTBLVAR, or MIXED
				  vector< vector<select_element *> *> &lfta_select_list,
										// NULL if the tblvar is not PROTOCOL,
										// else build the select list.
				  ext_fcn_list *Ext_fcns // is the fcn lfta-safe?
				 ){
//		Return value is the HFTA SE, unless fta_forbidden is true and
//		colref_source>=0 and the indicated source is PROTOCOL.
//		In that case no split was done, the make_fta_se_ref must
//		be done by the caller.

  int p, fcn_id;
  vector<scalarexp_t *> operand_list;
  vector<data_type *> dt_signature;
  scalarexp_t *ret_se, *l_se, *r_se;
  bool l_forbid, r_forbid, this_forbid;
  int l_csource, r_csource, this_csource;
  colref_t *new_cr;
  scalarexp_t *new_se;
  data_type *dt = se->get_data_type();

  switch(se->get_operator_type()){
    case SE_LITERAL:
		fta_forbidden = ! se->get_data_type()->fta_legal_type();
		colref_source = SPLIT_FTAVEC_NOTBLVAR;
		ret_se = new scalarexp_t(se->get_literal());
		ret_se->use_decorations_of(se);
		return(ret_se);

    case SE_PARAM:
		fta_forbidden = ! se->get_data_type()->fta_legal_type();
		colref_source = SPLIT_FTAVEC_NOTBLVAR;
		ret_se = scalarexp_t::make_param_reference(se->get_op().c_str());
		ret_se->use_decorations_of(se);
		return(ret_se);

	case SE_IFACE_PARAM:
		fta_forbidden = false;
		colref_source = se->get_ifpref()->get_tablevar_ref();
		ret_se = scalarexp_t::make_iface_param_reference(se->get_ifpref());
		ret_se->use_decorations_of(se);
		return(ret_se);

    case SE_COLREF:
//			No colref should be forbidden,
//			the schema is wrong, the fta_legal_type() fcn is wrong,
//			or the source table is actually a stream.
//			Issue a warning, but proceed with processing.
//			Also, should not be a ref to a gbvar.
//			(a gbvar ref only occurs in an aggregation node,
//			and these SEs are rehomed, not split.
		fta_forbidden = ! se->get_data_type()->fta_legal_type();
		colref_source = se->get_colref()->get_tablevar_ref();

		if(fta_forbidden && is_PROTOCOL_source(colref_source, lfta_select_list)){
			fprintf(stderr,"WARNING, a PROTOCOL colref is a forbidden data type in split_ftavec_se,"
							" colref is %s,"
							" type is %s, line=%d, col=%d\n",
							se->get_colref()->to_string().c_str(),
							se->get_data_type()->to_string().c_str(),
							se->lineno, se->charno
					);
		}

		if(se->is_gb()){
			fta_forbidden = true;	// eval in hfta.  ASSUME make copy as below.
		}

		ret_se = new scalarexp_t(se->get_colref());
		ret_se->use_decorations_of(se);
		return(ret_se);

    case SE_UNARY_OP:
		 l_se = split_ftavec_se(se->get_left_se(), l_forbid, colref_source, lfta_select_list, Ext_fcns);

		 this_forbid = ! dt->fta_legal_operation(l_se->get_data_type(), se->get_op());

//			If this operation is forbidden but the child SE is not,
//			AND the colref source in the se is a single PROTOCOL source
//			put the child se on the lfta_select_list, create a colref
//			which accesses this se, and make it the child of this op.
//			Exception : the child se is constant (only literal refs).
//			TODO: I think the exception is expressed by is_PROTOCOL_source
		 if(this_forbid && !l_forbid && is_PROTOCOL_source(colref_source, lfta_select_list)){
			 if(!is_literal_or_param_only(l_se)){
				 new_se = make_fta_se_ref(lfta_select_list, l_se,colref_source);
				 ret_se = new scalarexp_t(se->get_op().c_str(), new_se);
			 }
		 }else{
			 ret_se = new scalarexp_t(se->get_op().c_str(), l_se);
		 }
 		 ret_se->use_decorations_of(se);
		 fta_forbidden = this_forbid | l_forbid;
		 return(ret_se);

    case SE_BINARY_OP:
		 l_se = split_ftavec_se(se->get_left_se(), l_forbid, l_csource, lfta_select_list, Ext_fcns);
		 r_se = split_ftavec_se(se->get_right_se(), r_forbid, r_csource, lfta_select_list, Ext_fcns);

		 this_forbid = ! dt->fta_legal_operation(l_se->get_data_type(), r_se->get_data_type(), se->get_op());
		 colref_source=combine_colref_source(l_csource, r_csource);

//		  	Replace the left se if the parent must be hfta but the child can
//			be lfta. This translates to
//			a) result is PROTOCOL and forbidden, but left SE is not forbidden
//			OR b) if result is mixed but the left se is PROTOCOL, not forbidden
		 if( ((this_forbid || r_forbid) && !l_forbid && is_PROTOCOL_source(colref_source, lfta_select_list) ) ||
				(colref_source==SPLIT_FTAVEC_MIXED && !l_forbid &&
				 is_PROTOCOL_source(l_csource, lfta_select_list)) ){
			 if(!is_literal_or_param_only(l_se)){
				 new_se = make_fta_se_ref(lfta_select_list, l_se,l_csource);
				 l_se = new_se;
			 }
		 }

//			same logic as for right se.
		 if( ((this_forbid || l_forbid) && !r_forbid && is_PROTOCOL_source(colref_source, lfta_select_list) ) ||
				(colref_source==SPLIT_FTAVEC_MIXED && !r_forbid &&
				 is_PROTOCOL_source(r_csource, lfta_select_list)) ){
			 if(!is_literal_or_param_only(r_se)){
				 new_se = make_fta_se_ref(lfta_select_list, r_se,r_csource);
				 r_se = new_se;
			 }
		 }

		 ret_se = new scalarexp_t(se->get_op().c_str(), l_se, r_se);
 		 ret_se->use_decorations_of(se);
		 fta_forbidden = this_forbid || r_forbid || l_forbid;

		 return(ret_se);

    case SE_AGGR_STAR:
    case SE_AGGR_SE:

		fprintf(stderr,"INTERNAL ERROR, aggregate ref (%s) in split_ftavec_se."
						" line=%d, col=%d\n",
						se->get_op().c_str(),
						se->lineno, se->charno
				);
		exit(1);
		break;

	case SE_FUNC:
		{
			operand_list = se->get_operands();
			vector<scalarexp_t *> new_operands;
			vector<bool> forbidden_op;
			vector<int> csource;

			fta_forbidden = false;
			colref_source = SPLIT_FTAVEC_NOTBLVAR;
			for(p=0;p<operand_list.size();p++){
				l_se = split_ftavec_se(operand_list[p], l_forbid, l_csource, lfta_select_list, Ext_fcns);

				fta_forbidden |= l_forbid;
				colref_source = combine_colref_source(colref_source, l_csource);
				new_operands.push_back(l_se);
				forbidden_op.push_back(l_forbid);
				csource.push_back(l_csource);
				dt_signature.push_back(operand_list[p]->get_data_type() );
			}

			fcn_id = Ext_fcns->lookup_fcn(se->get_op(), dt_signature);
			if( fcn_id < 0 ){
				fprintf(stderr,"ERROR, no external function %s(",se->get_op().c_str());
				int o;
				for(o=0;o<operand_list.size();o++){
					if(o>0) fprintf(stderr,", ");
					fprintf(stderr,"%s",operand_list[o]->get_data_type()->to_string().c_str());
				}
				fprintf(stderr,") is defined, line %d, char %d\n", se->get_lineno(), se->get_charno() );
			if(fcn_id == -2) fprintf(stderr,"(multiple subsuming functions found)\n");
				return NULL;
			}

			fta_forbidden |= (! Ext_fcns->fta_legal(fcn_id));

//				Replace the non-forbidden operands.
//				the forbidden ones are already replaced.
			if(fta_forbidden || colref_source == SPLIT_FTAVEC_MIXED){
				for(p=0;p<new_operands.size();p++){
					if(! forbidden_op[p] && is_PROTOCOL_source(csource[p], lfta_select_list)){
			 			if(!is_literal_or_param_only(new_operands[p])){
						new_se = make_fta_se_ref(lfta_select_list, new_operands[p],csource[p]);
						new_operands[p] = new_se;
					  }
					}
				}
			}

			ret_se = new scalarexp_t(se->get_op().c_str(), new_operands);
			ret_se->use_decorations_of(se);

			return(ret_se);

		}
	default:
		printf("INTERNAL ERROR in split_ftavec_se: operator type %d\n",se->get_operator_type());
		exit(1);
	break;
  }
  return(false);

}


//		The predicates have already been
//		broken into conjunctions.
//		If any part of a conjunction is fta-forbidden,
//		it must be executed in the stream operator.
//		Else it is executed in the FTA.
//		A pre-analysis should determine whether this
//		predicate is fta-safe.  This procedure will
//		assume that it is fta-forbidden and will
//		prepare it for execution in the stream.

predicate_t *split_ftavec_pr(predicate_t *pr,
				  vector< vector<select_element *> *> &lfta_select_list,
						 ext_fcn_list *Ext_fcns
						 ){

  vector<literal_t *> llist;
  scalarexp_t *se_l, *se_r;
  bool l_forbid, r_forbid;
  int l_csource, r_csource;
  predicate_t *ret_pr, *pr_l, *pr_r;
  vector<scalarexp_t *> op_list, new_op_list;
  int o;
  vector<data_type *> dt_signature;


	switch(pr->get_operator_type()){
	case PRED_IN:
		se_l = split_ftavec_se(pr->get_left_se(), l_forbid, l_csource, lfta_select_list, Ext_fcns);

//				TODO: checking that the se is a PROTOCOL source should
//				take care of literal_or_param_only.
		if(!l_forbid && is_PROTOCOL_source(l_csource, lfta_select_list)){
		  if(!is_literal_or_param_only(se_l)){
			scalarexp_t *new_se = make_fta_se_ref(lfta_select_list, se_l,l_csource);
			se_l = new_se;
		  }
		}
		ret_pr = new predicate_t(se_l, pr->get_lit_vec());

		return(ret_pr);

	case PRED_COMPARE:
		se_l = split_ftavec_se(pr->get_left_se(), l_forbid, l_csource, lfta_select_list, Ext_fcns);
		if(!l_forbid  && is_PROTOCOL_source(l_csource, lfta_select_list)){
		  if(!is_literal_or_param_only(se_l)){
			scalarexp_t *new_se = make_fta_se_ref(lfta_select_list, se_l,l_csource);
			se_l = new_se;
		  }
		}

		se_r = split_ftavec_se(pr->get_right_se(), r_forbid, r_csource, lfta_select_list, Ext_fcns);
		if(!r_forbid  && is_PROTOCOL_source(r_csource, lfta_select_list)){
		  if(!is_literal_or_param_only(se_r)){
			scalarexp_t *new_se = make_fta_se_ref(lfta_select_list, se_r,r_csource);
			se_r = new_se;
		  }
		}

		ret_pr = new predicate_t(se_l, pr->get_op().c_str(), se_r);
		return(ret_pr);

	case PRED_UNARY_OP:
		pr_l = split_ftavec_pr(pr->get_left_pr(), lfta_select_list, Ext_fcns);
		ret_pr = new predicate_t(pr->get_op().c_str(), pr_l);
		return(ret_pr);

	case PRED_BINARY_OP:
		pr_l = split_ftavec_pr(pr->get_left_pr(), lfta_select_list, Ext_fcns);
		pr_r = split_ftavec_pr(pr->get_right_pr(), lfta_select_list, Ext_fcns);
		ret_pr = new predicate_t(pr->get_op().c_str(), pr_l, pr_r);
		return(ret_pr);

	case PRED_FUNC:
//			I can't push the predicate into the lfta, except by
//			returning a bool value, and that is not worth the trouble,
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			se_l = split_ftavec_se(op_list[o],l_forbid,l_csource,lfta_select_list,Ext_fcns);
			if(!l_forbid && is_PROTOCOL_source(l_csource, lfta_select_list)){
		  	  if(!is_literal_or_param_only(se_l)){
				scalarexp_t *new_se = make_fta_se_ref(lfta_select_list, se_l,l_csource);
				se_l = new_se;
		  	  }
			}
			new_op_list.push_back(se_l);
		}

		ret_pr =  new predicate_t(pr->get_op().c_str(), new_op_list);
		ret_pr->set_fcn_id(pr->get_fcn_id());
		return(ret_pr);
	default:
		fprintf(stderr,"INTERNAL ERROR in split_fta_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return(0);

}



////////////////////////////////////////////////////////////////////////
///		rehome_hfta_se rehome_hfta_pr
///		This is use to split an sgah operator (aggregation),
///		I just need to make gb, aggr references point to the
///		new gb, aggr table entries.


scalarexp_t *rehome_fta_se(scalarexp_t *se,
				  map< int, scalarexp_t * > *aggr_map
				 ){

  int p, fcn_id;
  int agr_id;
  vector<scalarexp_t *> operand_list;
  scalarexp_t *ret_se, *l_se, *r_se;
  colref_t *new_cr;
  scalarexp_t *new_se;
  data_type *dt = se->get_data_type();
  vector<scalarexp_t *> new_operands;

  switch(se->get_operator_type()){
    case SE_LITERAL:
		ret_se = new scalarexp_t(se->get_literal());
		ret_se->use_decorations_of(se);
		return(ret_se);

    case SE_PARAM:
		ret_se = scalarexp_t::make_param_reference(se->get_op().c_str());
		ret_se->use_decorations_of(se);
		return(ret_se);

	case SE_IFACE_PARAM:
		ret_se = scalarexp_t::make_iface_param_reference(se->get_ifpref());
		ret_se->use_decorations_of(se);
		return(ret_se);



    case SE_COLREF:
//			Must be a GB REF ...
//			I'm assuming that the hfta gbvar table has the
//			same sequence of entries as the input query's gbvar table.
//			Else I'll need some kind of translation table.

		if(! se->is_gb()){
			fprintf(stderr,"WARNING, a colref is not a gbver ref in rehome_hfta_se"
							" type is %s, line=%d, col=%d\n",
							se->get_data_type()->to_string().c_str(),
							se->lineno, se->charno
					);
		}

		ret_se = new scalarexp_t(se->get_colref());
		ret_se->use_decorations_of(se);		// just inherit the gbref
		return(ret_se);

    case SE_UNARY_OP:
		 l_se = rehome_fta_se(se->get_left_se(), aggr_map);

		 ret_se = new scalarexp_t(se->get_op().c_str(), l_se);
 		 ret_se->use_decorations_of(se);
		 return(ret_se);

    case SE_BINARY_OP:
		 l_se = rehome_fta_se(se->get_left_se(), aggr_map);
		 r_se = rehome_fta_se(se->get_right_se(), aggr_map);

		 ret_se = new scalarexp_t(se->get_op().c_str(), l_se, r_se);
 		 ret_se->use_decorations_of(se);

		 return(ret_se);

    case SE_AGGR_STAR:
    case SE_AGGR_SE:
		agr_id = se->get_aggr_ref();
		return (*aggr_map)[agr_id];
		break;

	case SE_FUNC:
		agr_id = se->get_aggr_ref();
		if(agr_id >= 0) return (*aggr_map)[agr_id];

		operand_list = se->get_operands();
		for(p=0;p<operand_list.size();p++){
			l_se = rehome_fta_se(operand_list[p], aggr_map);

			new_operands.push_back(l_se);
		}


		ret_se = new scalarexp_t(se->get_op().c_str(), new_operands);
		ret_se->use_decorations_of(se);

		return(ret_se);

	default:
		printf("INTERNAL ERROR in rehome_fta_se: operator type %d\n",se->get_operator_type());
		exit(1);
	break;
  }
  return(false);

}


//		The predicates have already been
//		broken into conjunctions.
//		If any part of a conjunction is fta-forbidden,
//		it must be executed in the stream operator.
//		Else it is executed in the FTA.
//		A pre-analysis should determine whether this
//		predicate is fta-safe.  This procedure will
//		assume that it is fta-forbidden and will
//		prepare it for execution in the stream.

predicate_t *rehome_fta_pr(predicate_t *pr,
						 map<int, scalarexp_t *> *aggr_map
						 ){

  vector<literal_t *> llist;
  scalarexp_t *se_l, *se_r;
  predicate_t *ret_pr, *pr_l, *pr_r;
  vector<scalarexp_t *> op_list, new_op_list;
  int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		se_l = rehome_fta_se(pr->get_left_se(), aggr_map);
		ret_pr = new predicate_t(se_l, pr->get_lit_vec());
		return(ret_pr);

	case PRED_COMPARE:
		se_l = rehome_fta_se(pr->get_left_se(), aggr_map);
		se_r = rehome_fta_se(pr->get_right_se(), aggr_map);
		ret_pr = new predicate_t(se_l, pr->get_op().c_str(), se_r);
		return(ret_pr);

	case PRED_UNARY_OP:
		pr_l = rehome_fta_pr(pr->get_left_pr(), aggr_map);
		ret_pr = new predicate_t(pr->get_op().c_str(), pr_l);
		return(ret_pr);

	case PRED_BINARY_OP:
		pr_l = rehome_fta_pr(pr->get_left_pr(), aggr_map);
		pr_r = rehome_fta_pr(pr->get_right_pr(), aggr_map);
		ret_pr = new predicate_t(pr->get_op().c_str(), pr_l, pr_r);
		return(ret_pr);

	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			se_l = rehome_fta_se(op_list[o], aggr_map);
			new_op_list.push_back(se_l);
		}
		ret_pr=  new predicate_t(pr->get_op().c_str(), new_op_list);
		ret_pr->set_fcn_id(pr->get_fcn_id());
		return(ret_pr);

	default:
		fprintf(stderr,"INTERNAL ERROR in rehome_fta_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return(0);

}


////////////////////////////////////////////////////////////////////
/////////////////		Create a STREAM table to represent the FTA output.

table_def *create_attributes(string tname, vector<select_element *> &select_list){
	int s;


//			Create a new STREAM schema for the output of the FTA.

	field_entry_list *fel = new field_entry_list();
	set<string> ufcns;
	for(s=0;s<select_list.size();s++){
		scalarexp_t *sel_se = select_list[s]->se;
		data_type *dt = sel_se->get_data_type();

//			Grab the annotations of the field.
//			As of this writing, the only meaningful annotations
//			are whether or not the attribute is temporal.
//			There can be an annotation of constant_t, but
//			I'll ignore this, it feels like an unsafe assumption
		param_list *plist = new param_list();
//		if(dt->is_temporal()){
			vector<string> param_strings = dt->get_param_keys();
			int p;
			for(p=0;p<param_strings.size();++p){
				string v = dt->get_param_val(param_strings[p]);
				if(v != "")
					plist->append(param_strings[p].c_str(),v.c_str());
				else
					plist->append(param_strings[p].c_str());
			}
//		}

//		char access_fcn_name[500];
		string colname = select_list[s]->name;
//		sprintf(access_fcn_name,"get_field_%s",colname.c_str());
		string access_fcn_name = "get_field_"+colname;
		field_entry *fe = new field_entry(
			dt->get_type_str(), colname, access_fcn_name, plist, ufcns
		);

		fel->append_field(fe);
	}

	table_def *fta_tbl = new table_def(
		tname.c_str(), NULL, NULL, fel, STREAM_SCHEMA
	);

	return(fta_tbl);

}

//------------------------------------------------------------------
//		Textual representation of the query node.



string spx_qpn::to_query_string(){

	string ret = "Select ";
	int s;
	for(s=0;s<select_list.size();s++){
		if(s>0) ret+=", ";
		ret += se_to_query_string(select_list[s]->se, NULL);
		if(select_list[s]->name != "") ret += " AS "+select_list[s]->name;
	}
	ret += "\n";

	ret += "From "+table_name->to_string()+"\n";

	if(where.size() > 0){
		ret += "Where ";
		int w;
		for(w=0;w<where.size();w++){
			if(w>0) ret += " AND ";
			ret += "(" + pred_to_query_str(where[w]->pr,NULL) + ")";
		}
		ret += "\n";
	}

	return(ret);
}




string sgah_qpn::to_query_string(){

	string ret = "Select ";
	int s;
	for(s=0;s<select_list.size();s++){
		if(s>0) ret+=", ";
		ret += se_to_query_string(select_list[s]->se, &aggr_tbl);
		if(select_list[s]->name != "") ret += " AS "+select_list[s]->name;
	}
	ret += "\n";

	ret += "From "+table_name->to_string()+"\n";

	if(where.size() > 0){
		ret += "Where ";
		int w;
		for(w=0;w<where.size();w++){
			if(w>0) ret += " AND ";
			ret += "(" + pred_to_query_str(where[w]->pr,&aggr_tbl) + ")";
		}
		ret += "\n";
	}

	if(gb_tbl.size() > 0){
		ret += "Group By ";
		int g;
		if(gb_tbl.gb_patterns.size() <= 1 || gb_tbl.gb_entry_type.size()==0){
			for(g=0;g<gb_tbl.size();g++){
				if(g>0) ret += ", ";
//			if(gb_tbl.get_reftype(g) == GBVAR_SE){
					ret += se_to_query_string(gb_tbl.get_def(g), &aggr_tbl) + " AS ";
//			}
				ret += gb_tbl.get_name(g);
			}
		}else{
			int gb_pos = 0;
			for(g=0;g<gb_tbl.gb_entry_type.size();++g){
				if(g>0) ret += ", ";
				if(gb_tbl.gb_entry_type[g] == ""){
					ret += se_to_query_string(gb_tbl.get_def(gb_pos),&aggr_tbl)+
						" AS "+ gb_tbl.get_name(gb_pos);
					gb_pos++;
				}
				if(gb_tbl.gb_entry_type[g] == "CUBE" ||
						gb_tbl.gb_entry_type[g] == "ROLLUP"){
					ret += gb_tbl.gb_entry_type[g] + "(";
					int gg = 0;
					for(gg=0;gg<gb_tbl.gb_entry_count[g];++gg){
						if(gg>0) ret += ", ";
						ret += se_to_query_string(gb_tbl.get_def(gb_pos),&aggr_tbl)+ " AS "+ gb_tbl.get_name(gb_pos);
						gb_pos++;
					}
					ret += ")";
				}
				if(gb_tbl.gb_entry_type[g] == "GROUPING_SETS"){
					ret += gb_tbl.gb_entry_type[g] + "(";
					int g1, g2;
					vector<vector<bool> > &local_components = gb_tbl.pattern_components[g];
					for(g1=0;g1<local_components.size();++g1){
						if(g1>0) ret+=",";
						bool first_field = true;
						ret += "\n\t\t(";
						for(g2=0;g2<=gb_tbl.gb_entry_count[g];g2++){
							if(local_components[g1][g2]){
								if(!first_field) ret+=", ";
								else first_field = false;
								ret +=  gb_tbl.get_name(gb_pos+g2);
							}
						}
						ret += ")";
					}
					ret += ") ";
					gb_pos += gb_tbl.gb_entry_count[g];
				}
			}
		}
		ret += "\n";
	}

	if(having.size() > 0){
		ret += "Having ";
		int h;
		for(h=0;h<having.size();h++){
			if(h>0) ret += " AND ";
			ret += "(" + pred_to_query_str(having[h]->pr,&aggr_tbl) + ")";
		}
		ret += "\n";
	}

	return(ret);
}


string rsgah_qpn::to_query_string(){

	string ret = "Select ";
	int s;
	for(s=0;s<select_list.size();s++){
		if(s>0) ret+=", ";
		ret += se_to_query_string(select_list[s]->se, &aggr_tbl);
		if(select_list[s]->name != "") ret += " AS "+select_list[s]->name;
	}
	ret += "\n";

	ret += "From "+table_name->to_string()+"\n";

	if(where.size() > 0){
		ret += "Where ";
		int w;
		for(w=0;w<where.size();w++){
			if(w>0) ret += " AND ";
			ret += "(" + pred_to_query_str(where[w]->pr,&aggr_tbl) + ")";
		}
		ret += "\n";
	}

	if(gb_tbl.size() > 0){
		ret += "Group By ";
		int g;
		for(g=0;g<gb_tbl.size();g++){
			if(g>0) ret += ", ";
//			if(gb_tbl.get_reftype(g) == GBVAR_SE){
				ret += se_to_query_string(gb_tbl.get_def(g), &aggr_tbl)+" AS ";
//			}
			ret += gb_tbl.get_name(g);
		}
		ret += "\n";
	}

	if(having.size() > 0){
		ret += "Having ";
		int h;
		for(h=0;h<having.size();h++){
			if(h>0) ret += " AND ";
			ret += "(" + pred_to_query_str(having[h]->pr,&aggr_tbl) + ")";
		}
		ret += "\n";
	}

	if(closing_when.size() > 0){
		ret += "Closing_When ";
		int h;
		for(h=0;h<closing_when.size();h++){
			if(h>0) ret += " AND ";
			ret += "(" + pred_to_query_str(closing_when[h]->pr,&aggr_tbl) + ")";
		}
		ret += "\n";
	}

	return(ret);
}


string sgahcwcb_qpn::to_query_string(){

	string ret = "Select ";
	int s;
	for(s=0;s<select_list.size();s++){
		if(s>0) ret+=", ";
		ret += se_to_query_string(select_list[s]->se, &aggr_tbl);
		if(select_list[s]->name != "") ret += " AS "+select_list[s]->name;
	}
	ret += "\n";

	ret += "From "+table_name->to_string()+"\n";

	if(where.size() > 0){
		ret += "Where ";
		int w;
		for(w=0;w<where.size();w++){
			if(w>0) ret += " AND ";
			ret += "(" + pred_to_query_str(where[w]->pr,&aggr_tbl) + ")";
		}
		ret += "\n";
	}

	if(gb_tbl.size() > 0){
		ret += "Group By ";
		int g;
		for(g=0;g<gb_tbl.size();g++){
			if(g>0) ret += ", ";
//			if(gb_tbl.get_reftype(g) == GBVAR_SE){
				ret += se_to_query_string(gb_tbl.get_def(g), &aggr_tbl) + " AS ";
//			}
			ret += gb_tbl.get_name(g);
		}
		ret += "\n";
	}

	if(sg_tbl.size() > 0){
		ret += "Supergroup ";
		int g;
		bool first_elem = true;
		for(g=0;g<gb_tbl.size();g++){
			if(sg_tbl.count(g)){
				if(first_elem){
					ret += ", ";
					first_elem = false;
				}
				ret += gb_tbl.get_name(g);
			}
		}
		ret += "\n";
	}

	if(having.size() > 0){
		ret += "Having ";
		int h;
		for(h=0;h<having.size();h++){
			if(h>0) ret += " AND ";
			ret += "(" + pred_to_query_str(having[h]->pr,&aggr_tbl) + ")";
		}
		ret += "\n";
	}


	if(cleanwhen.size() > 0){
		ret += "Cleaning_When ";
		int h;
		for(h=0;h<cleanwhen.size();h++){
			if(h>0) ret += " AND ";
			ret += "(" + pred_to_query_str(cleanwhen[h]->pr,&aggr_tbl) + ")";
		}
		ret += "\n";
	}

	if(cleanby.size() > 0){
		ret += "Cleaning_By ";
		int h;
		for(h=0;h<cleanby.size();h++){
			if(h>0) ret += " AND ";
			ret += "(" + pred_to_query_str(cleanby[h]->pr,&aggr_tbl) + ")";
		}
		ret += "\n";
	}

	return(ret);
}


string mrg_qpn::to_query_string(){

	string ret="Merge ";
	ret += mvars[0]->to_query_string() + " : " + mvars[1]->to_query_string();
	if(slack != NULL){
		ret += " SLACK "+se_to_query_string(slack, NULL);
	}

	ret += "\nFrom ";
	int t;
	for(t=0;t<fm.size();++t){
		if(t>0) ret += ", ";
		ret += fm[t]->to_string();
	}
	ret += "\n";

	return(ret);
}

string join_eq_hash_qpn::to_query_string(){

	string ret = "Select ";
	int s;
	for(s=0;s<select_list.size();s++){
		if(s>0) ret+=", ";
		ret += se_to_query_string(select_list[s]->se, NULL);
		if(select_list[s]->name != "") ret += " AS "+select_list[s]->name;
	}
	ret += "\n";

//			NOTE: assuming binary join.
	int properties = from[0]->get_property()+2*from[1]->get_property();
	switch(properties){
	case 0:
		ret += "INNER_JOIN ";
		break;
	case 1:
		ret += "LEFT_OUTER_JOIN ";
		break;
	case 2:
		ret += "RIGHT_OUTER_JOIN ";
		break;
	case 3:
		ret += "OUTER_JOIN ";
		break;
	}

	ret += "From ";
	int f;
	for(f=0;f<from.size();++f){
		if(f>0) ret+=", ";
		ret += from[f]->to_string();
	}
	ret += "\n";

	if(where.size() > 0){
		ret += "Where ";
		int w;
		for(w=0;w<where.size();w++){
			if(w>0) ret += " AND ";
			ret += "(" + pred_to_query_str(where[w]->pr,NULL) + ")";
		}
		ret += "\n";
	}

	return(ret);
}

string filter_join_qpn::to_query_string(){

	string ret = "Select ";
	int s;
	for(s=0;s<select_list.size();s++){
		if(s>0) ret+=", ";
		ret += se_to_query_string(select_list[s]->se, NULL);
		if(select_list[s]->name != "") ret += " AS "+select_list[s]->name;
	}
	ret += "\n";

//			NOTE: assuming binary join.
	ret += "FILTER_JOIN("+temporal_var->field+","+int_to_string(temporal_range)+") ";

	ret += "From ";
	int f;
	for(f=0;f<from.size();++f){
		if(f>0) ret+=", ";
		ret += from[f]->to_string();
	}
	ret += "\n";

	if(where.size() > 0){
		ret += "Where ";
		int w;
		for(w=0;w<where.size();w++){
			if(w>0) ret += " AND ";
			ret += "(" + pred_to_query_str(where[w]->pr,NULL) + ")";
		}
		ret += "\n";
	}

	return(ret);
}


// -----------------------------------------------------------------
//		Query node subclass specific processing.


vector<mrg_qpn *> mrg_qpn::split_sources(){
  vector<mrg_qpn *> ret;
  int i;

//			sanity check
	if(fm.size() != mvars.size()){
		fprintf(stderr,"INTERNAL ERROR in mrg_qpn::split_sources.  fm.size() = %lu, mvars.size() = %lu\n",fm.size(),mvars.size());
		exit(1);
	}
	if(fm.size() == 1){
		fprintf(stderr,"INTERNAL ERROR in mrg_qpn::split_sources, fm size is 1.\n");
		exit(1);
	}

/*
int ff;
printf("spliting sources merge node, name = %s, %d sources.\n\t",node_name.c_str(), fm.size());
for(ff=0;ff<fm.size();++ff){
printf("%s ",fm[ff]->to_string().c_str());
}
printf("\n");
*/

//		Handle special cases.
	if(fm.size() == 2){
		ret.push_back(this);
		return ret;
	}

	if(fm.size() == 3){
		mrg_qpn *new_mrg = (mrg_qpn *)this->make_copy("_cH1");
		new_mrg->fm.push_back(this->fm[0]);
		new_mrg->fm.push_back(this->fm[1]);
		new_mrg->mvars.push_back(this->mvars[0]);
		new_mrg->mvars.push_back(this->mvars[1]);

		this->fm.erase(this->fm.begin());
		this->mvars.erase(this->mvars.begin());
		string vname = fm[0]->get_var_name();
		this->fm[0] = new tablevar_t(new_mrg->node_name.c_str());
		this->fm[0]->set_range_var(vname);
		this->mvars[0]->set_field(table_layout->get_field_name(merge_fieldpos));
		this->mvars[0]->set_tablevar_ref(0);
		this->mvars[1]->set_tablevar_ref(1);

		ret.push_back(new_mrg);
		ret.push_back(this);

/*
printf("split sources %s (%s %s)\n",node_name.c_str(),new_mrg->node_name.c_str(),this->node_name.c_str());
for(i=0;i<new_mrg->fm.size();++i)
printf("\tsource %s var %d (%s, %s) \n",new_mrg->node_name.c_str(),i,new_mrg->fm[i]->to_string().c_str(), new_mrg->mvars[i]->to_string().c_str());
for(i=0;i<this->fm.size();++i)
printf("\tsource %s var %d (%s, %s) \n",this->node_name.c_str(),i,this->fm[i]->to_string().c_str(), this->mvars[i]->to_string().c_str());
*/

		return ret;
	}

//		General case.
//		divide up the sources between two children.
//		Then, recurse on the children.

		mrg_qpn *new_mrg1 = (mrg_qpn *)this->make_copy("_cH1");
		mrg_qpn *new_mrg2 = (mrg_qpn *)this->make_copy("_cH2");
		for(i=0;i<this->fm.size()/2;++i){
			new_mrg1->fm.push_back(this->fm[i]);
			new_mrg1->mvars.push_back(this->mvars[i]);
//printf("Pushing %d (%s, %s) to new_mrg1\n",i,fm[i]->to_string().c_str(), mvars[i]->to_string().c_str());
		}
		for(;i<this->fm.size();++i){
			new_mrg2->fm.push_back(this->fm[i]);
			new_mrg2->mvars.push_back(this->mvars[i]);
//printf("Pushing %d (%s, %s) to new_mrg2\n",i,fm[i]->to_string().c_str(), mvars[i]->to_string().c_str());
		}
		for(i=0;i<new_mrg1->mvars.size();++i)
			new_mrg1->mvars[i]->set_tablevar_ref(i);
		for(i=0;i<new_mrg2->mvars.size();++i)
			new_mrg2->mvars[i]->set_tablevar_ref(i);

//			Children created, make this merge them.
		fm.clear();
		mvars.clear();
//			var 1
		tablevar_t *tmp_tblvar = new tablevar_t(new_mrg1->node_name.c_str());
		tmp_tblvar->set_range_var("_mrg_var_1");
		fm.push_back(tmp_tblvar);
		colref_t *tmp_cref = new colref_t("_mrg_var_1",table_layout->get_field_name(merge_fieldpos).c_str());
		tmp_cref->set_tablevar_ref(0);
		mvars.push_back(tmp_cref);
//			var 2
		tmp_tblvar = new tablevar_t(new_mrg2->node_name.c_str());
		tmp_tblvar->set_range_var("_mrg_var_2");
		fm.push_back(tmp_tblvar);
		tmp_cref = new colref_t("_mrg_var_2",table_layout->get_field_name(merge_fieldpos).c_str());
		tmp_cref->set_tablevar_ref(1);
		mvars.push_back(tmp_cref);


/*
printf("split sources %s (%s %s)\n",node_name.c_str(),new_mrg1->node_name.c_str(),new_mrg2->node_name.c_str());
for(i=0;i<new_mrg1->fm.size();++i)
printf("\tsource %s var %d (%s, %s) \n",new_mrg1->node_name.c_str(),i,new_mrg1->fm[i]->to_string().c_str(), new_mrg1->mvars[i]->to_string().c_str());
for(i=0;i<new_mrg2->fm.size();++i)
printf("\tsource %s var %d (%s, %s) \n",new_mrg2->node_name.c_str(),i,new_mrg2->fm[i]->to_string().c_str(), new_mrg2->mvars[i]->to_string().c_str());
*/

//		Recurse and put them together
		vector<mrg_qpn *> st1 = new_mrg1->split_sources();
		ret.insert(ret.end(), st1.begin(), st1.end());
		vector<mrg_qpn *> st2 = new_mrg2->split_sources();
		ret.insert(ret.end(), st2.begin(), st2.end());

		ret.push_back(this);

		return(ret);

}



////////	Split helper function : resolve interfaces

vector<pair<string,string> > get_ifaces(tablevar_t *table, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx){
	vector<pair<string,string> > basic_ifaces;
	int ierr;
	if(table->get_ifq()){
		basic_ifaces= ifdb->eval(table->get_interface(),ierr);
		if(ierr==1){
		fprintf(stderr,"ERROR, Interface set %s not found.\n",table->get_interface().c_str());
		}
		if(ierr==2){
			fprintf(stderr,"ERROR, interface definition file didn't parse.\n");
		}
	}else{
		basic_ifaces.push_back(make_pair(table->get_machine(), table->get_interface()));
	}

	if(n_virtual_ifaces == 1)
		return basic_ifaces;

	int stride = n_virtual_ifaces / hfta_parallelism;
	int i,s;
	vector<pair<string,string> > ifaces;

	for(i=0;i<basic_ifaces.size();++i){
		string mach = basic_ifaces[i].first;
		string iface = basic_ifaces[i].second;
		for(s=hfta_idx*stride;s<(hfta_idx+1)*stride;++s){
			ifaces.push_back(pair<string, string>(mach,iface+"X"+int_to_string(2*s)));
		}
	}

	return ifaces;
}


/////////	Split helper function : compute slack in a generated
/////////	merge.

void mrg_qpn::resolve_slack(scalarexp_t *t_se, string fname, vector<pair<string, string> > &sources, ifq_t *ifdb, gb_table *gbt){
	int s,e,v;
	string es;

//		Find slack divisor, if any.
	string fnm;
	long long int slack_divisor = find_temporal_divisor(t_se,gbt, fnm);
	if(slack_divisor <= 0){
		slack = NULL;
		return;
	}

//		find max slack in the iface spec
	long long int max_slacker = 0, this_slacker;
	string rname = "Slack_"+fnm;
	for(s=0;s<sources.size();++s){
		string src_machine = sources[s].first;
		string src_iface = sources[s].second;
		vector<string> slack_vec = ifdb->get_iface_vals(src_machine, src_iface,rname,e,es);
		for(v=0;v<slack_vec.size();++v){
			if(sscanf(slack_vec[v].c_str(),"%qd",&this_slacker)){
				if(this_slacker > max_slacker)
					max_slacker = this_slacker;
			}
		}
	}

	if(max_slacker <= 0){
		slack = NULL;
		return;
	}

//		convert to SE
	long long int the_slack=(long long int)(ceil(((double)max_slacker)/((double)slack_divisor)));
	char tmps[256];
	sprintf(tmps,"%lld",the_slack);
	literal_t *slack_lit = new literal_t(tmps, LITERAL_LONGINT);
	slack = new scalarexp_t(slack_lit);
}


//------------------------------------------------------------------
//		split a node to extract LFTA components.


vector<qp_node *> mrg_qpn::split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx){
	// nothing to do, nothing to split, return copy of self.

	hfta_returned = 1;

	vector<qp_node *> ret_vec;

	ret_vec.push_back(this);
	return(ret_vec);

}

vector<qp_node *> filter_join_qpn::split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx){
	vector<qp_node *> ret_vec;

//		First check if the query can be pushed to the FTA.
	bool fta_ok = true;
	int s;
	for(s=0;s<select_list.size();s++){
		fta_ok &= check_fta_forbidden_se(select_list[s]->se,NULL, Ext_fcns);
	}
	int p;
	for(p=0;p<where.size();p++){
		fta_ok &= check_fta_forbidden_pr(where[p]->pr,NULL, Ext_fcns);
	}

	if(!fta_ok){
		fprintf(stderr,"ERROR, filter join %s is fta-unsafe.\n",node_name.c_str());
		exit(1);
	}

//		Can it be done in a single lfta?
//			Get the set of interfaces it accesses.
	int ierr;
	int si;
	vector<string> sel_names;
	vector<pair<string,string> > ifaces = get_ifaces(from[0], ifdb, n_virtual_ifaces, hfta_parallelism, hfta_idx);
	if (ifaces.empty()) {
		fprintf(stderr,"INTERNAL ERROR in filter_join_qpn::split_node_for_fta - empty interface set\n");
		exit(1);
	}

	if(ifaces.size() == 1){
//				Single interface, no need to merge.
		hfta_returned = 0;
		ret_vec.push_back(this);
		int i;
		for(i=0;i<from.size();i++){
			from[i]->set_machine(ifaces[0].first);
			from[i]->set_interface(ifaces[0].second);
			from[i]->set_ifq(false);
		}
		return(ret_vec);
	}else{
//				Multiple interfaces, generate the interface-specific queries plus
//				the merge.
		hfta_returned = 1;

		vector<string> sel_names;
		for(si=0;si<ifaces.size();++si){
			filter_join_qpn *fta_node = new filter_join_qpn();

//			Name the fta
			if(ifaces.size()==1)
				fta_node->set_node_name( node_name );
			else{
				string new_name =  "_"+node_name+"_"+ifaces[si].first+"_"+ifaces[si].second;
				untaboo(new_name);
				fta_node->set_node_name(new_name);
			}
			sel_names.push_back(fta_node->get_node_name());

//			Assign the table
			int f;
			for(f=0;f<from.size();f++){
				fta_node->from.push_back(from[f]->duplicate());
				fta_node->from[f]->set_machine(ifaces[si].first);
				fta_node->from[f]->set_interface(ifaces[si].second);
				fta_node->from[f]->set_ifq(false);
			}
			fta_node->temporal_var = temporal_var;
			fta_node->temporal_range = temporal_range;

			fta_node->use_bloom = use_bloom;

			for(s=0;s<select_list.size();s++){
				fta_node->select_list.push_back( dup_select(select_list[s], NULL) );
			}

			for(p=0;p<shared_pred.size();p++){
				predicate_t *new_pr = dup_pr(where[p]->pr, NULL);
				cnf_elem *new_cnf = new cnf_elem(new_pr);
				analyze_cnf(new_cnf);
				fta_node->shared_pred.push_back(new_cnf);
				fta_node->where.push_back(new_cnf);
			}
			for(p=0;p<pred_t0.size();p++){
				predicate_t *new_pr = dup_pr(where[p]->pr, NULL);
				cnf_elem *new_cnf = new cnf_elem(new_pr);
				analyze_cnf(new_cnf);
				fta_node->pred_t0.push_back(new_cnf);
				fta_node->where.push_back(new_cnf);
			}
			for(p=0;p<pred_t1.size();p++){
				predicate_t *new_pr = dup_pr(where[p]->pr, NULL);
				cnf_elem *new_cnf = new cnf_elem(new_pr);
				analyze_cnf(new_cnf);
				fta_node->pred_t1.push_back(new_cnf);
				fta_node->where.push_back(new_cnf);
			}
			for(p=0;p<hash_eq.size();p++){
				predicate_t *new_pr = dup_pr(where[p]->pr, NULL);
				cnf_elem *new_cnf = new cnf_elem(new_pr);
				analyze_cnf(new_cnf);
				fta_node->hash_eq.push_back(new_cnf);
				fta_node->where.push_back(new_cnf);
			}
			for(p=0;p<postfilter.size();p++){
				predicate_t *new_pr = dup_pr(where[p]->pr, NULL);
				cnf_elem *new_cnf = new cnf_elem(new_pr);
				analyze_cnf(new_cnf);
				fta_node->postfilter.push_back(new_cnf);
				fta_node->where.push_back(new_cnf);
			}

//			Xfer all of the parameters.
//			Use existing handle annotations.
			vector<string> param_names = param_tbl->get_param_names();
			int pi;
			for(pi=0;pi<param_names.size();pi++){
				data_type *dt = param_tbl->get_data_type(param_names[pi]);
				fta_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
			}
			fta_node->definitions = definitions;
			if(fta_node->resolve_if_params(ifdb, this->err_str)){
				this->error_code = 3;
				return ret_vec;
			}

			ret_vec.push_back(fta_node);
		}

		mrg_qpn *mrg_node = new mrg_qpn((filter_join_qpn *)ret_vec[0],
			 node_name,  sel_names,ifaces, ifdb);
		ret_vec.push_back(mrg_node);

		return(ret_vec);
	}

}

//		Use to search for unresolved interface param refs in an hfta.

int spx_qpn::count_ifp_refs(set<string> &ifpnames){
	int ret = 0;
	int i;
	for(i=0;i<select_list.size();++i)
		ret += count_se_ifp_refs(select_list[i]->se,ifpnames);
	for(i=0;i<where.size();++i)
		ret += count_pr_ifp_refs(where[i]->pr,ifpnames);
	return ret;
}

int sgah_qpn::count_ifp_refs(set<string> &ifpnames){
	int ret = 0;
	int i,j;
	for(i=0;i<select_list.size();++i)
		ret += count_se_ifp_refs(select_list[i]->se,ifpnames);
	for(i=0;i<where.size();++i)
		ret += count_pr_ifp_refs(where[i]->pr,ifpnames);
	for(i=0;i<having.size();++i)
		ret += count_pr_ifp_refs(having[i]->pr,ifpnames);
	for(i=0;i<aggr_tbl.size();++i){
		if(aggr_tbl.is_builtin(i) && !aggr_tbl.is_star_aggr(i)){
			ret += count_se_ifp_refs(aggr_tbl.get_aggr_se(i),ifpnames);
		}else{
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(i);
			for(j=0;j<opl.size();++j)
				ret += count_se_ifp_refs(opl[j],ifpnames);
		}
	}
	for(i=0;i<gb_tbl.size();++i){
		ret += count_se_ifp_refs(gb_tbl.get_def(i), ifpnames);
	}
	return ret;
}


int rsgah_qpn::count_ifp_refs(set<string> &ifpnames){
	int ret = 0;
	int i,j;
	for(i=0;i<select_list.size();++i)
		ret += count_se_ifp_refs(select_list[i]->se,ifpnames);
	for(i=0;i<where.size();++i)
		ret += count_pr_ifp_refs(where[i]->pr,ifpnames);
	for(i=0;i<having.size();++i)
		ret += count_pr_ifp_refs(having[i]->pr,ifpnames);
	for(i=0;i<closing_when.size();++i)
		ret += count_pr_ifp_refs(closing_when[i]->pr,ifpnames);
	for(i=0;i<aggr_tbl.size();++i){
		if(aggr_tbl.is_builtin(i) && !aggr_tbl.is_star_aggr(i)){
			ret += count_se_ifp_refs(aggr_tbl.get_aggr_se(i),ifpnames);
		}else{
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(i);
			for(j=0;j<opl.size();++j)
				ret += count_se_ifp_refs(opl[j],ifpnames);
		}
	}
	for(i=0;i<gb_tbl.size();++i){
		ret += count_se_ifp_refs(gb_tbl.get_def(i), ifpnames);
	}
	return ret;
}

int mrg_qpn::count_ifp_refs(set<string> &ifpnames){
	return 0;
}

int join_eq_hash_qpn::count_ifp_refs(set<string> &ifpnames){
	int ret = 0;
	int i;
	for(i=0;i<select_list.size();++i)
		ret += count_se_ifp_refs(select_list[i]->se,ifpnames);
	for(i=0;i<prefilter[0].size();++i)
		ret += count_pr_ifp_refs(prefilter[0][i]->pr,ifpnames);
	for(i=0;i<prefilter[1].size();++i)
		ret += count_pr_ifp_refs(prefilter[1][i]->pr,ifpnames);
	for(i=0;i<temporal_eq.size();++i)
		ret += count_pr_ifp_refs(temporal_eq[i]->pr,ifpnames);
	for(i=0;i<hash_eq.size();++i)
		ret += count_pr_ifp_refs(hash_eq[i]->pr,ifpnames);
	for(i=0;i<postfilter.size();++i)
		ret += count_pr_ifp_refs(postfilter[i]->pr,ifpnames);
	return ret;
}

int filter_join_qpn::count_ifp_refs(set<string> &ifpnames){
	int ret = 0;
	int i;
	for(i=0;i<select_list.size();++i)
		ret += count_se_ifp_refs(select_list[i]->se,ifpnames);
	for(i=0;i<where.size();++i)
		ret += count_pr_ifp_refs(where[i]->pr,ifpnames);
	return ret;
}


//		Resolve interface params to string literals
int filter_join_qpn::resolve_if_params( ifq_t *ifdb, string &err){
	int ret = 0;
	int i;
	string ifname = from[0]->get_interface();
	string ifmach = from[0]->get_machine();
	for(i=0;i<select_list.size();++i)
		if( resolve_se_ifp_refs(select_list[i]->se,ifmach, ifname, ifdb, err) )
			ret = 1;
	for(i=0;i<where.size();++i)
		if( resolve_pr_ifp_refs(where[i]->pr,ifmach, ifname, ifdb, err))
			ret = 1;
	return ret;
}


int spx_qpn::resolve_if_params( ifq_t *ifdb, string &err){
	int ret = 0;
	int i;
	string ifname = table_name->get_interface();
	string ifmach = table_name->get_machine();
	for(i=0;i<select_list.size();++i)
		if( resolve_se_ifp_refs(select_list[i]->se,ifmach, ifname, ifdb, err) )
			ret = 1;
	for(i=0;i<where.size();++i)
		if( resolve_pr_ifp_refs(where[i]->pr,ifmach, ifname, ifdb, err))
			ret = 1;
	return ret;
}

int sgah_qpn::resolve_if_params( ifq_t *ifdb, string &err){
	int ret = 0;
	int i,j;
	string ifname = table_name->get_interface();
	string ifmach = table_name->get_machine();

//printf("Select list has %d elements\n",select_list.size());
	for(i=0;i<select_list.size();++i){
//printf("\tresolving elemet %d\n",i);
		if( resolve_se_ifp_refs(select_list[i]->se,ifmach, ifname, ifdb, err) ){
			ret = 1;
		}
	}
	for(i=0;i<where.size();++i){
		if( resolve_pr_ifp_refs(where[i]->pr,ifmach, ifname, ifdb, err) )
			ret = 1;
	}
	for(i=0;i<having.size();++i){
		if( resolve_pr_ifp_refs(having[i]->pr,ifmach, ifname, ifdb, err) )
			ret = 1;
	}
//printf("aggr list has %d elements\n",select_list.size());
	for(i=0;i<aggr_tbl.size();++i){
//printf("\tresolving elemet %d\n",i);
		if(aggr_tbl.is_builtin(i) && !aggr_tbl.is_star_aggr(i)){
//printf("\t\t\tbuiltin\n");
			if( resolve_se_ifp_refs(aggr_tbl.get_aggr_se(i),ifmach, ifname, ifdb, err) )
					ret = 1;
		}else{
//printf("\t\t\tudaf\n");
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(i);
			for(j=0;j<opl.size();++j)
				if( resolve_se_ifp_refs(opl[j],ifmach, ifname, ifdb, err) )
					ret = 1;
		}
	}
	for(i=0;i<gb_tbl.size();++i){
		if( resolve_se_ifp_refs(gb_tbl.get_def(i), ifmach, ifname, ifdb, err) )
			ret = 1;
	}
	return ret;
}



/*
	SPLITTING A SELECTION_PROJECTION OPERATOR

	An SPX node may reference:
		literals, parameters, colrefs, functions, operators
	An SPX node may not reference:
		group-by variables, aggregates

	An SPX node contains
		selection list of SEs
		where list of CNF predicates

	Algorithm:
		If each selection SE and each where predicate is fta-safe
			execute entire operator as an LFTA.
		Else
			for each predicate in the where clause
			  if it is fta safe, execute it in the lfta
			  else, split each SE in the predicate, evaluate the
				top-level SEs in the hfta and eval the predicate on that.
			For each SE in the se list
			  Split the SE, eval the high level part, push onto hfta
				selection list

	Splitting an SE:
		A SE represents a value which must be computed.  The LFTA
		must provide sub-values from which the HFTA can compute the
		desired value.
		1) the SE is fta-safe
			Create an entry in the selection list of the LFTA which is
			the SE itself.  Reference this LFTA selection list entry in
			the HFTA (via a field name assigned to the lfta selection
			list entry).
		2) The SE is not fta-safe
			Determine the boundary between the fta-safe and the fta-unsafe
			portions of the SE.  The result is a rooted tree (which is
			evaluated at the HFTA) which references sub-SEs (which are
			evaluated at the LFTA).  Each of the sub-SEs is placed on
			the selection list of the LFTA and assigned field names,
			the top part is evaluated at the HFTA and references the
			sub-SEs through their assigned field names.
		The only SEs on the LFTA selection list are those created by
		the above mechanism.  The collection of assigned field names becomes
		the schema of the LFTA.

		TODO: insert tablevar names into the colrefs.

*/

vector<qp_node *> spx_qpn::split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx){

	int i;
	vector<qp_node *> ret_vec;

//			If the node reads from a stream, don't split.
//	int t = Schema->get_table_ref(table_name->get_schema_name());
	int t = table_name->get_schema_ref();
	if(Schema->get_schema_type(t) != PROTOCOL_SCHEMA){
		hfta_returned = 1;
		ret_vec.push_back(this);
		return(ret_vec);
	}


//			Get the set of interfaces it accesses.
	int ierr;
	int si;
	vector<string> sel_names;
	vector<pair<string,string> > ifaces = get_ifaces(table_name, ifdb, n_virtual_ifaces, hfta_parallelism, hfta_idx);
	if (ifaces.empty()) {
		fprintf(stderr,"INTERNAL ERROR in spx_qpn::split_node_for_fta - empty interface set\n");
		exit(1);
	}


//			The FTA node, it is always returned.

	spx_qpn *fta_node = new spx_qpn();
		fta_node->table_name = table_name;

//			for colname imputation
//	vector<string> fta_flds, stream_flds;


//		First check if the query can be pushed to the FTA.
	bool fta_ok = true;
	int s;
	for(s=0;s<select_list.size();s++){
		fta_ok &= check_fta_forbidden_se(select_list[s]->se,NULL, Ext_fcns);
	}
	int p;
	for(p=0;p<where.size();p++){
		fta_ok &= check_fta_forbidden_pr(where[p]->pr,NULL, Ext_fcns);
	}

	if(fta_ok){
////////////////////////////////////////////////////////////
//			The query can be executed entirely in the FTA.
		hfta_returned = 0;

		for(si=0;si<ifaces.size();++si){
			fta_node = new spx_qpn();

//			Name the fta
			if(ifaces.size()==1)
				fta_node->set_node_name( node_name );
			else{
				string new_name =  "_"+node_name+"_"+ifaces[si].first+"_"+ifaces[si].second;
				untaboo(new_name);
				fta_node->set_node_name(new_name);
			}
			sel_names.push_back(fta_node->get_node_name());

//			Assign the table
			fta_node->table_name = table_name->duplicate();
			fta_node->table_name->set_machine(ifaces[si].first);
			fta_node->table_name->set_interface(ifaces[si].second);
			fta_node->table_name->set_ifq(false);

			for(s=0;s<select_list.size();s++){
				fta_node->select_list.push_back( dup_select(select_list[s], NULL) );
			}
			for(p=0;p<where.size();p++){
				predicate_t *new_pr = dup_pr(where[p]->pr, NULL);
				cnf_elem *new_cnf = new cnf_elem(new_pr);
				analyze_cnf(new_cnf);

				fta_node->where.push_back(new_cnf);
			}

//			Xfer all of the parameters.
//			Use existing handle annotations.
			vector<string> param_names = param_tbl->get_param_names();
			int pi;
			for(pi=0;pi<param_names.size();pi++){
				data_type *dt = param_tbl->get_data_type(param_names[pi]);
				fta_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
			}
			fta_node->definitions = definitions;
			if(fta_node->resolve_if_params(ifdb, this->err_str)){
				this->error_code = 3;
				return ret_vec;
			}

			ret_vec.push_back(fta_node);
		}

		if(ifaces.size() > 1){
		spx_qpn *tmp_spx = (spx_qpn *)(ret_vec[0]);
			mrg_qpn *mrg_node = new mrg_qpn(tmp_spx,
				 node_name,  sel_names,ifaces, ifdb);
			/*
			Do not split sources until we are done with optimizations
			vector<mrg_qpn *> split_merge = mrg_node->split_sources();
			for(i=0;i<split_merge.size();++i){
				ret_vec.push_back(split_merge[i]);
			}
			hfta_returned = split_merge.size();
			*/
			ret_vec.push_back(mrg_node);
			hfta_returned = 1;
		}


// printf("OK as FTA.\n");
// printf("FTA node is:\n%s\n\n",fta_node->to_query_string().c_str() );

		return(ret_vec);
	}

////////////////////////////////////////////////////
//			The fta must be split.  Create a stream node.
//			NOTE : I am counting on the single
//			table in the from list.  (Joins handled in a different operator).

	hfta_returned = 1;

	spx_qpn *stream_node = new spx_qpn();
	stream_node->set_node_name( node_name );
//		Create the tablevar in the stream's FROM clause.
//		set the schema name to the name of the LFTA,
//		and use the same tablevar name.
	stream_node->table_name = new tablevar_t(
			 ("_fta_"+node_name).c_str()
	 );
	stream_node->table_name->set_range_var(table_name->get_var_name());

//			Name the fta
	fta_node->set_node_name( "_fta_"+node_name );

//			table var names of fta, stream.
    string fta_var = fta_node->table_name->get_var_name();
    string stream_var = stream_node->table_name->get_var_name();

//			Set up select list vector
	vector< vector<select_element *> *> select_vec;
	select_vec.push_back(&(fta_node->select_list)); // only one child


//			Split the select list into its FTA and stream parts.
//			If any part of the SE is fta-unsafe, it will return
//			a SE to execute at the stream ref'ing SE's evaluated
//			at the fta (which are put on the FTA's select list as a side effect).
//			If the SE is fta-safe, put it on the fta select list, make
//			a ref to it and put the ref on the stream select list.
	for(s=0;s<select_list.size();s++){
		bool fta_forbidden = false;
		int se_src = SPLIT_FTAVEC_NOTBLVAR;
//		scalarexp_t *root_se = split_fta_se(
//			select_list[s]->se,fta_forbidden, fta_node->select_list, Ext_fcns
//		);
		scalarexp_t *root_se = split_ftavec_se( select_list[s]->se,
					fta_forbidden, se_src, select_vec, Ext_fcns
		);
//		if(fta_forbidden){
		if(fta_forbidden || se_src == SPLIT_FTAVEC_NOTBLVAR){
			stream_node->select_list.push_back(
				new select_element(root_se, select_list[s]->name)
			);
		}else{
			scalarexp_t *new_se=make_fta_se_ref(fta_node->select_list,root_se,0);
			stream_node->select_list.push_back(
				new select_element(new_se, select_list[s]->name)
			);
		}
	}


//		The WHERE clause has already been split into a set of clauses
//		that are ANDED together.  For each clause, check if its FTA-safe.
//		If not, split its SE's into fta-safe and stream-executing parts,
//		then put a clause which ref's the SEs into the stream.
//		Else put it into the LFTA.
	predicate_t *pr_root;
	bool fta_forbidden;
	for(p=0;p<where.size();p++){
		if(! check_fta_forbidden_pr(where[p]->pr, NULL, Ext_fcns) ){
			pr_root = split_ftavec_pr(where[p]->pr,select_vec,Ext_fcns);
//			pr_root = split_fta_pr( where[p]->pr, fta_node->select_list, Ext_fcns);
			fta_forbidden = true;
		}else{
			pr_root = dup_pr(where[p]->pr, NULL);
			fta_forbidden = false;
		}
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);

		if(fta_forbidden){
			stream_node->where.push_back(cnf_root);
		}else{
			fta_node->where.push_back(cnf_root);
		}
	}



//			Divide the parameters among the stream, FTA.
//			Currently : assume that the stream receives all parameters
//			and parameter updates, incorporates them, then passes
//			all of the parameters to the FTA.
//			This will need to change (tables, fta-unsafe types. etc.)

//			I will pass on the use_handle_access marking, even
//			though the fcn call that requires handle access might
//			exist in only one of the parts of the query.
//			Parameter manipulation and handle access determination will
//			need to be revisited anyway.
	vector<string> param_names = param_tbl->get_param_names();
	int pi;
	for(pi=0;pi<param_names.size();pi++){
		data_type *dt = param_tbl->get_data_type(param_names[pi]);
		fta_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
		stream_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
	}

	fta_node->definitions = definitions; fta_node->definitions.erase("_referenced_ifaces");
	stream_node->definitions = definitions;

//		Now split by interfaces
	if(ifaces.size() > 1){
		for(si=0;si<ifaces.size();++si){
			spx_qpn *subq_node = new spx_qpn();

//			Name the subquery
			string new_name =  "_"+node_name+"_"+ifaces[si].first+"_"+ifaces[si].second;
			untaboo(new_name);
			subq_node->set_node_name( new_name) ;
			sel_names.push_back(subq_node->get_node_name());

//			Assign the table
			subq_node->table_name = fta_node->table_name->duplicate();
			subq_node->table_name->set_machine(ifaces[si].first);
			subq_node->table_name->set_interface(ifaces[si].second);
			subq_node->table_name->set_ifq(false);

			for(s=0;s<fta_node->select_list.size();s++){
				subq_node->select_list.push_back( dup_select(fta_node->select_list[s], NULL) );
			}
			for(p=0;p<fta_node->where.size();p++){
				predicate_t *new_pr = dup_pr(fta_node->where[p]->pr, NULL);
				cnf_elem *new_cnf = new cnf_elem(new_pr);
				analyze_cnf(new_cnf);

				subq_node->where.push_back(new_cnf);
			}
//			Xfer all of the parameters.
//			Use existing handle annotations.
			vector<string> param_names = param_tbl->get_param_names();
			int pi;
			for(pi=0;pi<param_names.size();pi++){
				data_type *dt = param_tbl->get_data_type(param_names[pi]);
				subq_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
			}
			if(subq_node->resolve_if_params(ifdb, this->err_str)){
				this->error_code = 3;
				return ret_vec;
			}
			subq_node->definitions = definitions; subq_node->definitions.erase("_referenced_ifaces");

			ret_vec.push_back(subq_node);
		}

		mrg_qpn *mrg_node = new mrg_qpn((spx_qpn *)(ret_vec[0]),
			 fta_node->node_name, sel_names, ifaces, ifdb);
		/*
		Do not split sources until we are done with optimizations
		vector<mrg_qpn *> split_merge = mrg_node->split_sources();
		for(i=0;i<split_merge.size();++i){
			ret_vec.push_back(split_merge[i]);
		}
		*/
		ret_vec.push_back(mrg_node);
		ret_vec.push_back(stream_node);
		hfta_returned = 1/*split_merge.size()*/ + 1;

	}else{
		fta_node->table_name->set_machine(ifaces[0].first);
		fta_node->table_name->set_interface(ifaces[0].second);
		fta_node->table_name->set_ifq(false);
		if(fta_node->resolve_if_params(ifdb, this->err_str)){
			this->error_code = 3;
			return ret_vec;
		}
		ret_vec.push_back(fta_node);
		ret_vec.push_back(stream_node);
		hfta_returned = 1;
	}

// printf("FTA node is:\n%s\n\n",fta_node->to_query_string().c_str() );
// printf("Stream node is:\n%s\n\n",stream_node->to_query_string().c_str() );


	return(ret_vec);
}


/*
	Splitting a aggregation+sampling operator.
    right now, return an error if any splitting is required.
*/

vector<qp_node *> sgahcwcb_qpn::split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx){

	hfta_returned = 1;

	vector<qp_node *> ret_vec;
	int s, p, g, a, o, i;
	int si;

	vector<string> fta_flds, stream_flds;

//			If the node reads from a stream, don't split.
//	int t = Schema->get_table_ref(table_name->get_schema_name());
	int t = table_name->get_schema_ref();
	if(Schema->get_schema_type(t) != PROTOCOL_SCHEMA){
		ret_vec.push_back(this);
		return(ret_vec);
	}

	fprintf(stderr,"ERROR : cannot split a sampling operator (not yet implemented).\n");
	exit(1);

	return ret_vec;


}


/*
	Splitting a running aggregation operator.
    The code is almost identical to that of the the sgah operator
    except that
       - there is no lfta-only option.
	   - the stream node is rsagh_qpn (lfta is sgah or spx)
	   - need to handle the closing when (similar to having)
*/

vector<qp_node *> rsgah_qpn::split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx){

	hfta_returned = 1;

	vector<qp_node *> ret_vec;
	int s, p, g, a, o, i;
	int si;

	vector<string> fta_flds, stream_flds;

//			If the node reads from a stream, don't split.
//	int t = Schema->get_table_ref(table_name->get_schema_name());
	int t = table_name->get_schema_ref();
	if(Schema->get_schema_type(t) != PROTOCOL_SCHEMA){
		ret_vec.push_back(this);
		return(ret_vec);
	}

//			Get the set of interfaces it accesses.
	int ierr;
	vector<string> sel_names;
	vector<pair<string,string> > ifaces = get_ifaces(table_name, ifdb, n_virtual_ifaces, hfta_parallelism, hfta_idx);
	if (ifaces.empty()) {
		fprintf(stderr,"INTERNAL ERROR in rsgah_qpn::split_node_for_fta - empty interface set\n");
		exit(1);
	}




//////////////////////////////////////////////////////////////
///			Split into lfta, hfta.

//			A rsgah node must always be split,
//			if for no other reason than to complete the
//			partial aggregation.

//			First, determine if the query can be spit into aggr/aggr,
//			or if it must be selection/aggr.
//			Splitting into selection/aggr is allowed only
//			if select_lfta is set.


	bool select_allowed = definitions.count("select_lfta")>0;
	bool select_rqd = false;

	set<int> unsafe_gbvars;		// for processing where clause
	for(g=0;g<gb_tbl.size();g++){
		if(! check_fta_forbidden_se(gb_tbl.get_def(g), &aggr_tbl, Ext_fcns)){
			if(!select_allowed){
			  sprintf(tmpstr,"ERROR in rsgah_qpn::split_node_for_fta : group by attribute (%s) has LFTA-unsafe definition but select_lfta is not enabled (%s).\n",
				gb_tbl.get_name(g).c_str(), se_to_query_string(gb_tbl.get_def(g), &aggr_tbl).c_str()
			  );
			  this->error_code = 1;
			  this->err_str = tmpstr;
			  return(ret_vec);
			}else{
			  select_rqd = true;
			  unsafe_gbvars.insert(g);
			}
		}
	}

//			Verify that the SEs in the aggregate definitions are fta-safe
	for(a=0;a<aggr_tbl.size();++a){
		scalarexp_t *ase = aggr_tbl.get_aggr_se(a);
		if(ase != NULL){	// COUNT(*) does not have a SE.
		  if(!select_allowed){
		    if(! check_fta_forbidden_se(aggr_tbl.get_aggr_se(a), &aggr_tbl, Ext_fcns)){
			  sprintf(tmpstr,"ERROR in rsgah_qpn::split_node_for_fta : aggregate (%s) has FTA-unsafe scalar expression but select_lfta is not enabled (%s).\n",
				aggr_tbl.get_op(a).c_str(), se_to_query_string(aggr_tbl.get_aggr_se(a), &aggr_tbl).c_str()
			  );
			  this->error_code = 1;
			  this->err_str = tmpstr;
			  return(ret_vec);
	  	    }
		  }else{
			select_rqd = true;
		  }
		}
	}

//			Verify that all of the ref'd UDAFs can be split.

	for(a=0;a<aggr_tbl.size();++a){
		if(! aggr_tbl.is_builtin(a)){
			int afcn = aggr_tbl.get_fcn_id(a);
			int super_id = Ext_fcns->get_superaggr_id(afcn);
			int sub_id = Ext_fcns->get_subaggr_id(afcn);
			if(super_id < 0 || sub_id < 0){
		  	  if(!select_allowed){
				this->err_str += "ERROR in rsgah_qpn::split_node_for_fta : UDAF "+aggr_tbl.get_op(a)+" doesn't have sub/super UDAFS so it can't be split, but select_lfta is not enabled.\n";
				this->error_code = 1;
				return(ret_vec);
			  }else{
				select_rqd = true;
			  }
			}
		}
    }

	for(p=0;p<where.size();p++){
		if(! check_fta_forbidden_pr(where[p]->pr, &aggr_tbl, Ext_fcns) ){
		  if(!select_allowed){
			sprintf(tmpstr,"ERROR in rsgah_qpn::split_node_for_fta : all of the WHERE predicate must be FTA-safe, but select_lfta is not enabled (%s).\n",
				pred_to_query_str(where[p]->pr,&aggr_tbl).c_str()
			);
			this->error_code = 1;
			this->err_str = tmpstr;
			return(ret_vec);
		  }else{
			select_rqd = true;
		  }
		}
	}


	if(! select_rqd){

/////////////////////////////////////////////////////
//			Split into  aggr/aggr.





	sgah_qpn *fta_node = new sgah_qpn();
		fta_node->table_name = table_name;
		fta_node->set_node_name( "_fta_"+node_name );
		fta_node->table_name->set_range_var(table_name->get_var_name());


	rsgah_qpn *stream_node = new rsgah_qpn();
		stream_node->table_name = new tablevar_t(  ("_fta_"+node_name).c_str());
		stream_node->set_node_name( node_name );
		stream_node->table_name->set_range_var(table_name->get_var_name());

//			First, process the group-by variables.
//			The fta must supply the values of all the gbvars.
//			If a gb is computed, the computation must be
//			performed at the FTA, so the SE must be FTA-safe.
//			Nice side effect : the gbvar table contains
//			matching entries for the original query, the lfta query,
//			and the hfta query.  So gbrefs in the new queries are set
//			correctly just by inheriting the gbrefs from the old query.
//			If this property changed, I'll need translation tables.


	for(g=0;g<gb_tbl.size();g++){
//			Insert the gbvar into the lfta.
		scalarexp_t *gbvar_def = dup_se(gb_tbl.get_def(g), &aggr_tbl);
		fta_node->gb_tbl.add_gb_var(
			gb_tbl.get_name(g), gb_tbl.get_tblvar_ref(g), gbvar_def, gb_tbl.get_reftype(g)
		);

//			Insert a ref to the value of the gbvar into the lfta select list.
		colref_t *new_cr = new colref_t(gb_tbl.get_name(g).c_str() );
		scalarexp_t *gbvar_fta = new scalarexp_t(new_cr);
 		gbvar_fta->set_gb_ref(g);
		gbvar_fta->set_data_type( gb_tbl.get_def(g)->get_data_type() );
		scalarexp_t *gbvar_stream = make_fta_se_ref(fta_node->select_list, gbvar_fta,0);

//			Insert the corresponding gbvar ref (gbvar_stream) into the stream.
 		gbvar_stream->set_gb_ref(-1);	// used as GBvar def
		stream_node->gb_tbl.add_gb_var(
			gbvar_stream->get_colref()->get_field(), -1, gbvar_stream,  gb_tbl.get_reftype(g)
		);

	}

//			SEs in the aggregate definitions.
//			They are all safe, so split them up for later processing.
	map<int, scalarexp_t *> hfta_aggr_se;
	for(a=0;a<aggr_tbl.size();++a){
		split_fta_aggr( &(aggr_tbl), a,
						&(stream_node->aggr_tbl), &(fta_node->aggr_tbl)  ,
						fta_node->select_list,
						hfta_aggr_se,
						Ext_fcns
					);
	}


//			Next, the select list.

	for(s=0;s<select_list.size();s++){
		bool fta_forbidden = false;
		scalarexp_t *root_se = rehome_fta_se(select_list[s]->se, &hfta_aggr_se);
		stream_node->select_list.push_back(
			new select_element(root_se, select_list[s]->name));
	}



//			All the predicates in the where clause must execute
//			in the fta.

	for(p=0;p<where.size();p++){
		predicate_t *new_pr = dup_pr(where[p]->pr, &aggr_tbl);
		cnf_elem *new_cnf = new cnf_elem(new_pr);
		analyze_cnf(new_cnf);

		fta_node->where.push_back(new_cnf);
	}

//			All of the predicates in the having clause must
//			execute in the stream node.

	for(p=0;p<having.size();p++){
		predicate_t *pr_root = rehome_fta_pr( having[p]->pr,  &hfta_aggr_se);
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);

		stream_node->having.push_back(cnf_root);
	}

//			All of the predicates in the closing when clause must
//			execute in the stream node.

	for(p=0;p<closing_when.size();p++){
		predicate_t *pr_root=rehome_fta_pr(closing_when[p]->pr,&hfta_aggr_se);
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);

		stream_node->closing_when.push_back(cnf_root);
	}


//			Divide the parameters among the stream, FTA.
//			Currently : assume that the stream receives all parameters
//			and parameter updates, incorporates them, then passes
//			all of the parameters to the FTA.
//			This will need to change (tables, fta-unsafe types. etc.)

//			I will pass on the use_handle_access marking, even
//			though the fcn call that requires handle access might
//			exist in only one of the parts of the query.
//			Parameter manipulation and handle access determination will
//			need to be revisited anyway.
	vector<string> param_names = param_tbl->get_param_names();
	int pi;
	for(pi=0;pi<param_names.size();pi++){
		data_type *dt = param_tbl->get_data_type(param_names[pi]);
		fta_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
		stream_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
	}
	fta_node->definitions = definitions; fta_node->definitions.erase("_referenced_ifaces");
	stream_node->definitions = definitions;

//		Now split by interfaces XXXX
	if(ifaces.size() > 1){
		for(si=0;si<ifaces.size();++si){
			sgah_qpn *subq_node = new sgah_qpn();

//			Name the subquery
			string new_name =  "_"+node_name+"_"+ifaces[si].first+"_"+ifaces[si].second;
			untaboo(new_name);
			subq_node->set_node_name( new_name) ;
			sel_names.push_back(subq_node->get_node_name());

//			Assign the table
			subq_node->table_name = fta_node->table_name->duplicate();
			subq_node->table_name->set_machine(ifaces[si].first);
			subq_node->table_name->set_interface(ifaces[si].second);
			subq_node->table_name->set_ifq(false);

//			the GB vars.
			for(g=0;g<fta_node->gb_tbl.size();g++){
//			Insert the gbvar into the lfta.
				scalarexp_t *gbvar_def = dup_se(fta_node->gb_tbl.get_def(g), NULL);
				subq_node->gb_tbl.add_gb_var(
					fta_node->gb_tbl.get_name(g), fta_node->gb_tbl.get_tblvar_ref(g), gbvar_def, fta_node->gb_tbl.get_reftype(g)
				);
			}

//			Insert the aggregates
			for(a=0;a<fta_node->aggr_tbl.size();++a){
				subq_node->aggr_tbl.add_aggr(fta_node->aggr_tbl.duplicate(a));
			}

			for(s=0;s<fta_node->select_list.size();s++){
				subq_node->select_list.push_back( dup_select(fta_node->select_list[s], NULL) );
			}
			for(p=0;p<fta_node->where.size();p++){
				predicate_t *new_pr = dup_pr(fta_node->where[p]->pr, NULL);
				cnf_elem *new_cnf = new cnf_elem(new_pr);
				analyze_cnf(new_cnf);

				subq_node->where.push_back(new_cnf);
			}
			for(p=0;p<fta_node->having.size();p++){
				predicate_t *new_pr = dup_pr(fta_node->having[p]->pr, NULL);
				cnf_elem *new_cnf = new cnf_elem(new_pr);
				analyze_cnf(new_cnf);

				subq_node->having.push_back(new_cnf);
			}
//			Xfer all of the parameters.
//			Use existing handle annotations.
			vector<string> param_names = param_tbl->get_param_names();
			int pi;
			for(pi=0;pi<param_names.size();pi++){
				data_type *dt = param_tbl->get_data_type(param_names[pi]);
				subq_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
			}
			subq_node->definitions = definitions; subq_node->definitions.erase("_referenced_ifaces");
			if(subq_node->resolve_if_params(ifdb, this->err_str)){
				this->error_code = 3;
				return ret_vec;
			}

			ret_vec.push_back(subq_node);
		}

		mrg_qpn *mrg_node = new mrg_qpn((sgah_qpn *)(ret_vec[0]),
			 fta_node->node_name, sel_names, ifaces, ifdb);

		/*
		Do not split sources until we are done with optimizations
		vector<mrg_qpn *> split_merge = mrg_node->split_sources();
		for(i=0;i<split_merge.size();++i){
			ret_vec.push_back(split_merge[i]);
		}
		*/
		ret_vec.push_back(mrg_node);
		ret_vec.push_back(stream_node);
		hfta_returned = 1/*split_merge.size()*/+1;

	}else{
		fta_node->table_name->set_machine(ifaces[0].first);
		fta_node->table_name->set_interface(ifaces[0].second);
		fta_node->table_name->set_ifq(false);
		if(fta_node->resolve_if_params(ifdb, this->err_str)){
			this->error_code = 3;
			return ret_vec;
		}
		ret_vec.push_back(fta_node);
		ret_vec.push_back(stream_node);
		hfta_returned = 1;
	}


//	ret_vec.push_back(fta_node);
//	ret_vec.push_back(stream_node);


	return(ret_vec);

	}

/////////////////////////////////////////////////////////////////////
///		Split into selection LFTA, aggregation HFTA.

	spx_qpn *fta_node = new spx_qpn();
		fta_node->table_name = table_name;
		fta_node->set_node_name( "_fta_"+node_name );
		fta_node->table_name->set_range_var(table_name->get_var_name());


	rsgah_qpn *stream_node = new rsgah_qpn();
		stream_node->table_name = new tablevar_t( ("_fta_"+node_name).c_str());
		stream_node->set_node_name( node_name );
		stream_node->table_name->set_range_var(table_name->get_var_name());


	vector< vector<select_element *> *> select_vec;
	select_vec.push_back(&(fta_node->select_list)); // only one child

//			Process the gbvars.  Split their defining SEs.
	for(g=0;g<gb_tbl.size();g++){
		bool fta_forbidden = false;
		int se_src = SPLIT_FTAVEC_NOTBLVAR;

		scalarexp_t *gbvar_se = split_ftavec_se( gb_tbl.get_def(g),
					fta_forbidden, se_src, select_vec, Ext_fcns
		);
//		if(fta_forbidden) (
		if(fta_forbidden || se_src == SPLIT_FTAVEC_NOTBLVAR){
			stream_node->gb_tbl.add_gb_var(
			  gb_tbl.get_name(g),gb_tbl.get_tblvar_ref(g),gbvar_se,gb_tbl.get_reftype(g)
			);
		}else{
			scalarexp_t *new_se=make_fta_se_ref(fta_node->select_list,gbvar_se,0);
			stream_node->gb_tbl.add_gb_var(
			  gb_tbl.get_name(g),gb_tbl.get_tblvar_ref(g),new_se,gb_tbl.get_reftype(g)
			);
		}
	}

//		Process the aggregate table.
//		Copy to stream, split the SEs.
	map<int, scalarexp_t *> hfta_aggr_se;	// for rehome
	for(a=0;a<aggr_tbl.size();++a){
		scalarexp_t *hse;
		if(aggr_tbl.is_builtin(a)){
			if(aggr_tbl.is_star_aggr(a)){
				stream_node->aggr_tbl.add_aggr(aggr_tbl.get_op(a),NULL, false);
				hse=scalarexp_t::make_star_aggr(aggr_tbl.get_op(a).c_str());
			}else{
				bool fta_forbidden = false;
				int se_src = SPLIT_FTAVEC_NOTBLVAR;

				scalarexp_t *agg_se = split_ftavec_se( aggr_tbl.get_aggr_se(a),
					fta_forbidden, se_src, select_vec, Ext_fcns
				);
//				if(fta_forbidden) (
				if(fta_forbidden || se_src == SPLIT_FTAVEC_NOTBLVAR){
					stream_node->aggr_tbl.add_aggr(aggr_tbl.get_op(a), agg_se,false);
					hse=scalarexp_t::make_se_aggr(aggr_tbl.get_op(a).c_str(),agg_se);
				}else{
			  		scalarexp_t *new_se=make_fta_se_ref(fta_node->select_list,agg_se,0);
					stream_node->aggr_tbl.add_aggr(aggr_tbl.get_op(a), new_se,false);
					hse=scalarexp_t::make_se_aggr(aggr_tbl.get_op(a).c_str(),new_se);
				}
			}
			hse->set_data_type(aggr_tbl.get_data_type(a));
			hse->set_aggr_id(a);
			hfta_aggr_se[a]=hse;
		}else{
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(a);
			vector<scalarexp_t *> new_opl;
			for(o=0;o<opl.size();++o){
				bool fta_forbidden = false;
				int se_src = SPLIT_FTAVEC_NOTBLVAR;
				scalarexp_t *agg_se = split_ftavec_se( opl[o],
					fta_forbidden, se_src, select_vec, Ext_fcns
				);
//				scalarexp_t *agg_se = split_ftavec_se( aggr_tbl.get_aggr_se(a),
//					fta_forbidden, se_src, select_vec, Ext_fcns
//				);
//				if(fta_forbidden) (
				if(fta_forbidden || se_src == SPLIT_FTAVEC_NOTBLVAR){
					new_opl.push_back(agg_se);
				}else{
			  		scalarexp_t *new_se=make_fta_se_ref(fta_node->select_list,agg_se,0);
					new_opl.push_back(new_se);
				}
			}
			stream_node->aggr_tbl.add_aggr(aggr_tbl.get_op(a), aggr_tbl.get_fcn_id(a), new_opl, aggr_tbl.get_storage_type(a),false, false,aggr_tbl.has_bailout(a));
			hse = new scalarexp_t(aggr_tbl.get_op(a).c_str(),new_opl);
			hse->set_data_type(Ext_fcns->get_fcn_dt(aggr_tbl.get_fcn_id(a)));
			hse->set_fcn_id(aggr_tbl.get_fcn_id(a));
			hse->set_aggr_id(a);
			hfta_aggr_se[a]=hse;
		}
	}


//		Process the WHERE clause.
//		If it is fta-safe AND it refs only fta-safe gbvars,
//		then expand the gbvars and put it into the lfta.
//		Else, split it into an hfta predicate ref'ing
//		se's computed partially in the lfta.

	predicate_t *pr_root;
	bool fta_forbidden;
	for(p=0;p<where.size();p++){
		if(! check_fta_forbidden_pr(where[p]->pr, NULL, Ext_fcns) || contains_gb_pr(where[p]->pr, unsafe_gbvars) ){
			pr_root = split_ftavec_pr(where[p]->pr,select_vec,Ext_fcns);
			fta_forbidden = true;
		}else{
			pr_root = dup_pr(where[p]->pr, NULL);
			expand_gbvars_pr(pr_root, gb_tbl);
			fta_forbidden = false;
		}
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);

		if(fta_forbidden){
			stream_node->where.push_back(cnf_root);
		}else{
			fta_node->where.push_back(cnf_root);
		}
	}


//		Process the Select clause, rehome it on the
//		new defs.
	for(s=0;s<select_list.size();s++){
		bool fta_forbidden = false;
		scalarexp_t *root_se = rehome_fta_se(select_list[s]->se, &hfta_aggr_se);
		stream_node->select_list.push_back(
			new select_element(root_se, select_list[s]->name));
	}


// Process the Having clause

//			All of the predicates in the having clause must
//			execute in the stream node.

	for(p=0;p<having.size();p++){
		predicate_t *pr_root = rehome_fta_pr( having[p]->pr,  &hfta_aggr_se);
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);

		stream_node->having.push_back(cnf_root);
	}
//			Same for closing when
	for(p=0;p<closing_when.size();p++){
		predicate_t *pr_root=rehome_fta_pr(closing_when[p]->pr,&hfta_aggr_se);
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);

		stream_node->closing_when.push_back(cnf_root);
	}


//		Handle parameters and a few last details.
	vector<string> param_names = param_tbl->get_param_names();
	int pi;
	for(pi=0;pi<param_names.size();pi++){
		data_type *dt = param_tbl->get_data_type(param_names[pi]);
		fta_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
		stream_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
	}

	fta_node->definitions = definitions; fta_node->definitions.erase("_referenced_ifaces");
	stream_node->definitions = definitions;

//		Now split by interfaces YYYY
	if(ifaces.size() > 1){
		for(si=0;si<ifaces.size();++si){
			spx_qpn *subq_node = new spx_qpn();

//			Name the subquery
			string new_name =  "_"+node_name+"_"+ifaces[si].first+"_"+ifaces[si].second;
			untaboo(new_name);
			subq_node->set_node_name( new_name) ;
			sel_names.push_back(subq_node->get_node_name());

//			Assign the table
			subq_node->table_name = fta_node->table_name->duplicate();
			subq_node->table_name->set_machine(ifaces[si].first);
			subq_node->table_name->set_interface(ifaces[si].second);
			subq_node->table_name->set_ifq(false);

			for(s=0;s<fta_node->select_list.size();s++){
				subq_node->select_list.push_back( dup_select(fta_node->select_list[s], NULL) );
			}
			for(p=0;p<fta_node->where.size();p++){
				predicate_t *new_pr = dup_pr(fta_node->where[p]->pr, NULL);
				cnf_elem *new_cnf = new cnf_elem(new_pr);
				analyze_cnf(new_cnf);

				subq_node->where.push_back(new_cnf);
			}
//			Xfer all of the parameters.
//			Use existing handle annotations.
			vector<string> param_names = param_tbl->get_param_names();
			int pi;
			for(pi=0;pi<param_names.size();pi++){
				data_type *dt = param_tbl->get_data_type(param_names[pi]);
				subq_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
			}
			subq_node->definitions = definitions; subq_node->definitions.erase("_referenced_ifaces");
			if(subq_node->resolve_if_params(ifdb, this->err_str)){
				this->error_code = 3;
				return ret_vec;
			}

			ret_vec.push_back(subq_node);
		}

		mrg_qpn *mrg_node = new mrg_qpn((spx_qpn *)(ret_vec[0]),
			 fta_node->node_name, sel_names, ifaces, ifdb);
		/*
		Do not split sources until we are done with optimizations
		vector<mrg_qpn *> split_merge = mrg_node->split_sources();
		for(i=0;i<split_merge.size();++i){
			ret_vec.push_back(split_merge[i]);
		}
		*/
		ret_vec.push_back(mrg_node);
		ret_vec.push_back(stream_node);
		hfta_returned = 1/*split_merge.size()*/+1;

	}else{
		fta_node->table_name->set_machine(ifaces[0].first);
		fta_node->table_name->set_interface(ifaces[0].second);
		fta_node->table_name->set_ifq(false);
		if(fta_node->resolve_if_params(ifdb, this->err_str)){
			this->error_code = 3;
			return ret_vec;
		}
		ret_vec.push_back(fta_node);
		ret_vec.push_back(stream_node);
		hfta_returned = 1;
	}

	return(ret_vec);

}


/*
		Splitting an aggregation operator

		An aggregation operator can reference
			literals, parameters, colrefs, group-by vars, aggregates,
			operators, functions

		an aggregation contains
			A selection list of SEs
			A where list of predicates
			A list group-by variable definition
			A list of aggregates to be computed
			A HAVING list of predicates.

		Aggregation involves two phases:
			1) given an input tuple, determine if it satisfies all of
				the WHERE predicates.  If so, compute the group.
				Look up the group, update its aggregates.
			2) given a closed group and its aggregates, determine
				if these values satisfy all of the HAVING predicates.
				If so, evaluate the SEs on the selection list from the
				group and its aggregates.
		The two-phase nature of aggregation places restrictions on
		what can be referenced by different components of the operator
		(in addition to functions and operators).
		- group-by variables : literals, parameters, colrefs
		- WHERE predicates : group-by vars, literals, params, colrefs
		- HAVING predicates : group-by vars, literals, params, aggregates
		- Selection list SEs : group-by vars, literals, params, aggregates

		Splitting an aggregation operator into an LFTA/HFTA part
		involves performing partial aggregation at the LFTA and
		completing the aggregation at the HFTA.
		- given a tuple, the LFTA part evaluates the WHERE clause,
		  and if it is satisfied, computes the group.  lookup the group
		  and update the aggregates.  output the group and its partial
		  aggregates
		- Given a partial aggregate from the LFTA, look up the group and
		  update its aggregates.  When the group is closed, evalute
		  the HAVING clause and the SEs on the selection list.
		THEREFORE the selection list of the LFTA must consist of the
		group-by variables and the set of (bare) subaggregate values
		necessary to compute the super aggregates.
		Unlike the case with the SPX operator, the SE splitting point
		is at the GBvar and the aggregate value level.

		ALGORITHM:
		For each group-by variable
			Put the GB variable definition in the LFTA GBVAR list.
			Put the GBVAR in the LFTA selection list (as an SE).
			Put a reference to that GBVAR in the HFTA GBVAR list.
		For each aggregate
			Split the aggregate into a superaggregate and a subaggregate.
				The SE of the superaggregate references the subaggregate value.
				(this will need modifications for MF aggregation)
		For each SE in the selection list, HAVING predicate
			Make GBVAR references point to the new GBVAR
			make the aggregate value references point to the new aggregates.

		SEs are not so much split as their ref's are changed.

		TODO: insert tablevar names into the colrefs.
*/



vector<qp_node *> sgah_qpn::split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx){

	hfta_returned = 1;

	vector<qp_node *> ret_vec;
	int s, p, g, a, o, i;
	int si;

	vector<string> fta_flds, stream_flds;

//			If the node reads from a stream, don't split.
//	int t = Schema->get_table_ref(table_name->get_schema_name());
	int t = table_name->get_schema_ref();
	if(Schema->get_schema_type(t) != PROTOCOL_SCHEMA){
		ret_vec.push_back(this);
		return(ret_vec);
	}

//			Get the set of interfaces it accesses.
	int ierr;
	vector<string> sel_names;
	vector<pair<string,string> > ifaces = get_ifaces(table_name, ifdb, n_virtual_ifaces, hfta_parallelism, hfta_idx);
	if (ifaces.empty()) {
		fprintf(stderr,"INTERNAL ERROR in sgah_qpn::split_node_for_fta - empty interface set\n");
		exit(1);
	}



//////////////////////////////////////////////
//		Is this LFTA-only?
	if(definitions.count("lfta_aggregation")>0){
//			Yes.  Ensure that everything is lfta-safe.

//			Check only one interface is accessed.
		if(ifaces.size()>1){
			this->err_str = "ERROR, group-by query "+node_name+" is lfta-only, but it accesses more than one interface:\n";
			for(si=0;si<ifaces.size();++si)
				this->err_str += "\t"+ifaces[si].first+"."+ifaces[si].second+"\n";
			this->error_code = 2;
			return(ret_vec);
		}

//			Check the group-by attributes
		for(g=0;g<gb_tbl.size();g++){
			if(! check_fta_forbidden_se(gb_tbl.get_def(g), &aggr_tbl, Ext_fcns)){
				sprintf(tmpstr,"ERROR in sgah_qpn::split_node_for_fta : group by attribute (%s) has LFTA-unsafe definition and the query is lfta-only (%s).\n",
					gb_tbl.get_name(g).c_str(), se_to_query_string(gb_tbl.get_def(g), &aggr_tbl).c_str()
				);
				this->error_code = 1;
				this->err_str = tmpstr;
				return(ret_vec);
			}
		}

//			Verify that the SEs in the aggregate definitions are fta-safe
		for(a=0;a<aggr_tbl.size();++a){
			scalarexp_t *ase = aggr_tbl.get_aggr_se(a);
			if(ase != NULL){	// COUNT(*) does not have a SE.
			  	if(! check_fta_forbidden_se(aggr_tbl.get_aggr_se(a), &aggr_tbl, Ext_fcns)){
					sprintf(tmpstr,"ERROR in sgah_qpn::split_node_for_fta : aggregate (%s) has LFTA-unsafe scalar expression and the query is lfta-only (%s).\n",
						aggr_tbl.get_op(a).c_str(), se_to_query_string(aggr_tbl.get_aggr_se(a), &aggr_tbl).c_str()
					);
					this->error_code = 1;
					this->err_str = tmpstr;
					return(ret_vec);
	  	  		}
			}
			if(! aggr_tbl.fta_legal(a,Ext_fcns)){
			  if(! check_fta_forbidden_se(aggr_tbl.get_aggr_se(a), &aggr_tbl, Ext_fcns)){
				sprintf(tmpstr,"ERROR in sgah_qpn::split_node_for_fta : aggregate (%s) has LFTA-unsafe aggregate and the query is lfta-only (%s).\n",
					aggr_tbl.get_op(a).c_str(), se_to_query_string(aggr_tbl.get_aggr_se(a), &aggr_tbl).c_str()
				);
				this->error_code = 1;
				this->err_str = tmpstr;
				return(ret_vec);
	  	  		}
			}
		}

//		Ensure that all the aggregates are fta-safe ....

//		select list

		for(s=0;s<select_list.size();s++){
			if(! check_fta_forbidden_se(select_list[s]->se,NULL, Ext_fcns)){
				sprintf(tmpstr,"ERROR in sgah_qpn::split_node_for_fta : all of the WHERE predicate must be LFTA-safe and the query is lfta-only (%s).\n",
					pred_to_query_str(where[p]->pr,&aggr_tbl).c_str()
				);
				this->error_code = 1;
				this->err_str = tmpstr;
				return(ret_vec);
			}
		}

// 		where predicate

		for(p=0;p<where.size();p++){
			if(! check_fta_forbidden_pr(where[p]->pr, &aggr_tbl, Ext_fcns) ){
				sprintf(tmpstr,"ERROR in sgah_qpn::split_node_for_fta : all of the WHERE predicate must be LFTA-safe and the query is lfta-only (%s).\n",
					pred_to_query_str(where[p]->pr,&aggr_tbl).c_str()
				);
				this->error_code = 1;
				this->err_str = tmpstr;
				return(ret_vec);
			}
		}


//		having predicate
		if(having.size()>0){
			sprintf(tmpstr,"ERROR in sgah_qpn::split_node_for_fta :  the query is lfta-only, so it can't have a HAVING clause.(%s).\n",
				pred_to_query_str(where[p]->pr,&aggr_tbl).c_str()
			);
			this->error_code = 1;
			this->err_str = tmpstr;
			return(ret_vec);
		}
//			The query is lfta safe, return it.

		hfta_returned = 0;
		ret_vec.push_back(this);
		return(ret_vec);
	}

//////////////////////////////////////////////////////////////
///			Split into lfta, hfta.

//			A sgah node must always be split,
//			if for no other reason than to complete the
//			partial aggregation.

//			First, determine if the query can be spit into aggr/aggr,
//			or if it must be selection/aggr.
//			Splitting into selection/aggr is allowed only
//			if select_lfta is set.


	bool select_allowed = definitions.count("select_lfta")>0;
	bool select_rqd = false;

	set<int> unsafe_gbvars;		// for processing where clause
	for(g=0;g<gb_tbl.size();g++){
		if(! check_fta_forbidden_se(gb_tbl.get_def(g), &aggr_tbl, Ext_fcns)){
			if(!select_allowed){
			  sprintf(tmpstr,"ERROR in sgah_qpn::split_node_for_fta : group by attribute (%s) has LFTA-unsafe definition but select_lfta is not enabled (%s).\n",
				gb_tbl.get_name(g).c_str(), se_to_query_string(gb_tbl.get_def(g), &aggr_tbl).c_str()
			  );
			  this->error_code = 1;
			  this->err_str = tmpstr;
			  return(ret_vec);
			}else{
			  select_rqd = true;
			  unsafe_gbvars.insert(g);
			}
		}
	}

//			Verify that the SEs in the aggregate definitions are fta-safe
	for(a=0;a<aggr_tbl.size();++a){
		scalarexp_t *ase = aggr_tbl.get_aggr_se(a);
		if(ase != NULL){	// COUNT(*) does not have a SE.
		  if(!select_allowed){
		    if(! check_fta_forbidden_se(aggr_tbl.get_aggr_se(a), &aggr_tbl, Ext_fcns)){
			  sprintf(tmpstr,"ERROR in sgah_qpn::split_node_for_fta : aggregate (%s) has FTA-unsafe scalar expression but select_lfta is not enabled (%s).\n",
				aggr_tbl.get_op(a).c_str(), se_to_query_string(aggr_tbl.get_aggr_se(a), &aggr_tbl).c_str()
			  );
			  this->error_code = 1;
			  this->err_str = tmpstr;
			  return(ret_vec);
	  	    }
		  }else{
			select_rqd = true;
		  }
		}
	}

//			Verify that all of the ref'd UDAFs can be split.

	for(a=0;a<aggr_tbl.size();++a){
		if(! aggr_tbl.is_builtin(a)){
			int afcn = aggr_tbl.get_fcn_id(a);
			int super_id = Ext_fcns->get_superaggr_id(afcn);
			int sub_id = Ext_fcns->get_subaggr_id(afcn);
			if(super_id < 0 || sub_id < 0){
		  	  if(!select_allowed){
				this->err_str += "ERROR in sgah_qpn::split_node_for_fta : UDAF "+aggr_tbl.get_op(a)+" doesn't have sub/super UDAFS so it can't be split, but select_lfta is not enabled.\n";
				this->error_code = 1;
				return(ret_vec);
			  }else{
				select_rqd = true;
			  }
			}
		}
    }

	for(p=0;p<where.size();p++){
		if(! check_fta_forbidden_pr(where[p]->pr, &aggr_tbl, Ext_fcns) ){
		  if(!select_allowed){
			sprintf(tmpstr,"ERROR in sgah_qpn::split_node_for_fta : all of the WHERE predicate must be FTA-safe, but select_lfta is not enabled (%s).\n",
				pred_to_query_str(where[p]->pr,&aggr_tbl).c_str()
			);
			this->error_code = 1;
			this->err_str = tmpstr;
			return(ret_vec);
		  }else{
			select_rqd = true;
		  }
		}
	}


	if(! select_rqd){

/////////////////////////////////////////////////////
//			Split into  aggr/aggr.





	sgah_qpn *fta_node = new sgah_qpn();
		fta_node->table_name = table_name;
		fta_node->set_node_name( "_fta_"+node_name );
		fta_node->table_name->set_range_var(table_name->get_var_name());


	sgah_qpn *stream_node = new sgah_qpn();
		stream_node->table_name = new tablevar_t(  ("_fta_"+node_name).c_str());
		stream_node->set_node_name( node_name );
		stream_node->table_name->set_range_var(table_name->get_var_name());

//			allowed stream disorder.  Default is 2,
//			can override with max_lfta_disorder setting.
//			Also limit the hfta disorder, set to lfta disorder + 1.
//			can override with max_hfta_disorder.

	fta_node->lfta_disorder = 2;
	if(this->get_val_of_def("max_lfta_disorder") != ""){
		int d = atoi(this->get_val_of_def("max_lfta_disorder").c_str() );
		if(d<1){
			fprintf(stderr,"Warning, max_lfta_disorder in node %s is %d, must be at least 1, ignoring.\n",node_name.c_str(), d);
		}else{
			fta_node->lfta_disorder = d;
printf("node %s setting lfta_disorder = %d\n",node_name.c_str(),fta_node->lfta_disorder);
		}
	}
	if(fta_node->lfta_disorder > 1)
		stream_node->hfta_disorder = fta_node->lfta_disorder + 1;
	else
		stream_node->hfta_disorder =  1;

	if(this->get_val_of_def("max_hfta_disorder") != ""){
		int d = atoi(this->get_val_of_def("max_hfta_disorder").c_str() );
		if(d<fta_node->lfta_disorder){
			fprintf(stderr,"Warning, max_hfta_disorder in node %s is %d, must be at least the max lfta disorder %d, ignoring.\n",node_name.c_str(), d,fta_node->lfta_disorder);
		}else{
			fta_node->lfta_disorder = d;
		}
		if(fta_node->lfta_disorder < fta_node->hfta_disorder){
			fta_node->hfta_disorder = fta_node->lfta_disorder + 1;
		}
	}

//			First, process the group-by variables.
//			The fta must supply the values of all the gbvars.
//			If a gb is computed, the computation must be
//			performed at the FTA, so the SE must be FTA-safe.
//			Nice side effect : the gbvar table contains
//			matching entries for the original query, the lfta query,
//			and the hfta query.  So gbrefs in the new queries are set
//			correctly just by inheriting the gbrefs from the old query.
//			If this property changed, I'll need translation tables.


	for(g=0;g<gb_tbl.size();g++){
//			Insert the gbvar into the lfta.
		scalarexp_t *gbvar_def = dup_se(gb_tbl.get_def(g), &aggr_tbl);
		fta_node->gb_tbl.add_gb_var(
			gb_tbl.get_name(g), gb_tbl.get_tblvar_ref(g), gbvar_def, gb_tbl.get_reftype(g)
		);

//			Insert a ref to the value of the gbvar into the lfta select list.
		colref_t *new_cr = new colref_t(gb_tbl.get_name(g).c_str() );
		scalarexp_t *gbvar_fta = new scalarexp_t(new_cr);
 		gbvar_fta->set_gb_ref(g);
		gbvar_fta->set_data_type( gb_tbl.get_def(g)->get_data_type() );
		scalarexp_t *gbvar_stream = make_fta_se_ref(fta_node->select_list, gbvar_fta,0);

//			Insert the corresponding gbvar ref (gbvar_stream) into the stream.
 		gbvar_stream->set_gb_ref(-1);	// used as GBvar def
		stream_node->gb_tbl.add_gb_var(
			gbvar_stream->get_colref()->get_field(), -1, gbvar_stream,  gb_tbl.get_reftype(g)
		);
	}
//			multiple aggregation patterns, if any, go with the hfta
	stream_node->gb_tbl.set_pattern_info( &gb_tbl);

//			SEs in the aggregate definitions.
//			They are all safe, so split them up for later processing.
	map<int, scalarexp_t *> hfta_aggr_se;
	for(a=0;a<aggr_tbl.size();++a){
		split_fta_aggr( &(aggr_tbl), a,
						&(stream_node->aggr_tbl), &(fta_node->aggr_tbl)  ,
						fta_node->select_list,
						hfta_aggr_se,
						Ext_fcns
					);
/*
//		OLD TRACING CODE

int ii;
for(ii=0;ii<fta_flds.size() || ii < fta_node->select_list.size();++ii){
	if(ii<fta_flds.size())
		printf("\t%s : ",fta_flds[ii].c_str());
	else
		printf("\t. : ");
	if(ii<fta_node->select_list.size())
		printf("%s\n",fta_node->select_list[ii]->to_string().c_str());
	else
		printf(".\n");
}
printf("hfta aggregates are:");
for(ii=0;ii<stream_node->aggr_tbl.size();++ii){
	printf(" %s",stream_node->aggr_tbl.get_op(ii).c_str());
}
printf("\nlfta aggregates are:");
for(ii=0;ii<fta_node->aggr_tbl.size();++ii){
	printf(" %s",fta_node->aggr_tbl.get_op(ii).c_str());
}
printf("\n\n");
*/

	}


//			Next, the select list.

	for(s=0;s<select_list.size();s++){
		bool fta_forbidden = false;
		scalarexp_t *root_se = rehome_fta_se(select_list[s]->se, &hfta_aggr_se);
		stream_node->select_list.push_back(
			new select_element(root_se, select_list[s]->name));
	}



//			All the predicates in the where clause must execute
//			in the fta.

	for(p=0;p<where.size();p++){
		predicate_t *new_pr = dup_pr(where[p]->pr, &aggr_tbl);
		cnf_elem *new_cnf = new cnf_elem(new_pr);
		analyze_cnf(new_cnf);

		fta_node->where.push_back(new_cnf);
	}

//			All of the predicates in the having clause must
//			execute in the stream node.

	for(p=0;p<having.size();p++){
		predicate_t *pr_root = rehome_fta_pr( having[p]->pr,  &hfta_aggr_se);
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);

		stream_node->having.push_back(cnf_root);
	}


//			Divide the parameters among the stream, FTA.
//			Currently : assume that the stream receives all parameters
//			and parameter updates, incorporates them, then passes
//			all of the parameters to the FTA.
//			This will need to change (tables, fta-unsafe types. etc.)

//			I will pass on the use_handle_access marking, even
//			though the fcn call that requires handle access might
//			exist in only one of the parts of the query.
//			Parameter manipulation and handle access determination will
//			need to be revisited anyway.
	vector<string> param_names = param_tbl->get_param_names();
	int pi;
	for(pi=0;pi<param_names.size();pi++){
		data_type *dt = param_tbl->get_data_type(param_names[pi]);
		fta_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
		stream_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
	}
	fta_node->definitions = definitions; fta_node->definitions.erase("_referenced_ifaces");
	stream_node->definitions = definitions;

//		Now split by interfaces XXXX
	if(ifaces.size() > 1){
		for(si=0;si<ifaces.size();++si){
			sgah_qpn *subq_node = new sgah_qpn();

//			Name the subquery
			string new_name =  "_"+node_name+"_"+ifaces[si].first+"_"+ifaces[si].second;
			untaboo(new_name);
			subq_node->set_node_name( new_name) ;
			sel_names.push_back(subq_node->get_node_name());

//			Assign the table
			subq_node->table_name = fta_node->table_name->duplicate();
			subq_node->table_name->set_machine(ifaces[si].first);
			subq_node->table_name->set_interface(ifaces[si].second);
			subq_node->table_name->set_ifq(false);

//			the GB vars.
			for(g=0;g<fta_node->gb_tbl.size();g++){
//			Insert the gbvar into the lfta.
				scalarexp_t *gbvar_def = dup_se(fta_node->gb_tbl.get_def(g), NULL);
				subq_node->gb_tbl.add_gb_var(
					fta_node->gb_tbl.get_name(g), fta_node->gb_tbl.get_tblvar_ref(g), gbvar_def, fta_node->gb_tbl.get_reftype(g)
				);
			}

//			Insert the aggregates
			for(a=0;a<fta_node->aggr_tbl.size();++a){
				subq_node->aggr_tbl.add_aggr(fta_node->aggr_tbl.duplicate(a));
			}

			for(s=0;s<fta_node->select_list.size();s++){
				subq_node->select_list.push_back( dup_select(fta_node->select_list[s], NULL) );
			}
			for(p=0;p<fta_node->where.size();p++){
				predicate_t *new_pr = dup_pr(fta_node->where[p]->pr, NULL);
				cnf_elem *new_cnf = new cnf_elem(new_pr);
				analyze_cnf(new_cnf);

				subq_node->where.push_back(new_cnf);
			}
			for(p=0;p<fta_node->having.size();p++){
				predicate_t *new_pr = dup_pr(fta_node->having[p]->pr, NULL);
				cnf_elem *new_cnf = new cnf_elem(new_pr);
				analyze_cnf(new_cnf);

				subq_node->having.push_back(new_cnf);
			}
//			Xfer all of the parameters.
//			Use existing handle annotations.
			vector<string> param_names = param_tbl->get_param_names();
			int pi;
			for(pi=0;pi<param_names.size();pi++){
				data_type *dt = param_tbl->get_data_type(param_names[pi]);
				subq_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
			}
			subq_node->definitions = definitions; subq_node->definitions.erase("_referenced_ifaces");
			if(subq_node->resolve_if_params(ifdb, this->err_str)){
				this->error_code = 3;
				return ret_vec;
			}

//			THe disorder
			subq_node->lfta_disorder = fta_node->lfta_disorder;

			ret_vec.push_back(subq_node);
		}

		mrg_qpn *mrg_node = new mrg_qpn((sgah_qpn *)(ret_vec[0]),
			 fta_node->node_name, sel_names, ifaces, ifdb);
		mrg_node->set_disorder(fta_node->lfta_disorder);

		/*
		Do not split sources until we are done with optimizations
		vector<mrg_qpn *> split_merge = mrg_node->split_sources();
		for(i=0;i<split_merge.size();++i){
			ret_vec.push_back(split_merge[i]);
		}
		*/
		ret_vec.push_back(mrg_node);
		ret_vec.push_back(stream_node);
		hfta_returned = 1/*split_merge.size()*/+1;

	}else{
		fta_node->table_name->set_machine(ifaces[0].first);
		fta_node->table_name->set_interface(ifaces[0].second);
		fta_node->table_name->set_ifq(false);
		if(fta_node->resolve_if_params(ifdb, this->err_str)){
			this->error_code = 3;
			return ret_vec;
		}
		ret_vec.push_back(fta_node);
		ret_vec.push_back(stream_node);
		hfta_returned = 1;
	}


//	ret_vec.push_back(fta_node);
//	ret_vec.push_back(stream_node);


	return(ret_vec);

	}

/////////////////////////////////////////////////////////////////////
///		Split into selection LFTA, aggregation HFTA.

	spx_qpn *fta_node = new spx_qpn();
		fta_node->table_name = table_name;
		fta_node->set_node_name( "_fta_"+node_name );
		fta_node->table_name->set_range_var(table_name->get_var_name());


	sgah_qpn *stream_node = new sgah_qpn();
		stream_node->table_name = new tablevar_t( ("_fta_"+node_name).c_str());
		stream_node->set_node_name( node_name );
		stream_node->table_name->set_range_var(table_name->get_var_name());


	vector< vector<select_element *> *> select_vec;
	select_vec.push_back(&(fta_node->select_list)); // only one child

//			Process the gbvars.  Split their defining SEs.
	for(g=0;g<gb_tbl.size();g++){
		bool fta_forbidden = false;
		int se_src = SPLIT_FTAVEC_NOTBLVAR;

		scalarexp_t *gbvar_se = split_ftavec_se( gb_tbl.get_def(g),
					fta_forbidden, se_src, select_vec, Ext_fcns
		);
//		if(fta_forbidden) (
		if(fta_forbidden || se_src == SPLIT_FTAVEC_NOTBLVAR){
			stream_node->gb_tbl.add_gb_var(
			  gb_tbl.get_name(g),gb_tbl.get_tblvar_ref(g),gbvar_se,gb_tbl.get_reftype(g)
			);
		}else{
			scalarexp_t *new_se=make_fta_se_ref(fta_node->select_list,gbvar_se,0);
			stream_node->gb_tbl.add_gb_var(
			  gb_tbl.get_name(g),gb_tbl.get_tblvar_ref(g),new_se,gb_tbl.get_reftype(g)
			);
		}
	}
	stream_node->gb_tbl.set_pattern_info( &gb_tbl);

//		Process the aggregate table.
//		Copy to stream, split the SEs.
	map<int, scalarexp_t *> hfta_aggr_se;	// for rehome
	for(a=0;a<aggr_tbl.size();++a){
		scalarexp_t *hse;
		if(aggr_tbl.is_builtin(a)){
			if(aggr_tbl.is_star_aggr(a)){
				stream_node->aggr_tbl.add_aggr(aggr_tbl.get_op(a),NULL, false);
				hse=scalarexp_t::make_star_aggr(aggr_tbl.get_op(a).c_str());
			}else{
				bool fta_forbidden = false;
				int se_src = SPLIT_FTAVEC_NOTBLVAR;

				scalarexp_t *agg_se = split_ftavec_se( aggr_tbl.get_aggr_se(a),
					fta_forbidden, se_src, select_vec, Ext_fcns
				);
//				if(fta_forbidden) (
				if(fta_forbidden || se_src == SPLIT_FTAVEC_NOTBLVAR){
					stream_node->aggr_tbl.add_aggr(aggr_tbl.get_op(a), agg_se,false);
					hse=scalarexp_t::make_se_aggr(aggr_tbl.get_op(a).c_str(),agg_se);
				}else{
			  		scalarexp_t *new_se=make_fta_se_ref(fta_node->select_list,agg_se,0);
					stream_node->aggr_tbl.add_aggr(aggr_tbl.get_op(a), new_se,false);
					hse=scalarexp_t::make_se_aggr(aggr_tbl.get_op(a).c_str(),new_se);
				}
			}
			hse->set_data_type(aggr_tbl.get_data_type(a));
			hse->set_aggr_id(a);
			hfta_aggr_se[a]=hse;
		}else{
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(a);
			vector<scalarexp_t *> new_opl;
			for(o=0;o<opl.size();++o){
				bool fta_forbidden = false;
				int se_src = SPLIT_FTAVEC_NOTBLVAR;
				scalarexp_t *agg_se = split_ftavec_se( opl[o],
					fta_forbidden, se_src, select_vec, Ext_fcns
				);
//				scalarexp_t *agg_se = split_ftavec_se( aggr_tbl.get_aggr_se(a),
//					fta_forbidden, se_src, select_vec, Ext_fcns
//				);
//				if(fta_forbidden) (
				if(fta_forbidden || se_src == SPLIT_FTAVEC_NOTBLVAR){
					new_opl.push_back(agg_se);
				}else{
			  		scalarexp_t *new_se=make_fta_se_ref(fta_node->select_list,agg_se,0);
					new_opl.push_back(new_se);
				}
			}
			stream_node->aggr_tbl.add_aggr(aggr_tbl.get_op(a), aggr_tbl.get_fcn_id(a), new_opl, aggr_tbl.get_storage_type(a),false, false,aggr_tbl.has_bailout(a));
			hse = new scalarexp_t(aggr_tbl.get_op(a).c_str(),new_opl);
			hse->set_data_type(Ext_fcns->get_fcn_dt(aggr_tbl.get_fcn_id(a)));
			hse->set_fcn_id(aggr_tbl.get_fcn_id(a));
			hse->set_aggr_id(a);
			hfta_aggr_se[a]=hse;
		}
	}


//		Process the WHERE clause.
//		If it is fta-safe AND it refs only fta-safe gbvars,
//		then expand the gbvars and put it into the lfta.
//		Else, split it into an hfta predicate ref'ing
//		se's computed partially in the lfta.

	predicate_t *pr_root;
	bool fta_forbidden;
	for(p=0;p<where.size();p++){
		if(! check_fta_forbidden_pr(where[p]->pr, NULL, Ext_fcns) || contains_gb_pr(where[p]->pr, unsafe_gbvars) ){
			pr_root = split_ftavec_pr(where[p]->pr,select_vec,Ext_fcns);
			fta_forbidden = true;
		}else{
			pr_root = dup_pr(where[p]->pr, NULL);
			expand_gbvars_pr(pr_root, gb_tbl);
			fta_forbidden = false;
		}
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);

		if(fta_forbidden){
			stream_node->where.push_back(cnf_root);
		}else{
			fta_node->where.push_back(cnf_root);
		}
	}


//		Process the Select clause, rehome it on the
//		new defs.
	for(s=0;s<select_list.size();s++){
		bool fta_forbidden = false;
		scalarexp_t *root_se = rehome_fta_se(select_list[s]->se, &hfta_aggr_se);
		stream_node->select_list.push_back(
			new select_element(root_se, select_list[s]->name));
	}


// Process the Having clause

//			All of the predicates in the having clause must
//			execute in the stream node.

	for(p=0;p<having.size();p++){
		predicate_t *pr_root = rehome_fta_pr( having[p]->pr,  &hfta_aggr_se);
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);

		stream_node->having.push_back(cnf_root);
	}

//		Handle parameters and a few last details.
	vector<string> param_names = param_tbl->get_param_names();
	int pi;
	for(pi=0;pi<param_names.size();pi++){
		data_type *dt = param_tbl->get_data_type(param_names[pi]);
		fta_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
		stream_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
	}

	fta_node->definitions = definitions; fta_node->definitions.erase("_referenced_ifaces");
	stream_node->definitions = definitions;

//		Now split by interfaces YYYY
	if(ifaces.size() > 1){
		for(si=0;si<ifaces.size();++si){
			spx_qpn *subq_node = new spx_qpn();

//			Name the subquery
			string new_name =  "_"+node_name+"_"+ifaces[si].first+"_"+ifaces[si].second;
			untaboo(new_name);
			subq_node->set_node_name( new_name) ;
			sel_names.push_back(subq_node->get_node_name());

//			Assign the table
			subq_node->table_name = fta_node->table_name->duplicate();
			subq_node->table_name->set_machine(ifaces[si].first);
			subq_node->table_name->set_interface(ifaces[si].second);
			subq_node->table_name->set_ifq(false);

			for(s=0;s<fta_node->select_list.size();s++){
				subq_node->select_list.push_back( dup_select(fta_node->select_list[s], NULL) );
			}
			for(p=0;p<fta_node->where.size();p++){
				predicate_t *new_pr = dup_pr(fta_node->where[p]->pr, NULL);
				cnf_elem *new_cnf = new cnf_elem(new_pr);
				analyze_cnf(new_cnf);

				subq_node->where.push_back(new_cnf);
			}
//			Xfer all of the parameters.
//			Use existing handle annotations.
			vector<string> param_names = param_tbl->get_param_names();
			int pi;
			for(pi=0;pi<param_names.size();pi++){
				data_type *dt = param_tbl->get_data_type(param_names[pi]);
				subq_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
			}
			subq_node->definitions = definitions; subq_node->definitions.erase("_referenced_ifaces");
			if(subq_node->resolve_if_params(ifdb, this->err_str)){
				this->error_code = 3;
				return ret_vec;
			}

			ret_vec.push_back(subq_node);
		}

		mrg_qpn *mrg_node = new mrg_qpn((spx_qpn *)(ret_vec[0]),
			 fta_node->node_name, sel_names, ifaces, ifdb);
		/*
		Do not split sources until we are done with optimizations
		vector<mrg_qpn *> split_merge = mrg_node->split_sources();
		for(i=0;i<split_merge.size();++i){
			ret_vec.push_back(split_merge[i]);
		}
		*/
		ret_vec.push_back(mrg_node);
		ret_vec.push_back(stream_node);
		hfta_returned = 1/*split_merge.size()*/+1;

	}else{
		fta_node->table_name->set_machine(ifaces[0].first);
		fta_node->table_name->set_interface(ifaces[0].second);
		fta_node->table_name->set_ifq(false);
		if(fta_node->resolve_if_params(ifdb, this->err_str)){
			this->error_code = 3;
			return ret_vec;
		}
		ret_vec.push_back(fta_node);
		ret_vec.push_back(stream_node);
		hfta_returned = 1;
	}


//	ret_vec.push_back(fta_node);
//	ret_vec.push_back(stream_node);


	return(ret_vec);

}


/*
	SPLITTING A EQ-TEMPORAL, HASH JOIN OPERATOR

	An JOIN_EQ_HASH_QPN node may reference:
		literals, parameters, colrefs, functions, operators
	An JOIN_EQ_HASH_QPN node may not reference:
		group-by variables, aggregates

	An JOIN_EQ_HASH_QPN node contains
		selection list of SEs
		where list of CNF predicates, broken into:
			prefilter[2]
			temporal_eq
			hash_eq
			postfilter

	Algorithm:
		For each tablevar whose source is a PROTOCOL
			Create a LFTA for that tablevar
			Push as many prefilter[..] predicates to that tablevar as is
				possible.
			Split the SEs in the select list, and the predicates not
				pushed to the LFTA.

*/

vector<qp_node *> join_eq_hash_qpn::split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx){

	vector<qp_node *> ret_vec;
	int f,p,s;

//			If the node reads from streams only, don't split.
	bool stream_only = true;
	for(f=0;f<from.size();++f){
//		int t = Schema->get_table_ref(from[f]->get_schema_name());
		int t = from[f]->get_schema_ref();
		if(Schema->get_schema_type(t) == PROTOCOL_SCHEMA) stream_only = false;
	}
	if(stream_only){
		hfta_returned = 1;
		ret_vec.push_back(this);
		return(ret_vec);
	}


//			The HFTA node, it is always returned.

	join_eq_hash_qpn *stream_node = new join_eq_hash_qpn();
	for(f=0;f<from.size();++f){
//		tablevar_t *tmp_tblvar = new tablevar_t( from[f]->get_interface().c_str(), from[f]->get_schema_name().c_str());
		tablevar_t *tmp_tblvar =  from[f]->duplicate();
//		tmp_tblvar->set_range_var(from[f]->get_var_name());

		stream_node->from.push_back(tmp_tblvar);
	}
	stream_node->set_node_name(node_name);

//			Create spx (selection) children for each PROTOCOL source.
	vector<spx_qpn *> child_vec;
	vector< vector<select_element *> *> select_vec;
	for(f=0;f<from.size();++f){
//		int t = Schema->get_table_ref(from[f]->get_schema_name());
		int t = from[f]->get_schema_ref();
		if(Schema->get_schema_type(t) == PROTOCOL_SCHEMA){
			spx_qpn *child_qpn = new spx_qpn();
			sprintf(tmpstr,"_fta_%d_%s",f,node_name.c_str());
			child_qpn->set_node_name(string(tmpstr));
			child_qpn->table_name = new tablevar_t(
			   from[f]->get_interface().c_str(), from[f]->get_schema_name().c_str(), from[f]->get_ifq());
			child_qpn->table_name->set_range_var(from[f]->get_var_name());

			child_vec.push_back(child_qpn);
			select_vec.push_back(&(child_qpn->select_list));

//			Update the stream's FROM clause to read from this child
			stream_node->from[f]->set_interface("");
			stream_node->from[f]->set_schema(tmpstr);
		}else{
			child_vec.push_back(NULL);
			select_vec.push_back(NULL);
		}
	}

//		Push lfta-safe prefilter to the lfta
//		TODO: I'm not copying the preds, I dont *think* it will be a problem.
	predicate_t *pr_root;

	for(f=0;f<from.size();++f){
	  vector<cnf_elem *> pred_vec = prefilter[f];
	  if(child_vec[f] != NULL){
		for(p=0;p<pred_vec.size();++p){
			if(check_fta_forbidden_pr(pred_vec[p]->pr,NULL, Ext_fcns)){
				child_vec[f]->where.push_back(pred_vec[p]);
			}else{
				pr_root = split_ftavec_pr(pred_vec[p]->pr,select_vec,Ext_fcns);
				cnf_elem *cnf_root = new cnf_elem(pr_root);
				analyze_cnf(cnf_root);
				stream_node->prefilter[f].push_back(cnf_root);
			}
		}
	  }else{
		for(p=0;p<pred_vec.size();++p){
			stream_node->prefilter[f].push_back(pred_vec[p]);
		}
	  }

	}

//		Process the other predicates
	for(p=0;p<temporal_eq.size();++p){
		pr_root = split_ftavec_pr(temporal_eq[p]->pr,select_vec,Ext_fcns);
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);
		stream_node->temporal_eq.push_back(cnf_root);
	}
	for(p=0;p<hash_eq.size();++p){
		pr_root = split_ftavec_pr(hash_eq[p]->pr,select_vec,Ext_fcns);
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);
		stream_node->hash_eq.push_back(cnf_root);
	}
	for(p=0;p<postfilter.size();++p){
		pr_root = split_ftavec_pr(postfilter[p]->pr,select_vec,Ext_fcns);
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);
		stream_node->postfilter.push_back(cnf_root);
	}

//		Process the SEs
	for(s=0;s<select_list.size();s++){
		bool fta_forbidden = false;
		int se_src = SPLIT_FTAVEC_NOTBLVAR;
		scalarexp_t *root_se = split_ftavec_se( select_list[s]->se,
					fta_forbidden, se_src, select_vec, Ext_fcns
		);
		if(fta_forbidden || !is_PROTOCOL_source(se_src, select_vec)){
			stream_node->select_list.push_back(
				new select_element(root_se, select_list[s]->name) );
		}else{
			scalarexp_t *new_se=make_fta_se_ref(select_vec,root_se,se_src);
			stream_node->select_list.push_back(
				new select_element(new_se, select_list[s]->name)
			);
		}
	}


//		I need to "rehome" the colrefs -- make the annotations in the colrefs
//		agree with their tablevars.
	for(f=0;f<child_vec.size();++f){
	  if(child_vec[f]!=NULL){
		vector<tablevar_t *> fm; fm.push_back(child_vec[f]->table_name);

		for(s=0;s<child_vec[f]->select_list.size();++s)
			bind_colref_se(child_vec[f]->select_list[s]->se, fm,0,0);
		for(p=0;p<child_vec[f]->where.size();++p)
//			bind_colref_pr(child_vec[f]->where[p]->pr, fm,f,0);
			bind_colref_pr(child_vec[f]->where[p]->pr, fm,0,0);
	  }
	}

//		rehome the colrefs in the hfta node.
	for(f=0;f<stream_node->from.size();++f){
	  stream_node->where.clear();
	  for(s=0;s<stream_node->from.size();++s){
		for(p=0;p<stream_node->prefilter[s].size();++p){
		  bind_colref_pr((stream_node->prefilter[s])[p]->pr,stream_node->from,f,f);
		}
	  }
	  for(p=0;p<stream_node->temporal_eq.size();++p){
		bind_colref_pr(stream_node->temporal_eq[p]->pr,stream_node->from,f,f);
	  }
	  for(p=0;p<stream_node->hash_eq.size();++p){
		bind_colref_pr(stream_node->hash_eq[p]->pr,stream_node->from,f,f);
	  }
	  for(p=0;p<stream_node->postfilter.size();++p){
		bind_colref_pr(stream_node->postfilter[p]->pr,stream_node->from,f,f);
	  }
	  for(s=0;s<stream_node->select_list.size();++s){
		bind_colref_se(stream_node->select_list[s]->se,stream_node->from,f,f);
	  }
	}

//			Rebuild the WHERE clause
	stream_node->where.clear();
	for(s=0;s<stream_node->from.size();++s){
		for(p=0;p<stream_node->prefilter[s].size();++p){
		  stream_node->where.push_back((stream_node->prefilter[s])[p]);
		}
	}
	for(p=0;p<stream_node->temporal_eq.size();++p){
		stream_node->where.push_back(stream_node->temporal_eq[p]);
	}
	for(p=0;p<stream_node->hash_eq.size();++p){
		stream_node->where.push_back(stream_node->hash_eq[p]);
	}
	for(p=0;p<stream_node->postfilter.size();++p){
		stream_node->where.push_back(stream_node->postfilter[p]);
	}


//		Build the return list
	vector<qp_node *> hfta_nodes;
	hfta_returned = 1;
	for(f=0;f<from.size();++f){
		if(child_vec[f] != NULL){
			spx_qpn *c_node = child_vec[f];
			vector<pair<string, string> > ifaces = get_ifaces(c_node->table_name, ifdb, n_virtual_ifaces, hfta_parallelism, hfta_idx);
			if (ifaces.empty()) {
				fprintf(stderr,"INTERNAL ERROR in join_eq_hash_qpn::split_node_for_fta - empty interface set\n");
				exit(1);
			}

			if(ifaces.size() == 1){
				c_node->table_name->set_machine(ifaces[0].first);
				c_node->table_name->set_interface(ifaces[0].second);
				c_node->table_name->set_ifq(false);
				if(c_node->resolve_if_params(ifdb, this->err_str)){
					this->error_code = 3;
					return ret_vec;
				}
				ret_vec.push_back(c_node);
			}else{
				vector<string> sel_names;
				int si;
				for(si=0;si<ifaces.size();++si){
					spx_qpn *subq_node = new spx_qpn();

//			Name the subquery
					string new_name =  "_"+c_node->node_name+"_"+ifaces[si].first+"_"+ifaces[si].second;
					untaboo(new_name);
					subq_node->set_node_name( new_name) ;
					sel_names.push_back(subq_node->get_node_name());

//			Assign the table
					subq_node->table_name = c_node->table_name->duplicate();
					subq_node->table_name->set_machine(ifaces[si].first);
					subq_node->table_name->set_interface(ifaces[si].second);
					subq_node->table_name->set_ifq(false);

					for(s=0;s<c_node->select_list.size();s++){
					  subq_node->select_list.push_back(dup_select(c_node->select_list[s], NULL));
					}
					for(p=0;p<c_node->where.size();p++){
					  predicate_t *new_pr = dup_pr(c_node->where[p]->pr, NULL);
					  cnf_elem *new_cnf = new cnf_elem(new_pr);
					  analyze_cnf(new_cnf);

printf("table name is %s\n",subq_node->table_name->to_string().c_str());
					  subq_node->where.push_back(new_cnf);
					}
//			Xfer all of the parameters.
//			Use existing handle annotations.
//					vector<string> param_names = param_tbl->get_param_names();
//					int pi;
//					for(pi=0;pi<param_names.size();pi++){
//						data_type *dt = param_tbl->get_data_type(param_names[pi]);
//						subq_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
//									param_tbl->handle_access(param_names[pi]));
//					}
//					subq_node->definitions = definitions;

				if(subq_node->resolve_if_params(ifdb, this->err_str)){
					this->error_code = 3;
					return ret_vec;
				}

					ret_vec.push_back(subq_node);
				}
				int lpos = ret_vec.size()-1	;
				mrg_qpn *mrg_node = new mrg_qpn((spx_qpn *)(ret_vec[lpos]),c_node->node_name,sel_names, ifaces, ifdb);
				/*
				Do not split sources until we are done with optimizations
				vector<mrg_qpn *> split_merge = mrg_node->split_sources();
				int i;
				for(i=0;i<split_merge.size();++i){
					hfta_nodes.push_back(split_merge[i]);
				}
				*/
				hfta_nodes.push_back(mrg_node);
			}
		}
	}
	int i;
	for(i=0;i<hfta_nodes.size();++i) ret_vec.push_back(hfta_nodes[i]);
	ret_vec.push_back(stream_node);
	hfta_returned = hfta_nodes.size()+1;

//			Currently : assume that the stream receives all parameters
//			and parameter updates, incorporates them, then passes
//			all of the parameters to the FTA.
//			This will need to change (tables, fta-unsafe types. etc.)

//			I will pass on the use_handle_access marking, even
//			though the fcn call that requires handle access might
//			exist in only one of the parts of the query.
//			Parameter manipulation and handle access determination will
//			need to be revisited anyway.
	vector<string> param_names = param_tbl->get_param_names();
	int pi;
	for(pi=0;pi<param_names.size();pi++){
		int ri;
		data_type *dt = param_tbl->get_data_type(param_names[pi]);
		for(ri=0;ri<ret_vec.size();++ri){
			ret_vec[ri]->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
			ret_vec[ri]->definitions = definitions; ret_vec[ri]->definitions.erase("_referenced_ifaces");
		}
	}



	return(ret_vec);

}


/////////////////////////////////////////////////////////////
////			extract_opview

//		Common processing
int process_opview(tablevar_t *fmtbl, int pos, string node_name,
				 table_list *Schema,
				vector<query_node *> &qnodes,
				opview_set &opviews,
				vector<table_exp_t *> &ret, string rootnm, string silo_nm){

	int s,f,q,m;

	int schref = fmtbl->get_schema_ref();
	if(schref <= 0)
		return 0;

	if(Schema->get_schema_type(schref) == OPERATOR_VIEW_SCHEMA){
		opview_entry *opv = new opview_entry();
		opv->parent_qname = node_name;
		opv->root_name = rootnm;
		opv->view_name = fmtbl->get_schema_name();
		opv->pos = pos;
		sprintf(tmpstr,"%s_UDOP%d_%s",node_name.c_str(),pos,opv->view_name.c_str());
		opv->udop_alias = tmpstr;
		fmtbl->set_udop_alias(opv->udop_alias);

		opv->exec_fl = Schema->get_op_prop(schref, string("file"));
		opv->liveness_timeout = atoi(Schema->get_op_prop(schref, string("liveness_timeout")).c_str());

		vector<subquery_spec *> subq = Schema->get_subqueryspecs(schref);
		for(s=0;s<subq.size();++s){
//				Validate that the fields match.
			subquery_spec *sqs = subq[s];
			vector<field_entry *> flds = Schema->get_fields(sqs->name+silo_nm);
			if(flds.size() == 0){
				fprintf(stderr,"INTERNAL ERROR: subquery %s of view %s not found in Schema.\n",sqs->name.c_str(), opv->view_name.c_str());
				return(1);
			}
			if(flds.size() < sqs->types.size()){
				fprintf(stderr,"ERROR: subquery %s of view %s does not have enough fields (%lu found, %lu expected).\n",sqs->name.c_str(), opv->view_name.c_str(),flds.size(), sqs->types.size());
				return(1);
			}
			bool failed = false;
			for(f=0;f<sqs->types.size();++f){
				data_type dte(sqs->types[f],sqs->modifiers[f]);
				data_type dtf(flds[f]->get_type(),flds[f]->get_modifier_list());
				if(! dte.subsumes_type(&dtf) ){
					fprintf(stderr,"ERROR: subquery %s of view %s does not have the correct type for field %d (%s found, %s expected).\n",sqs->name.c_str(), opv->view_name.c_str(),f,dtf.to_string().c_str(), dte.to_string().c_str());
					failed = true;
				}
/*
				if(dte.is_temporal() && (dte.get_temporal() != dtf.get_temporal()) ){
					string pstr = dte.get_temporal_string();
					fprintf(stderr,"ERROR: subquery %s of view %s does not have the expected temporal value %s of field %d.\n",sqs->name.c_str(), opv->view_name.c_str(),pstr.c_str(),f);
					failed = true;
				}
*/
			}
			if(failed)
				return(1);
///				Validation done, find the subquery, make a copy of the
///				parse tree, and add it to the return list.
			for(q=0;q<qnodes.size();++q)
				if(qnodes[q]->name == sqs->name)
					break;
			if(q==qnodes.size()){
				fprintf(stderr,"INTERNAL ERROR: subquery %s of view %s not found in list of query names.\n",sqs->name.c_str(), opv->view_name.c_str());
				return(1);
			}

			table_exp_t *newq = dup_table_exp(qnodes[q]->parse_tree);
			sprintf(tmpstr,"%s_OP%d_%s_SUBQ%d",node_name.c_str(),pos,opv->view_name.c_str(),s);
			string newq_name = tmpstr;
			newq->nmap["query_name"] = newq_name;
			ret.push_back(newq);
			opv->subq_names.push_back(newq_name);
		}
		fmtbl->set_opview_idx(opviews.append(opv));
	}

	return 0;
}

vector<table_exp_t *> spx_qpn::extract_opview(table_list *Schema, vector<query_node *> &qnodes, opview_set &opviews, string rootnm, string silo_name){
	vector<table_exp_t *> ret;

	int retval = process_opview(table_name,0,node_name,
								Schema,qnodes,opviews,ret, rootnm, silo_name);
	if(retval) exit(1);
    return(ret);
}


vector<table_exp_t *> sgah_qpn::extract_opview(table_list *Schema,  vector<query_node *> &qnodes, opview_set &opviews, string rootnm, string silo_name){
	vector<table_exp_t *> ret;

	int retval = process_opview(table_name,0,node_name,
								Schema,qnodes,opviews,ret, rootnm, silo_name);
	if(retval) exit(1);
    return(ret);
}

vector<table_exp_t *> rsgah_qpn::extract_opview(table_list *Schema,  vector<query_node *> &qnodes, opview_set &opviews, string rootnm, string silo_name){
	vector<table_exp_t *> ret;

	int retval = process_opview(table_name,0,node_name,
								Schema,qnodes,opviews,ret, rootnm, silo_name);
	if(retval) exit(1);
    return(ret);
}


vector<table_exp_t *> sgahcwcb_qpn::extract_opview(table_list *Schema,  vector<query_node *> &qnodes, opview_set &opviews, string rootnm, string silo_name){
	vector<table_exp_t *> ret;

	int retval = process_opview(table_name,0,node_name,
								Schema,qnodes,opviews,ret, rootnm, silo_name);
	if(retval) exit(1);
    return(ret);
}



vector<table_exp_t *> mrg_qpn::extract_opview(table_list *Schema,  vector<query_node *> &qnodes, opview_set &opviews, string rootnm, string silo_name){
	vector<table_exp_t *> ret;
	int f;
	for(f=0;f<fm.size();++f){
		int retval = process_opview(fm[f],f,node_name,
								Schema,qnodes,opviews,ret, rootnm, silo_name);
		if(retval) exit(1);
	}
    return(ret);
}




vector<table_exp_t *> join_eq_hash_qpn::extract_opview(table_list *Schema,  vector<query_node *> &qnodes, opview_set &opviews, string rootnm, string silo_name){
	vector<table_exp_t *> ret;
	int f;
	for(f=0;f<from.size();++f){
		int retval = process_opview(from[f],f,node_name,
								Schema,qnodes,opviews,ret, rootnm, silo_name);
		if(retval) exit(1);
	}
    return(ret);
}

vector<table_exp_t *> filter_join_qpn::extract_opview(table_list *Schema,  vector<query_node *> &qnodes, opview_set &opviews, string rootnm, string silo_name){
	vector<table_exp_t *> ret;
	int f;
	for(f=0;f<from.size();++f){
		int retval = process_opview(from[f],f,node_name,
								Schema,qnodes,opviews,ret, rootnm, silo_name);
		if(retval) exit(1);
	}
    return(ret);
}



//////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////
///////			Additional methods



//////////////////////////////////////////////////////////////////
//		Get schema of operator output

table_def *mrg_qpn::get_fields(){
	return(table_layout);
}


table_def *spx_qpn::get_fields(){
	return(create_attributes(node_name, select_list));
}

table_def *sgah_qpn::get_fields(){
	return(create_attributes(node_name, select_list));
}

table_def *rsgah_qpn::get_fields(){
	return(create_attributes(node_name, select_list));
}

table_def *sgahcwcb_qpn::get_fields(){
	return(create_attributes(node_name, select_list));
}

table_def *filter_join_qpn::get_fields(){
	return(create_attributes(node_name, select_list));
}


table_def *join_eq_hash_qpn::get_fields(){
	int i, h, s, t;

//			First, gather temporal colrefs and SEs.
	map<col_id, temporal_type> temporal_cids;
	vector<scalarexp_t *> temporal_se;
	for(h=0;h<temporal_eq.size();++h){
		scalarexp_t *sel = temporal_eq[h]->pr->get_left_se();
		scalarexp_t *ser = temporal_eq[h]->pr->get_right_se();

		if(sel->get_operator_type() == SE_COLREF){
			col_id tcol(sel->get_colref());
			if(temporal_cids.count(tcol) == 0){
				temporal_cids[tcol] = sel->get_data_type()->get_temporal();
			}
		}else{
			temporal_se.push_back(sel);
		}

		if(ser->get_operator_type() == SE_COLREF){
			col_id tcol(ser->get_colref());
			if(temporal_cids.count(tcol) == 0){
				temporal_cids[tcol] = ser->get_data_type()->get_temporal();
			}
		}else{
			temporal_se.push_back(ser);
		}
	}

//		Mark select elements as nontemporal, then deduce which
//		ones are temporal.
	for(s=0;s<select_list.size();++s){
		select_list[s]->se->get_data_type()->set_temporal(
			compute_se_temporal(select_list[s]->se, temporal_cids)
		);
//				Second chance if it is an exact match to an SE.
//	for(s=0;s<select_list.size();++s){
		if(! select_list[s]->se->get_data_type()->is_temporal() ){
			for(t=0;t<temporal_se.size();++t){
				if(is_equivalent_se(temporal_se[t], select_list[s]->se)){
					select_list[s]->se->get_data_type()->set_temporal(
						temporal_se[t]->get_data_type()->get_temporal()
					);
				}
			}
		}
//	}
	}

//			If there is an outer join, verify that
//			the temporal attributes are actually temporal.
//			NOTE: this code must be synchronized with the
//			equivalence finding in join_eq_hash_qpn::generate_functor
//			(and also, the join_eq_hash_qpn constructor)
  if(from[0]->get_property() || from[1]->get_property()){
	set<string> l_equiv, r_equiv;
	for(i=0;i<temporal_eq.size();i++){
		scalarexp_t *lse = 	temporal_eq[i]->pr->get_left_se();
		scalarexp_t *rse = 	temporal_eq[i]->pr->get_right_se();
		if(lse->get_operator_type()==SE_COLREF){
			l_equiv.insert(lse->get_colref()->get_field());
		}
		if(rse->get_operator_type()==SE_COLREF){
			r_equiv.insert(rse->get_colref()->get_field());
		}
	}

	for(s=0;s<select_list.size();++s){
		if(select_list[s]->se->get_data_type()->is_temporal()){
			col_id_set cid_set;
			col_id_set::iterator ci;
			bool failed = false;
			gather_se_col_ids(select_list[s]->se,cid_set, NULL);
			for(ci=cid_set.begin();ci!=cid_set.end();++ci){
				if((*ci).tblvar_ref == 0){
					 if(from[0]->get_property()){
						if(l_equiv.count((*ci).field) == 0){
							failed = true;
						}
					}
				}else{
					 if(from[1]->get_property()){
						if(r_equiv.count((*ci).field) == 0){
							failed = true;
						}
					}
				}
			}
			if(failed){
				select_list[s]->se->get_data_type()->reset_temporal();
			}
		}
	}
  }


	return create_attributes(node_name, select_list);
}



//-----------------------------------------------------------------
//			get output tables


//			Get tablevar_t names of input and output tables

//	output_file_qpn::output_file_qpn(){source_op_name = ""; }
	vector<tablevar_t *> output_file_qpn::get_input_tbls(){
		return(fm);
	}

	vector<tablevar_t *> mrg_qpn::get_input_tbls(){
		return(fm);
	}

	vector<tablevar_t *> spx_qpn::get_input_tbls(){
		vector<tablevar_t *> retval(1,table_name);
		return(retval);
	}

	vector<tablevar_t *> sgah_qpn::get_input_tbls(){
		vector<tablevar_t *> retval(1,table_name);
		return(retval);
	}

	vector<tablevar_t *> rsgah_qpn::get_input_tbls(){
		vector<tablevar_t *> retval(1,table_name);
		return(retval);
	}

	vector<tablevar_t *> sgahcwcb_qpn::get_input_tbls(){
		vector<tablevar_t *> retval(1,table_name);
		return(retval);
	}

	vector<tablevar_t *> join_eq_hash_qpn::get_input_tbls(){
		return(from);
	}

	vector<tablevar_t *> filter_join_qpn::get_input_tbls(){
		return(from);
	}

//-----------------------------------------------------------------
//			get output tables


//		This does not make sense, this fcn returns the output table *name*,
//		not its schema, and then there is another fcn to rturn the schema.
	vector<tablevar_t *> output_file_qpn::get_output_tbls(){
		vector<tablevar_t *> retval(1,new tablevar_t(node_name.c_str()));
		return(retval);
	}

	vector<tablevar_t *> mrg_qpn::get_output_tbls(){
		vector<tablevar_t *> retval(1,new tablevar_t(node_name.c_str()));
		return(retval);
	}

	vector<tablevar_t *> spx_qpn::get_output_tbls(){
		vector<tablevar_t *> retval(1,new tablevar_t(node_name.c_str()));
		return(retval);
	}

	vector<tablevar_t *> sgah_qpn::get_output_tbls(){
		vector<tablevar_t *> retval(1,new tablevar_t(node_name.c_str()));
		return(retval);
	}

	vector<tablevar_t *> rsgah_qpn::get_output_tbls(){
		vector<tablevar_t *> retval(1,new tablevar_t(node_name.c_str()));
		return(retval);
	}

	vector<tablevar_t *> sgahcwcb_qpn::get_output_tbls(){
		vector<tablevar_t *> retval(1,new tablevar_t(node_name.c_str()));
		return(retval);
	}

	vector<tablevar_t *> join_eq_hash_qpn::get_output_tbls(){
		vector<tablevar_t *> retval(1,new tablevar_t(node_name.c_str()));
		return(retval);
	}

	vector<tablevar_t *> filter_join_qpn::get_output_tbls(){
		vector<tablevar_t *> retval(1,new tablevar_t(node_name.c_str()));
		return(retval);
	}



//-----------------------------------------------------------------
//			Bind to schema

//		Associate colrefs with this schema.
//		Also, use this opportunity to create table_layout (the output schema).
//		If the output schema is ever needed before
void mrg_qpn::bind_to_schema(table_list *Schema){
	int t;
	for(t=0;t<fm.size();++t){
		int tblref = Schema->get_table_ref(fm[t]->get_schema_name());
		if(tblref>=0)
    		fm[t]->set_schema_ref(tblref );
	}

//		Here I assume that the colrefs have been reorderd
//		during analysis so that mvars line up with fm.
	mvars[0]->set_schema_ref(fm[0]->get_schema_ref());
	mvars[1]->set_schema_ref(fm[1]->get_schema_ref());


}



//		Associate colrefs in SEs with this schema.
void spx_qpn::bind_to_schema(table_list *Schema){
//			Bind the tablevars in the From clause to the Schema
//			(it might have changed from analysis time)
	int t = Schema->get_table_ref(table_name->get_schema_name() );
	if(t>=0)
    	table_name->set_schema_ref(t );

//			Get the "from" clause
	tablevar_list_t fm(table_name);

//			Bind all SEs to this schema
	int p;
	for(p=0;p<where.size();++p){
		bind_to_schema_pr(where[p]->pr, &fm, Schema);
	}
	int s;
	for(s=0;s<select_list.size();++s){
		bind_to_schema_se(select_list[s]->se, &fm, Schema);
	}

//		Collect set of tuples referenced in this HFTA
//		input, internal, or output.

}

col_id_set spx_qpn::get_colrefs(bool ext_fcns_only,table_list *Schema){
	col_id_set retval, tmp_cset;
	int p;
	for(p=0;p<where.size();++p){
		gather_pr_col_ids(where[p]->pr, tmp_cset, NULL);
	}
	int s;
	for(s=0;s<select_list.size();++s){
		gather_se_col_ids(select_list[s]->se, tmp_cset, NULL);
	}
	col_id_set::iterator  cisi;
	if(ext_fcns_only){
		for(cisi=tmp_cset.begin();cisi!=tmp_cset.end();++cisi){
			field_entry *fe = Schema->get_field((*cisi).schema_ref, (*cisi).field);
			if(fe->get_unpack_fcns().size()>0)
				retval.insert((*cisi));
		}
		return retval;
	}

	return tmp_cset;
}

col_id_set filter_join_qpn::get_colrefs(bool ext_fcns_only,table_list *Schema){
	col_id_set retval, tmp_cset;
	int p;
	for(p=0;p<where.size();++p){
		gather_pr_col_ids(where[p]->pr, tmp_cset, NULL);
	}
	int s;
	for(s=0;s<select_list.size();++s){
		gather_se_col_ids(select_list[s]->se, tmp_cset, NULL);
	}
	col_id_set::iterator  cisi;
	if(ext_fcns_only){
		for(cisi=tmp_cset.begin();cisi!=tmp_cset.end();++cisi){
			field_entry *fe = Schema->get_field((*cisi).schema_ref, (*cisi).field);
			if(fe->get_unpack_fcns().size()>0)
				retval.insert((*cisi));
		}
		return retval;
	}

	return tmp_cset;
}



//		Associate colrefs in SEs with this schema.
void join_eq_hash_qpn::bind_to_schema(table_list *Schema){
//			Bind the tablevars in the From clause to the Schema
//			(it might have changed from analysis time)
	int f;
	for(f=0;f<from.size();++f){
		string snm = from[f]->get_schema_name();
		int tbl_ref = Schema->get_table_ref(snm);
		if(tbl_ref >= 0)
    		from[f]->set_schema_ref(tbl_ref);
	}

//			Bind all SEs to this schema
	tablevar_list_t fm(from);

	int p;
	for(p=0;p<where.size();++p){
		bind_to_schema_pr(where[p]->pr, &fm, Schema);
	}
	int s;
	for(s=0;s<select_list.size();++s){
		bind_to_schema_se(select_list[s]->se, &fm, Schema);
	}

//		Collect set of tuples referenced in this HFTA
//		input, internal, or output.

}

void filter_join_qpn::bind_to_schema(table_list *Schema){
//			Bind the tablevars in the From clause to the Schema
//			(it might have changed from analysis time)
	int f;
	for(f=0;f<from.size();++f){
		string snm = from[f]->get_schema_name();
		int tbl_ref = Schema->get_table_ref(snm);
		if(tbl_ref >= 0)
    		from[f]->set_schema_ref(tbl_ref);
	}

//			Bind all SEs to this schema
	tablevar_list_t fm(from);

	int p;
	for(p=0;p<where.size();++p){
		bind_to_schema_pr(where[p]->pr, &fm, Schema);
	}
	int s;
	for(s=0;s<select_list.size();++s){
		bind_to_schema_se(select_list[s]->se, &fm, Schema);
	}

//		Collect set of tuples referenced in this HFTA
//		input, internal, or output.

}




void sgah_qpn::bind_to_schema(table_list *Schema){
//			Bind the tablevars in the From clause to the Schema
//			(it might have changed from analysis time)


	int t = Schema->get_table_ref(table_name->get_schema_name() );
	if(t>=0)
    	table_name->set_schema_ref(t );

//			Get the "from" clause
	tablevar_list_t fm(table_name);



//			Bind all SEs to this schema
	int p;
	for(p=0;p<where.size();++p){
		bind_to_schema_pr(where[p]->pr, &fm, Schema);
	}
	for(p=0;p<having.size();++p){
		bind_to_schema_pr(having[p]->pr, &fm, Schema);
	}
	int s;
	for(s=0;s<select_list.size();++s){
		bind_to_schema_se(select_list[s]->se, &fm, Schema);
	}
	int g;
	for(g=0;g<gb_tbl.size();++g){
		bind_to_schema_se(gb_tbl.get_def(g), &fm, Schema);
	}
	int a;
	for(a=0;a<aggr_tbl.size();++a){
		if(aggr_tbl.is_builtin(a)){
			bind_to_schema_se(aggr_tbl.get_aggr_se(a), &fm, Schema);
		}else{
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(a);
			int o;
			for(o=0;o<opl.size();++o){
				bind_to_schema_se(opl[o],&fm,Schema);
			}
		}
	}
}

col_id_set sgah_qpn::get_colrefs(bool ext_fcns_only,table_list *Schema){
	col_id_set retval, tmp_cset;
	int p;
	for(p=0;p<where.size();++p){
		gather_pr_col_ids(where[p]->pr, tmp_cset, &gb_tbl);
	}
	int g;
	for(g=0;g<gb_tbl.size();++g){
		gather_se_col_ids(gb_tbl.get_def(g), tmp_cset, &gb_tbl);
	}
	int a;
	for(a=0;a<aggr_tbl.size();++a){
		if(aggr_tbl.is_builtin(a)){
			gather_se_col_ids(aggr_tbl.get_aggr_se(a), tmp_cset, &gb_tbl);
		}else{
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(a);
			int o;
			for(o=0;o<opl.size();++o){
				gather_se_col_ids(opl[o], tmp_cset, &gb_tbl);
			}
		}
	}

	col_id_set::iterator  cisi;
	if(ext_fcns_only){
		for(cisi=tmp_cset.begin();cisi!=tmp_cset.end();++cisi){
			field_entry *fe = Schema->get_field((*cisi).schema_ref, (*cisi).field);
			if(fe->get_unpack_fcns().size()>0)
				retval.insert((*cisi));
		}
		return retval;
	}

	return tmp_cset;
}


void rsgah_qpn::bind_to_schema(table_list *Schema){
//			Bind the tablevars in the From clause to the Schema
//			(it might have changed from analysis time)
	int t = Schema->get_table_ref(table_name->get_schema_name() );
	if(t>=0)
    	table_name->set_schema_ref(t );

//			Get the "from" clause
	tablevar_list_t fm(table_name);

//			Bind all SEs to this schema
	int p;
	for(p=0;p<where.size();++p){
		bind_to_schema_pr(where[p]->pr, &fm, Schema);
	}
	for(p=0;p<having.size();++p){
		bind_to_schema_pr(having[p]->pr, &fm, Schema);
	}
	for(p=0;p<closing_when.size();++p){
		bind_to_schema_pr(closing_when[p]->pr, &fm, Schema);
	}
	int s;
	for(s=0;s<select_list.size();++s){
		bind_to_schema_se(select_list[s]->se, &fm, Schema);
	}
	int g;
	for(g=0;g<gb_tbl.size();++g){
		bind_to_schema_se(gb_tbl.get_def(g), &fm, Schema);
	}
	int a;
	for(a=0;a<aggr_tbl.size();++a){
		if(aggr_tbl.is_builtin(a)){
			bind_to_schema_se(aggr_tbl.get_aggr_se(a), &fm, Schema);
		}else{
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(a);
			int o;
			for(o=0;o<opl.size();++o){
				bind_to_schema_se(opl[o],&fm,Schema);
			}
		}
	}
}


void sgahcwcb_qpn::bind_to_schema(table_list *Schema){
//			Bind the tablevars in the From clause to the Schema
//			(it might have changed from analysis time)
	int t = Schema->get_table_ref(table_name->get_schema_name() );
	if(t>=0)
    	table_name->set_schema_ref(t );

//			Get the "from" clause
	tablevar_list_t fm(table_name);

//			Bind all SEs to this schema
	int p;
	for(p=0;p<where.size();++p){
		bind_to_schema_pr(where[p]->pr, &fm, Schema);
	}
	for(p=0;p<having.size();++p){
		bind_to_schema_pr(having[p]->pr, &fm, Schema);
	}
	for(p=0;p<having.size();++p){
		bind_to_schema_pr(cleanby[p]->pr, &fm, Schema);
	}
	for(p=0;p<having.size();++p){
		bind_to_schema_pr(cleanwhen[p]->pr, &fm, Schema);
	}
	int s;
	for(s=0;s<select_list.size();++s){
		bind_to_schema_se(select_list[s]->se, &fm, Schema);
	}
	int g;
	for(g=0;g<gb_tbl.size();++g){
		bind_to_schema_se(gb_tbl.get_def(g), &fm, Schema);
	}
	int a;
	for(a=0;a<aggr_tbl.size();++a){
		if(aggr_tbl.is_builtin(a)){
			bind_to_schema_se(aggr_tbl.get_aggr_se(a), &fm, Schema);
		}else{
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(a);
			int o;
			for(o=0;o<opl.size();++o){
				bind_to_schema_se(opl[o],&fm,Schema);
			}
		}
	}
}






///////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////
///		Functions for code generation.


//-----------------------------------------------------------------
//		get_cplx_lit_tbl

cplx_lit_table *mrg_qpn::get_cplx_lit_tbl(ext_fcn_list *Ext_fcns){
	return(new cplx_lit_table());
}

cplx_lit_table *spx_qpn::get_cplx_lit_tbl(ext_fcn_list *Ext_fcns){
	int i;
	cplx_lit_table *complex_literals = new cplx_lit_table();

	for(i=0;i<select_list.size();i++){
		find_complex_literal_se(select_list[i]->se, Ext_fcns, complex_literals);
	}
	for(i=0;i<where.size();++i){
		find_complex_literal_pr(where[i]->pr,Ext_fcns, complex_literals);
	}

	return(complex_literals);
}

cplx_lit_table *sgah_qpn::get_cplx_lit_tbl(ext_fcn_list *Ext_fcns){
	int i,j;
	cplx_lit_table *complex_literals = new cplx_lit_table();

	for(i=0;i<aggr_tbl.size();++i){
		if(aggr_tbl.is_builtin(i) && !aggr_tbl.is_star_aggr(i)){
			find_complex_literal_se(aggr_tbl.get_aggr_se(i), Ext_fcns, complex_literals);
		}else{
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(i);
			for(j=0;j<opl.size();++j)
				find_complex_literal_se(opl[j], Ext_fcns, complex_literals);
		}
	}

	for(i=0;i<select_list.size();i++){
		find_complex_literal_se(select_list[i]->se, Ext_fcns, complex_literals);
	}
    for(i=0;i<gb_tbl.size();i++){
        find_complex_literal_se(gb_tbl.get_def(i), Ext_fcns, complex_literals);
    }
	for(i=0;i<where.size();++i){
		find_complex_literal_pr(where[i]->pr,Ext_fcns, complex_literals);
	}
	for(i=0;i<having.size();++i){
			find_complex_literal_pr(having[i]->pr,Ext_fcns, complex_literals);
	}

	return(complex_literals);
}


cplx_lit_table *rsgah_qpn::get_cplx_lit_tbl(ext_fcn_list *Ext_fcns){
	int i,j;
	cplx_lit_table *complex_literals = new cplx_lit_table();

	for(i=0;i<aggr_tbl.size();++i){
		if(aggr_tbl.is_builtin(i) && !aggr_tbl.is_star_aggr(i)){
			find_complex_literal_se(aggr_tbl.get_aggr_se(i), Ext_fcns, complex_literals);
		}else{
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(i);
			for(j=0;j<opl.size();++j)
				find_complex_literal_se(opl[j], Ext_fcns, complex_literals);
		}
	}

	for(i=0;i<select_list.size();i++){
		find_complex_literal_se(select_list[i]->se, Ext_fcns, complex_literals);
	}
    for(i=0;i<gb_tbl.size();i++){
        find_complex_literal_se(gb_tbl.get_def(i), Ext_fcns, complex_literals);
    }
	for(i=0;i<where.size();++i){
		find_complex_literal_pr(where[i]->pr,Ext_fcns, complex_literals);
	}
	for(i=0;i<having.size();++i){
			find_complex_literal_pr(having[i]->pr,Ext_fcns, complex_literals);
	}
	for(i=0;i<closing_when.size();++i){
			find_complex_literal_pr(closing_when[i]->pr,Ext_fcns, complex_literals);
	}

	return(complex_literals);
}


cplx_lit_table *sgahcwcb_qpn::get_cplx_lit_tbl(ext_fcn_list *Ext_fcns){
	int i,j;
	cplx_lit_table *complex_literals = new cplx_lit_table();

	for(i=0;i<aggr_tbl.size();++i){
		if(aggr_tbl.is_builtin(i) && !aggr_tbl.is_star_aggr(i)){
			find_complex_literal_se(aggr_tbl.get_aggr_se(i), Ext_fcns, complex_literals);
		}else{
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(i);
			for(j=0;j<opl.size();++j)
				find_complex_literal_se(opl[j], Ext_fcns, complex_literals);
		}
	}

	for(i=0;i<select_list.size();i++){
		find_complex_literal_se(select_list[i]->se, Ext_fcns, complex_literals);
	}
    for(i=0;i<gb_tbl.size();i++){
        find_complex_literal_se(gb_tbl.get_def(i), Ext_fcns, complex_literals);
    }
	for(i=0;i<where.size();++i){
		find_complex_literal_pr(where[i]->pr,Ext_fcns, complex_literals);
	}
	for(i=0;i<having.size();++i){
			find_complex_literal_pr(having[i]->pr,Ext_fcns, complex_literals);
	}
	for(i=0;i<cleanwhen.size();++i){
			find_complex_literal_pr(cleanwhen[i]->pr,Ext_fcns, complex_literals);
	}
	for(i=0;i<cleanby.size();++i){
			find_complex_literal_pr(cleanby[i]->pr,Ext_fcns, complex_literals);
	}

	return(complex_literals);
}

cplx_lit_table *join_eq_hash_qpn::get_cplx_lit_tbl(ext_fcn_list *Ext_fcns){
	int i;
	cplx_lit_table *complex_literals = new cplx_lit_table();

	for(i=0;i<select_list.size();i++){
		find_complex_literal_se(select_list[i]->se, Ext_fcns, complex_literals);
	}
	for(i=0;i<where.size();++i){
		find_complex_literal_pr(where[i]->pr,Ext_fcns, complex_literals);
	}

	return(complex_literals);
}

cplx_lit_table *filter_join_qpn::get_cplx_lit_tbl(ext_fcn_list *Ext_fcns){
	int i;
	cplx_lit_table *complex_literals = new cplx_lit_table();

	for(i=0;i<select_list.size();i++){
		find_complex_literal_se(select_list[i]->se, Ext_fcns, complex_literals);
	}
	for(i=0;i<where.size();++i){
		find_complex_literal_pr(where[i]->pr,Ext_fcns, complex_literals);
	}

	return(complex_literals);
}




//-----------------------------------------------------------------
//		get_handle_param_tbl

vector<handle_param_tbl_entry *> mrg_qpn::get_handle_param_tbl(ext_fcn_list *Ext_fcns){
    vector<handle_param_tbl_entry *> retval;
	return(retval);
}


vector<handle_param_tbl_entry *> spx_qpn::get_handle_param_tbl(ext_fcn_list *Ext_fcns){
	int i;
    vector<handle_param_tbl_entry *> retval;

	for(i=0;i<select_list.size();i++){
		find_param_handles_se(select_list[i]->se, Ext_fcns, retval);
	}
	for(i=0;i<where.size();++i){
		find_param_handles_pr(where[i]->pr,Ext_fcns, retval);
	}

	return(retval);
}


vector<handle_param_tbl_entry *> sgah_qpn::get_handle_param_tbl(ext_fcn_list *Ext_fcns){
	int i,j;
    vector<handle_param_tbl_entry *> retval;


	for(i=0;i<aggr_tbl.size();++i){
		if(aggr_tbl.is_builtin(i) && !aggr_tbl.is_star_aggr(i)){
			find_param_handles_se(aggr_tbl.get_aggr_se(i), Ext_fcns, retval);
		}else{
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(i);
			for(j=0;j<opl.size();++j)
				find_param_handles_se(opl[j], Ext_fcns, retval);
		}
	}
	for(i=0;i<select_list.size();i++){
		find_param_handles_se(select_list[i]->se, Ext_fcns, retval);
	}
    for(i=0;i<gb_tbl.size();i++){
        find_param_handles_se(gb_tbl.get_def(i), Ext_fcns, retval);
    }
	for(i=0;i<where.size();++i){
		find_param_handles_pr(where[i]->pr,Ext_fcns, retval);
	}
	for(i=0;i<having.size();++i){
			find_param_handles_pr(having[i]->pr,Ext_fcns, retval);
	}

	return(retval);
}


vector<handle_param_tbl_entry *> rsgah_qpn::get_handle_param_tbl(ext_fcn_list *Ext_fcns){
	int i,j;
    vector<handle_param_tbl_entry *> retval;


	for(i=0;i<aggr_tbl.size();++i){
		if(aggr_tbl.is_builtin(i) && !aggr_tbl.is_star_aggr(i)){
			find_param_handles_se(aggr_tbl.get_aggr_se(i), Ext_fcns, retval);
		}else{
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(i);
			for(j=0;j<opl.size();++j)
				find_param_handles_se(opl[j], Ext_fcns, retval);
		}
	}
	for(i=0;i<select_list.size();i++){
		find_param_handles_se(select_list[i]->se, Ext_fcns, retval);
	}
    for(i=0;i<gb_tbl.size();i++){
        find_param_handles_se(gb_tbl.get_def(i), Ext_fcns, retval);
    }
	for(i=0;i<where.size();++i){
		find_param_handles_pr(where[i]->pr,Ext_fcns, retval);
	}
	for(i=0;i<having.size();++i){
			find_param_handles_pr(having[i]->pr,Ext_fcns, retval);
	}
	for(i=0;i<closing_when.size();++i){
			find_param_handles_pr(closing_when[i]->pr,Ext_fcns, retval);
	}

	return(retval);
}


vector<handle_param_tbl_entry *> sgahcwcb_qpn::get_handle_param_tbl(ext_fcn_list *Ext_fcns){
	int i,j;
    vector<handle_param_tbl_entry *> retval;


	for(i=0;i<aggr_tbl.size();++i){
		if(aggr_tbl.is_builtin(i) && !aggr_tbl.is_star_aggr(i)){
			find_param_handles_se(aggr_tbl.get_aggr_se(i), Ext_fcns, retval);
		}else{
			vector<scalarexp_t *> opl = aggr_tbl.get_operand_list(i);
			for(j=0;j<opl.size();++j)
				find_param_handles_se(opl[j], Ext_fcns, retval);
		}
	}
	for(i=0;i<select_list.size();i++){
		find_param_handles_se(select_list[i]->se, Ext_fcns, retval);
	}
    for(i=0;i<gb_tbl.size();i++){
        find_param_handles_se(gb_tbl.get_def(i), Ext_fcns, retval);
    }
	for(i=0;i<where.size();++i){
		find_param_handles_pr(where[i]->pr,Ext_fcns, retval);
	}
	for(i=0;i<having.size();++i){
			find_param_handles_pr(having[i]->pr,Ext_fcns, retval);
	}
	for(i=0;i<cleanwhen.size();++i){
			find_param_handles_pr(cleanwhen[i]->pr,Ext_fcns, retval);
	}
	for(i=0;i<cleanby.size();++i){
			find_param_handles_pr(cleanby[i]->pr,Ext_fcns, retval);
	}

	return(retval);
}

vector<handle_param_tbl_entry *> join_eq_hash_qpn::get_handle_param_tbl(ext_fcn_list *Ext_fcns){
	int i;
    vector<handle_param_tbl_entry *> retval;

	for(i=0;i<select_list.size();i++){
		find_param_handles_se(select_list[i]->se, Ext_fcns, retval);
	}
	for(i=0;i<where.size();++i){
		find_param_handles_pr(where[i]->pr,Ext_fcns, retval);
	}

	return(retval);
}


vector<handle_param_tbl_entry *> filter_join_qpn::get_handle_param_tbl(ext_fcn_list *Ext_fcns){
	int i;
    vector<handle_param_tbl_entry *> retval;

	for(i=0;i<select_list.size();i++){
		find_param_handles_se(select_list[i]->se, Ext_fcns, retval);
	}
	for(i=0;i<where.size();++i){
		find_param_handles_pr(where[i]->pr,Ext_fcns, retval);
	}

	return(retval);
}

///////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////
///		Functions for operator output rates estimations


//-----------------------------------------------------------------
//		get_rate_estimate

double spx_qpn::get_rate_estimate() {

	// dummy method for now
	return SPX_SELECTIVITY * DEFAULT_INTERFACE_RATE;
}

double sgah_qpn::get_rate_estimate() {

	// dummy method for now
	return SGAH_SELECTIVITY * DEFAULT_INTERFACE_RATE;
}

double rsgah_qpn::get_rate_estimate() {

	// dummy method for now
	return RSGAH_SELECTIVITY * DEFAULT_INTERFACE_RATE;
}

double sgahcwcb_qpn::get_rate_estimate() {

	// dummy method for now
	return SGAHCWCB_SELECTIVITY * DEFAULT_INTERFACE_RATE;
}

double mrg_qpn::get_rate_estimate() {

	// dummy method for now
	return MRG_SELECTIVITY * DEFAULT_INTERFACE_RATE;
}

double join_eq_hash_qpn::get_rate_estimate() {

	// dummy method for now
	return JOIN_EQ_HASH_SELECTIVITY * DEFAULT_INTERFACE_RATE;
}


//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
/////		Generate functors




//-------------------------------------------------------------------------
//			Code generation utilities.
//-------------------------------------------------------------------------

//		Globals referenced by generate utilities

static gb_table *segen_gb_tbl;            // Table of all group-by attributes.



//			Generate code that makes reference
//			to the tuple, and not to any aggregates.
//				NEW : it might reference a stateful function.
static string generate_se_code(scalarexp_t *se,table_list *schema){
	string ret;
    data_type *ldt, *rdt;
	int o;
	vector<scalarexp_t *> operands;


	switch(se->get_operator_type()){
	case SE_LITERAL:
		if(se->is_handle_ref()){
			sprintf(tmpstr,"handle_param_%d",se->get_handle_ref() );
			ret = tmpstr;
			return(ret);
		}
		if(se->get_literal()->is_cpx_lit()){
			sprintf(tmpstr,"complex_literal_%d",se->get_literal()->get_cpx_lit_ref() );
			ret = tmpstr;
			return(ret);
		}
		return(se->get_literal()->to_hfta_C_code("")); // not complex no constr.
	case SE_PARAM:
		if(se->is_handle_ref()){
			sprintf(tmpstr,"handle_param_%d",se->get_handle_ref() );
			ret = tmpstr;
			return(ret);
		}
		ret.append("param_");
		ret.append(se->get_param_name());
		return(ret);
	case SE_UNARY_OP:
        ldt = se->get_left_se()->get_data_type();
        if(ldt->complex_operator(se->get_op()) ){
			ret.append( ldt->get_complex_operator(se->get_op()) );
			ret.append("(");
			ret.append(generate_se_code(se->get_left_se(),schema));
            ret.append(")");
		}else{
			ret.append("(");
			ret.append(se->get_op());
			ret.append(generate_se_code(se->get_left_se(),schema));
			ret.append(")");
		}
		return(ret);
	case SE_BINARY_OP:
        ldt = se->get_left_se()->get_data_type();
        rdt = se->get_right_se()->get_data_type();

        if(ldt->complex_operator(rdt, se->get_op()) ){
			ret.append( ldt->get_complex_operator(rdt, se->get_op()) );
			ret.append("(");
			ret.append(generate_se_code(se->get_left_se(),schema));
			ret.append(", ");
			ret.append(generate_se_code(se->get_right_se(),schema));
			ret.append(")");
		}else{
			ret.append("(");
			ret.append(generate_se_code(se->get_left_se(),schema));
			ret.append(se->get_op());
			ret.append(generate_se_code(se->get_right_se(),schema));
			ret.append(")");
		}
		return(ret);
	case SE_COLREF:
		if(se->is_gb()){		// OK to ref gb attrs, but they're not yet unpacked ...
							// so return the defining code.
			int gref = se->get_gb_ref();
			scalarexp_t *gdef_se = segen_gb_tbl->get_def(gref);
			ret = generate_se_code(gdef_se, schema );

		}else{
    		sprintf(tmpstr,"unpack_var_%s_%d",
    		  se->get_colref()->get_field().c_str(), se->get_colref()->get_tablevar_ref() );
    		ret = tmpstr;
		}
		return(ret);
	case SE_FUNC:
		if(se->is_partial()){
			sprintf(tmpstr,"partial_fcn_result_%d",se->get_partial_ref());
			ret = tmpstr;
		}else{
			ret += se->op + "(";
			operands = se->get_operands();
			bool first_elem = true;
			if(se->get_storage_state() != ""){
				ret += "&(stval->state_var_"+se->get_storage_state()+"),cd";
				first_elem = false;
			}
			for(o=0;o<operands.size();o++){
				if(first_elem) first_elem=false; else ret += ", ";
				if(operands[o]->get_data_type()->is_buffer_type() &&
					(! (operands[o]->is_handle_ref()) ) )
					ret.append("&");
				ret += generate_se_code(operands[o], schema);
			}
			ret += ")";
		}
		return(ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in generate_se_code (hfta), line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		return("ERROR in generate_se_code");
	}
}

//		generate code that refers only to aggregate data and constants.
//			NEW : modified to handle superaggregates and stateful fcn refs.
//			Assume that the state is in *stval
static string generate_se_code_fm_aggr(scalarexp_t *se, string gbvar, string aggvar, table_list *schema){

	string ret;
    data_type *ldt, *rdt;
	int o;
	vector<scalarexp_t *> operands;


	switch(se->get_operator_type()){
	case SE_LITERAL:
		if(se->is_handle_ref()){
			sprintf(tmpstr,"handle_param_%d",se->get_handle_ref() );
			ret = tmpstr;
			return(ret);
		}
		if(se->get_literal()->is_cpx_lit()){
			sprintf(tmpstr,"complex_literal_%d",se->get_literal()->get_cpx_lit_ref() );
			ret = tmpstr;
			return(ret);
		}
		return(se->get_literal()->to_hfta_C_code("")); // not complex no constr.
	case SE_PARAM:
		if(se->is_handle_ref()){
			sprintf(tmpstr,"handle_param_%d",se->get_handle_ref() );
			ret = tmpstr;
			return(ret);
		}
		ret.append("param_");
		ret.append(se->get_param_name());
		return(ret);
	case SE_UNARY_OP:
        ldt = se->get_left_se()->get_data_type();
        if(ldt->complex_operator(se->get_op()) ){
			ret.append( ldt->get_complex_operator(se->get_op()) );
			ret.append("(");
			ret.append(generate_se_code_fm_aggr(se->get_left_se(),gbvar,aggvar,schema));
            ret.append(")");
		}else{
			ret.append("(");
			ret.append(se->get_op());
			ret.append(generate_se_code_fm_aggr(se->get_left_se(),gbvar,aggvar,schema));
			ret.append(")");
		}
		return(ret);
	case SE_BINARY_OP:
        ldt = se->get_left_se()->get_data_type();
        rdt = se->get_right_se()->get_data_type();

        if(ldt->complex_operator(rdt, se->get_op()) ){
			ret.append( ldt->get_complex_operator(rdt, se->get_op()) );
			ret.append("(");
			ret.append(generate_se_code_fm_aggr(se->get_left_se(),gbvar,aggvar,schema));
			ret.append(", ");
			ret.append(generate_se_code_fm_aggr(se->get_right_se(),gbvar,aggvar,schema));
			ret.append(")");
		}else{
			ret.append("(");
			ret.append(generate_se_code_fm_aggr(se->get_left_se(),gbvar,aggvar,schema));
			ret.append(se->get_op());
			ret.append(generate_se_code_fm_aggr(se->get_right_se(),gbvar,aggvar,schema));
			ret.append(")");
		}
		return(ret);
	case SE_COLREF:
		if(se->is_gb()){		// OK to ref gb attrs, but they're not yet unpacked ...
							// so return the defining code.
			sprintf(tmpstr,"%s%d",gbvar.c_str(),se->get_gb_ref());
			ret = tmpstr;

		}else{
    		fprintf(stderr,"ERROR reference to non-GB column ref not permitted here,"
				"error in query_plan.cc:generate_se_code_fm_aggr, line %d, character %d.\n",
				se->get_lineno(), se->get_charno());
    		ret = tmpstr;
		}
		return(ret);
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
		if(se->is_superaggr()){
			sprintf(tmpstr,"stval->aggr_var%d",se->get_aggr_ref());
		}else{
			sprintf(tmpstr,"%saggr_var%d",aggvar.c_str(),se->get_aggr_ref());
		}
		ret = tmpstr;
		return(ret);
	case SE_FUNC:
//				Is it a UDAF?
		if(se->get_aggr_ref() >= 0){
			sprintf(tmpstr,"udaf_ret_%d",se->get_aggr_ref());
			ret = tmpstr;
			return(ret);
		}

		if(se->is_partial()){
			sprintf(tmpstr,"partial_fcn_result_%d",se->get_partial_ref());
			ret = tmpstr;
		}else{
			ret += se->op + "(";
			bool first_elem = true;
			if(se->get_storage_state() != ""){
				ret += "&(stval->state_var_"+se->get_storage_state()+"),cd";
				first_elem = false;
			}
			operands = se->get_operands();
			for(o=0;o<operands.size();o++){
				if(first_elem) first_elem=false; else ret += ", ";
				if(operands[o]->get_data_type()->is_buffer_type() &&
					(! (operands[o]->is_handle_ref()) ) )
					ret.append("&");
				ret += generate_se_code_fm_aggr(operands[o], gbvar,aggvar, schema);
			}
			ret += ")";
		}
		return(ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in query_plan.cc::generate_se_code_fm_aggr, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		return("ERROR in generate_se_code_fm_aggr");
	}

}


static string unpack_partial_fcn_fm_aggr(scalarexp_t *se, int pfn_id, string gbvar, string aggvar, table_list *schema){
	string ret;
	int o;
	vector<scalarexp_t *> operands;


	if(se->get_operator_type() != SE_FUNC){
		fprintf(stderr,"INTERNAL ERROR, non-function SE passed to unpack_partial_fcn_fm_aggr. line %d, character %d\n",
				se->get_lineno(), se->get_charno());
		return("ERROR in unpack_partial_fcn_fm_aggr");
	}

	ret = "\tretval = " + se->get_op() + "( ",
	sprintf(tmpstr, "&partial_fcn_result_%d",pfn_id);
	ret += tmpstr;

	if(se->get_storage_state() != ""){
		ret += ",&(stval->state_var_"+se->get_storage_state()+"),cd";
	}

	operands = se->get_operands();
	for(o=0;o<operands.size();o++){
		ret += ", ";
		if(operands[o]->get_data_type()->is_buffer_type() &&
					(! (operands[o]->is_handle_ref()) ) )
			ret.append("&");
		ret += generate_se_code_fm_aggr(operands[o], gbvar,aggvar, schema);
	}
	ret += ");\n";

	return(ret);
}


static string unpack_partial_fcn(scalarexp_t *se, int pfn_id, table_list *schema){
	string ret;
	int o;
	vector<scalarexp_t *> operands;

	if(se->get_operator_type() != SE_FUNC){
		fprintf(stderr,"INTERNAL ERROR, non-function SE passed to unpack_partial_fcn. line %d, character %d\n",
				se->get_lineno(), se->get_charno());
		return("ERROR in unpack_partial_fcn");
	}

	ret = "\tretval = " + se->get_op() + "( ",
	sprintf(tmpstr, "&partial_fcn_result_%d",pfn_id);
	ret += tmpstr;

	if(se->get_storage_state() != ""){
		ret += ",&(stval->state_var_"+se->get_storage_state()+"),cd";
	}

	operands = se->get_operands();
	for(o=0;o<operands.size();o++){
		ret += ", ";
		if(operands[o]->get_data_type()->is_buffer_type() &&
					(! (operands[o]->is_handle_ref()) ) )
			ret.append("&");
		ret += generate_se_code(operands[o], schema);
	}
	ret += ");\n";

	return(ret);
}

static string generate_cached_fcn(scalarexp_t *se, int pfn_id, table_list *schema){
	string ret;
	int o;
	vector<scalarexp_t *> operands;

	if(se->get_operator_type() != SE_FUNC){
		fprintf(stderr,"INTERNAL ERROR, non-function SE passed to generate_cached_fcn. line %d, character %d\n",
				se->get_lineno(), se->get_charno());
		return("ERROR in generate_cached_fcn");
	}

	ret = se->get_op()+"(";

	if(se->get_storage_state() != ""){
		ret += "&(stval->state_var_"+se->get_storage_state()+"),cd,";
	}

	operands = se->get_operands();
	for(o=0;o<operands.size();o++){
		if(o) ret += ", ";
		if(operands[o]->get_data_type()->is_buffer_type() &&
					(! (operands[o]->is_handle_ref()) ) )
			ret.append("&");
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

	return("ERROR UNKNOWN BOOLEAN OPERATOR");
}


static string generate_predicate_code(predicate_t *pr,table_list *schema){
	string ret;
	vector<literal_t *>  litv;
	int i;
    data_type *ldt, *rdt;
	vector<scalarexp_t *> op_list;
	int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
        ldt = pr->get_left_se()->get_data_type();

		ret.append("( ");
		litv = pr->get_lit_vec();
		for(i=0;i<litv.size();i++){
			if(i>0) ret.append(" || ");
			ret.append("( ");

	        if(ldt->complex_comparison(ldt) ){
				ret.append( ldt->get_hfta_comparison_fcn(ldt) );
				ret.append("( ");
				if(ldt->is_buffer_type() )
					ret.append("&");
				ret.append(generate_se_code(pr->get_left_se(), schema));
				ret.append(", ");
				if(ldt->is_buffer_type() )
					ret.append("&");
				if(litv[i]->is_cpx_lit()){
					sprintf(tmpstr,"complex_literal_%d",litv[i]->get_cpx_lit_ref() );
					ret += tmpstr;
				}else{
					ret.append(litv[i]->to_C_code(""));
				}
				ret.append(") == 0");
			}else{
				ret.append(generate_se_code(pr->get_left_se(), schema));
				ret.append(" == ");
				ret.append(litv[i]->to_hfta_C_code(""));
			}

			ret.append(" )");
		}
		ret.append(" )");
		return(ret);

	case PRED_COMPARE:
        ldt = pr->get_left_se()->get_data_type();
        rdt = pr->get_right_se()->get_data_type();

		ret.append("( ");
        if(ldt->complex_comparison(rdt) ){
			ret.append(ldt->get_hfta_comparison_fcn(rdt));
			ret.append("(");
			if(ldt->is_buffer_type() )
				ret.append("&");
			ret.append(generate_se_code(pr->get_left_se(),schema) );
			ret.append(", ");
			if(rdt->is_buffer_type() )
				ret.append("&");
			ret.append(generate_se_code(pr->get_right_se(),schema) );
			ret.append(") ");
			ret.append( generate_C_comparison_op(pr->get_op()));
			ret.append("0");
		}else{
			ret.append(generate_se_code(pr->get_left_se(),schema) );
			ret.append( generate_C_comparison_op(pr->get_op()));
			ret.append(generate_se_code(pr->get_right_se(),schema) );
		}
		ret.append(" )");
		return(ret);
	case PRED_UNARY_OP:
		ret.append("( ");
		ret.append( generate_C_boolean_op(pr->get_op()) );
		ret.append(generate_predicate_code(pr->get_left_pr(),schema) );
		ret.append(" )");
		return(ret);
	case PRED_BINARY_OP:
		ret.append("( ");
		ret.append(generate_predicate_code(pr->get_left_pr(),schema) );
		ret.append( generate_C_boolean_op(pr->get_op()) );
		ret.append(generate_predicate_code(pr->get_right_pr(),schema) );
		ret.append(" )");
		return(ret);
	case PRED_FUNC:
		ret += pr->get_op() + "( ";
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			if(o>0) ret += ", ";
			if(op_list[o]->get_data_type()->is_buffer_type() && (! (op_list[o]->is_handle_ref()) ) )
					ret.append("&");
			ret += generate_se_code(op_list[o], schema);
		}
		ret += " )";
		return(ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in generate_predicate_code, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		return("ERROR in generate_predicate_code");
	}
}

static string generate_predicate_code_fm_aggr(predicate_t *pr, string gbvar, string aggvar,table_list *schema){
	string ret;
	vector<literal_t *>  litv;
	int i;
    data_type *ldt, *rdt;
	vector<scalarexp_t *> op_list;
	int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
        ldt = pr->get_left_se()->get_data_type();

		ret.append("( ");
		litv = pr->get_lit_vec();
		for(i=0;i<litv.size();i++){
			if(i>0) ret.append(" || ");
			ret.append("( ");

	        if(ldt->complex_comparison(ldt) ){
				ret.append( ldt->get_hfta_comparison_fcn(ldt) );
				ret.append("( ");
				if(ldt->is_buffer_type() )
					ret.append("&");
				ret.append(generate_se_code_fm_aggr(pr->get_left_se(), gbvar, aggvar, schema));
				ret.append(", ");
				if(ldt->is_buffer_type() )
					ret.append("&");
				if(litv[i]->is_cpx_lit()){
					sprintf(tmpstr,"complex_literal_%d",litv[i]->get_cpx_lit_ref() );
					ret += tmpstr;
				}else{
					ret.append(litv[i]->to_C_code(""));
				}
				ret.append(") == 0");
			}else{
				ret.append(generate_se_code_fm_aggr(pr->get_left_se(), gbvar, aggvar, schema));
				ret.append(" == ");
				ret.append(litv[i]->to_hfta_C_code(""));
			}

			ret.append(" )");
		}
		ret.append(" )");
		return(ret);

	case PRED_COMPARE:
        ldt = pr->get_left_se()->get_data_type();
        rdt = pr->get_right_se()->get_data_type();

		ret.append("( ");
        if(ldt->complex_comparison(rdt) ){
			ret.append(ldt->get_hfta_comparison_fcn(rdt));
			ret.append("(");
			if(ldt->is_buffer_type() )
				ret.append("&");
			ret.append(generate_se_code_fm_aggr(pr->get_left_se(), gbvar, aggvar,schema) );
			ret.append(", ");
			if(rdt->is_buffer_type() )
				ret.append("&");
			ret.append(generate_se_code_fm_aggr(pr->get_right_se(), gbvar, aggvar,schema) );
			ret.append(") ");
			ret.append( generate_C_comparison_op(pr->get_op()));
			ret.append("0");
		}else{
			ret.append(generate_se_code_fm_aggr(pr->get_left_se(), gbvar, aggvar,schema) );
			ret.append( generate_C_comparison_op(pr->get_op()));
			ret.append(generate_se_code_fm_aggr(pr->get_right_se(), gbvar, aggvar,schema) );
		}
		ret.append(" )");
		return(ret);
	case PRED_UNARY_OP:
		ret.append("( ");
		ret.append( generate_C_boolean_op(pr->get_op()) );
		ret.append(generate_predicate_code_fm_aggr(pr->get_left_pr(), gbvar, aggvar,schema) );
		ret.append(" )");
		return(ret);
	case PRED_BINARY_OP:
		ret.append("( ");
		ret.append(generate_predicate_code_fm_aggr(pr->get_left_pr(), gbvar, aggvar,schema) );
		ret.append( generate_C_boolean_op(pr->get_op()) );
		ret.append(generate_predicate_code_fm_aggr(pr->get_right_pr(), gbvar, aggvar,schema) );
		ret.append(" )");
		return(ret);
	case PRED_FUNC:
		ret += pr->get_op() + "( ";
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			if(o>0) ret += ", ";
			if(op_list[o]->get_data_type()->is_buffer_type() && (! (op_list[o]->is_handle_ref()) ) )
					ret.append("&");
			ret += generate_se_code_fm_aggr(op_list[o], gbvar, aggvar, schema);
		}
		ret += " )";
		return(ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in generate_predicate_code, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		return("ERROR in generate_predicate_code");
	}
}


//				Aggregation code


static string generate_equality_test(string &lhs_op, string &rhs_op, data_type *dt){
	string ret;

    if(dt->complex_comparison(dt) ){
		ret.append(dt->get_hfta_comparison_fcn(dt));
		ret.append("(");
			if(dt->is_buffer_type() )
				ret.append("&");
		ret.append(lhs_op);
		ret.append(", ");
			if(dt->is_buffer_type() )
				ret.append("&");
		ret.append(rhs_op );
		ret.append(") == 0");
	}else{
		ret.append(lhs_op );
		ret.append(" == ");
		ret.append(rhs_op );
	}

	return(ret);
}

static string generate_comparison(string &lhs_op, string &rhs_op, data_type *dt){
	string ret;

    if(dt->complex_comparison(dt) ){
		ret.append(dt->get_hfta_comparison_fcn(dt));
		ret.append("(");
			if(dt->is_buffer_type() )
				ret.append("&");
		ret.append(lhs_op);
		ret.append(", ");
			if(dt->is_buffer_type() )
				ret.append("&");
		ret.append(rhs_op );
		ret.append(") == 0");
	}else{
		ret.append(lhs_op );
		ret.append(" == ");
		ret.append(rhs_op );
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
		retval += op+"_HFTA_AGGR_UPDATE_(";
		if(atbl->get_storage_type(aidx)->get_type() != fstring_t) retval+="&";
		retval+="("+var+")";
		vector<scalarexp_t *> opl = atbl->get_operand_list(aidx);
		for(o=0;o<opl.size();++o){{
			retval += ",";
			if(opl[o]->get_data_type()->is_buffer_type() && (! (opl[o]->is_handle_ref()) ) )
					retval.append("&");
				retval += generate_se_code(opl[o], schema);
			}
		}
		retval += ");\n";

		return retval;
	}


//			builtin processing
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
		sprintf(tmpstr,"aggr_tmp_%d",aidx);
		retval += dt->make_host_cvar(tmpstr);
		retval += " = ";
		retval += generate_se_code(atbl->get_aggr_se(aidx), schema )+";\n";
		if(dt->complex_comparison(dt)){
			if(dt->is_buffer_type())
			  sprintf(tmpstr,"\t\tif(%s(&aggr_tmp_%d,&(%s)) < 0)\n",dt->get_hfta_comparison_fcn(dt).c_str(), aidx, var.c_str());
			else
			  sprintf(tmpstr,"\t\tif(%s(aggr_tmp_%d,%s) < 0)\n",dt->get_hfta_comparison_fcn(dt).c_str(), aidx, var.c_str());
		}else{
			sprintf(tmpstr,"\t\tif(aggr_tmp_%d < %s)\n",aidx,var.c_str());
		}
		retval.append(tmpstr);
		if(dt->is_buffer_type()){
			sprintf(tmpstr,"\t\t\t%s(&(%s),&aggr_tmp_%d);\n",dt->get_hfta_buffer_replace().c_str(),var.c_str(),aidx);
		}else{
			sprintf(tmpstr,"\t\t\t%s = aggr_tmp_%d;\n",var.c_str(),aidx);
		}
		retval.append(tmpstr);

		return(retval);
	}
	if(op == "MAX"){
		sprintf(tmpstr,"aggr_tmp_%d",aidx);
		retval+=dt->make_host_cvar(tmpstr);
		retval+=" = ";
		retval+=generate_se_code(atbl->get_aggr_se(aidx), schema )+";\n";
		if(dt->complex_comparison(dt)){
			if(dt->is_buffer_type())
			 sprintf(tmpstr,"\t\tif(%s(&aggr_tmp_%d,&(%s)) > 0)\n",dt->get_hfta_comparison_fcn(dt).c_str(), aidx, var.c_str());
			else
			 sprintf(tmpstr,"\t\tif(%s(aggr_tmp_%d,%s) > 0)\n",dt->get_hfta_comparison_fcn(dt).c_str(), aidx, var.c_str());
		}else{
			sprintf(tmpstr,"\t\tif(aggr_tmp_%d > %s)\n",aidx,var.c_str());
		}
		retval.append(tmpstr);
		if(dt->is_buffer_type()){
			sprintf(tmpstr,"\t\t\t%s(&(%s),&aggr_tmp_%d);\n",dt->get_hfta_buffer_replace().c_str(),var.c_str(),aidx);
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
	exit(1);
	return(retval);

}


//		superaggr minus.

static string generate_superaggr_minus(string var, string supervar, aggregate_table *atbl,int aidx, table_list *schema){
	string retval = "\t\t";
	string op = atbl->get_op(aidx);

//		Is it a UDAF
	if(! atbl->is_builtin(aidx)) {
		int o;
		retval += op+"_HFTA_AGGR_MINUS_(";
		if(atbl->get_storage_type(aidx)->get_type() != fstring_t) retval+="&";
		retval+="("+supervar+"),";
		if(atbl->get_storage_type(aidx)->get_type() != fstring_t) retval+="&";
		retval+="("+var+");\n";

		return retval;
	}


	if(op == "COUNT" || op == "SUM"){
		retval += supervar + "-=" +var + ";\n";
		return(retval);
	}

	if(op == "XOR_AGGR"){
		retval += supervar + "^=" +var + ";\n";
		return(retval);
	}

	if(op=="MIN" || op == "MAX")
		return "";

	fprintf(stderr,"INTERNAL ERROR : aggregate %s not recognized in generate_superaggr_minus.\n",op.c_str());
	exit(1);
	return(retval);

}




static string generate_aggr_init(string var, aggregate_table *atbl,int aidx, table_list *schema){
	string retval;
	string op = atbl->get_op(aidx);

//			UDAF processing
	if(! atbl->is_builtin(aidx)){
//			initialize
		retval += "\t"+atbl->get_op(aidx)+"_HFTA_AGGR_INIT_(";
		if(atbl->get_storage_type(aidx)->get_type() != fstring_t) retval+="&";
		retval+="("+var+"));\n";
//			Add 1st tupl
		retval += "\t"+atbl->get_op(aidx)+"_HFTA_AGGR_UPDATE_(";
		if(atbl->get_storage_type(aidx)->get_type() != fstring_t) retval+="&";
		retval+="("+var+")";
		vector<scalarexp_t *> opl = atbl->get_operand_list(aidx);
		int o;
		for(o=0;o<opl.size();++o){
			retval += ",";
			if(opl[o]->get_data_type()->is_buffer_type() && (! (opl[o]->is_handle_ref()) ) )
					retval.append("&");
				retval += generate_se_code(opl[o],schema);
			}
		retval += ");\n";
		return(retval);
	}

//			builtin aggregate processing
	data_type *dt = atbl->get_data_type(aidx);

	if(op == "COUNT"){
		retval = var;
		retval.append(" = 1;\n");
		return(retval);
	}

	if(op == "SUM" || op == "MIN" || op == "MAX" || op == "AND_AGGR" ||
									op == "OR_AGGR" || op == "XOR_AGGR"){
		if(dt->is_buffer_type()){
			sprintf(tmpstr,"\t\taggr_tmp_%d = %s;\n",aidx,generate_se_code(atbl->get_aggr_se(aidx), schema ).c_str() );
			retval.append(tmpstr);
			sprintf(tmpstr,"\t\t%s(&(%s),&aggr_tmp_%d);\n",dt->get_hfta_buffer_assign_copy().c_str(),var.c_str(),aidx);
			retval.append(tmpstr);
		}else{
			retval = var;
			retval += " = ";
			retval.append(generate_se_code(atbl->get_aggr_se(aidx), schema));
			retval.append(";\n");
		}
		return(retval);
	}

	fprintf(stderr,"INTERNAL ERROR : aggregate %s not recognized in generate_aggr_init.\n",op.c_str());
	exit(1);
	return(retval);

}



static string generate_aggr_reinitialize(string var, aggregate_table *atbl,int aidx, table_list *schema){
	string retval;
	string op = atbl->get_op(aidx);

//			UDAF processing
	if(! atbl->is_builtin(aidx)){
//			initialize
		retval +=  "\t"+atbl->get_op(aidx);
		if(atbl->is_running_aggr(aidx)){
			retval += "_HFTA_AGGR_REINIT_(";
		}else{
			retval += "_HFTA_AGGR_INIT_(";
		}
		if(atbl->get_storage_type(aidx)->get_type() != fstring_t) retval+="&";
		retval+="("+var+"));\n";
		return(retval);
	}

//			builtin aggregate processing
	data_type *dt = atbl->get_data_type(aidx);

	if(op == "COUNT"){
		retval = var;
		retval.append(" = 0;\n");
		return(retval);
	}

	if(op == "SUM" ||  op == "AND_AGGR" ||
									op == "OR_AGGR" || op == "XOR_AGGR"){
		if(dt->is_buffer_type()){
			return("ERROR, cannot yet handle reinitialization of builtin aggregates over strings.");
		}else{
			retval = var;
			retval += " = ";
			literal_t l(dt->type_indicator());
			retval.append(l.to_string());
			retval.append(";\n");
		}
		return(retval);
	}

	if(op == "MIN"){
		if(dt->is_buffer_type()){
			return("ERROR, cannot yet handle reinitialization of builtin aggregates over strings.");
		}else{
			retval = var;
			retval += " = ";
			retval.append(dt->get_max_literal());
			retval.append(";\n");
		}
		return(retval);
	}

	if(op == "MAX"){
		if(dt->is_buffer_type()){
			return("ERROR, cannot yet handle reinitialization of builtin aggregates over strings.");
		}else{
			retval = var;
			retval += " = ";
			retval.append(dt->get_min_literal());
			retval.append(";\n");
		}
		return(retval);
	}

	fprintf(stderr,"INTERNAL ERROR : aggregate %s not recognized in generate_aggr_reinitialize.\n",op.c_str());
	exit(1);
	return(retval);

}


//			Generate parameter holding vars from a param table.
static string generate_param_vars(param_table *param_tbl){
	string ret;
	int p;
	vector<string> param_vec = param_tbl->get_param_names();
	for(p=0;p<param_vec.size();p++){
		data_type *dt = param_tbl->get_data_type(param_vec[p]);
		sprintf(tmpstr,"param_%s;\n", param_vec[p].c_str());
		ret += "\t"+dt->make_host_cvar(tmpstr)+";\n";
		if(param_tbl->handle_access(param_vec[p])){
			ret += "\tstruct search_handle *param_handle_"+param_vec[p]+";\n";
		}
	}
	return(ret);
}

//			Parameter manipulation routines
static string generate_load_param_block(string functor_name,
							param_table *param_tbl,
							vector<handle_param_tbl_entry *> param_handle_table
							){
	int p;
	vector<string> param_names = param_tbl->get_param_names();

	string ret = "int load_params_"+functor_name+"(gs_int32_t sz, void *value){\n";
    ret.append("\tint pos=0;\n");
    ret.append("\tint data_pos;\n");

	for(p=0;p<param_names.size();p++){
		data_type *dt = param_tbl->get_data_type(param_names[p]);
		if(dt->is_buffer_type()){
			sprintf(tmpstr,"tmp_var_%s;\n", param_names[p].c_str());
			ret += "\t"+dt->make_host_cvar(tmpstr)+";\n";
		}
	}


//		Verify that the block is of minimum size
	if(param_names.size() > 0){
		ret += "//\tVerify that the value block is large enough */\n";
		ret.append("\n\tdata_pos = ");
		for(p=0;p<param_names.size();p++){
			if(p>0) ret.append(" + ");
			data_type *dt = param_tbl->get_data_type(param_names[p]);
			ret.append("sizeof( ");
			ret.append( dt->get_host_cvar_type() );
			ret.append(" )");
		}
		ret.append(";\n");
		ret.append("\tif(data_pos > sz) return 1;\n\n");
	}

///////////////////////
///		Verify that all strings can be unpacked.

	ret += "//\tVerify that the strings can be unpacked */\n";
	for(p=0;p<param_names.size();p++){
		data_type *dt = param_tbl->get_data_type(param_names[p]);
		if(dt->is_buffer_type()){
			sprintf(tmpstr,"\ttmp_var_%s =  *( (%s *)((gs_sp_t )value+pos) );\n",param_names[p].c_str(), dt->get_host_cvar_type().c_str() );
			ret.append(tmpstr);
			switch( dt->get_type() ){
			case v_str_t:
//				ret += "\ttmp_var_"+param_names[p]+".offset = ntohl( tmp_var_"+param_names[p]+".offset );\n";		// ntoh conversion
//				ret += "\ttmp_var_"+param_names[p]+".length = ntohl( tmp_var_"+param_names[p]+".length );\n";	// ntoh conversion
				sprintf(tmpstr,"\tif( (int)(tmp_var_%s.offset) + tmp_var_%s.length > sz) return 1;\n",param_names[p].c_str(), param_names[p].c_str() );
				ret.append(tmpstr);
				sprintf(tmpstr,"\ttmp_var_%s.offset = (int)( (gs_sp_t )value + (int)(tmp_var_%s.offset) );\n",param_names[p].c_str(), param_names[p].c_str() );
				ret.append(tmpstr);
			break;
			default:
				fprintf(stderr,"ERROR: parameter %s is of type %s, a buffered type, but I don't know how to unpack it as a parameter.\n",param_names[p].c_str(), dt->to_string().c_str() );
				exit(1);
			break;
			}
		}
		ret += "\tpos += sizeof( "+dt->get_host_cvar_type()+" );\n";
	}


/////////////////////////

	ret += "/*\tThe block is OK, do the unpacking.  */\n";
	ret += "\tpos = 0;\n";

	for(p=0;p<param_names.size();p++){
		data_type *dt = param_tbl->get_data_type(param_names[p]);
		if(dt->is_buffer_type()){
            sprintf(tmpstr,"\t%s(&param_%s, &tmp_var_%s);\n", dt->get_hfta_buffer_assign_copy().c_str(),param_names[p].c_str(),param_names[p].c_str() );
            ret.append(tmpstr);
		}else{
//			if(dt->needs_hn_translation()){
//				sprintf(tmpstr,"\tparam_%s =  %s( *( (%s *)( (gs_sp_t )value+pos) ) );\n",
//				  param_names[p].c_str(), dt->ntoh_translation().c_str(), dt->get_host_cvar_type().c_str() );
//			}else{
				sprintf(tmpstr,"\tparam_%s =  *( (%s *)( (gs_sp_t )value+pos) );\n",
				  param_names[p].c_str(), dt->get_host_cvar_type().c_str() );
//			}
			ret.append(tmpstr);
		}
		ret += "\tpos += sizeof( "+dt->get_host_cvar_type()+" );\n";
	}

//			TODO: I think this method of handle registration is obsolete
//			and should be deleted.
//			   some examination reveals that handle_access is always false.
	for(p=0;p<param_names.size();p++){
		if(param_tbl->handle_access(param_names[p]) ){
			data_type *pdt = param_tbl->get_data_type(param_names[p]);
//					create the new.
			ret += "\tt->param_handle_"+param_names[p]+" = " +
				pdt->handle_registration_name() +
				"((struct FTA *)t, &(t->param_"+param_names[p]+"));\n";
		}
	}
//			Register the pass-by-handle parameters

	ret += "/* register the pass-by-handle parameters */\n";

    int ph;
    for(ph=0;ph<param_handle_table.size();++ph){
		data_type pdt(param_handle_table[ph]->type_name);
		switch(param_handle_table[ph]->val_type){
		case cplx_lit_e:
			break;
		case litval_e:
			break;
		case param_e:
			sprintf(tmpstr,"\thandle_param_%d = %s(",ph,param_handle_table[ph]->lfta_registration_fcn().c_str());
			ret += tmpstr;
			if(pdt.is_buffer_type()) ret += "&(";
			ret += "param_"+param_handle_table[ph]->param_name;
			if(pdt.is_buffer_type()) ret += ")";
		    ret += ");\n";
			break;
		default:
			fprintf(stderr, "INTERNAL ERROR unknown case found when processing pass-by-handle parameter table.\n");
			exit(1);
		}
	}


	ret += "\treturn(0);\n";
	ret.append("}\n\n");

	return(ret);

}

static string generate_delete_param_block(string functor_name,
						param_table *param_tbl,
						vector<handle_param_tbl_entry *> param_handle_table
				){

	int p;
	vector<string> param_names = param_tbl->get_param_names();

	string ret = "void destroy_params_"+functor_name+"(){\n";

	for(p=0;p<param_names.size();p++){
		data_type *dt = param_tbl->get_data_type(param_names[p]);
		if(dt->is_buffer_type()){
			sprintf(tmpstr,"\t\t%s(&param_%s);\n",dt->get_hfta_buffer_destroy().c_str(),param_names[p].c_str());
			ret.append(tmpstr);
		}
		if(param_tbl->handle_access(param_names[p]) ){
			ret += "\t\t" + dt->get_handle_destructor() +
				"(t->param_handle_" + param_names[p] + ");\n";
		}
	}

	ret += "//\t\tDeregister handles.\n";
    int ph;
    for(ph=0;ph<param_handle_table.size();++ph){
		if(param_handle_table[ph]->val_type == param_e){
		  sprintf(tmpstr, "\t\t%s(handle_param_%d);\n",
			param_handle_table[ph]->lfta_deregistration_fcn().c_str(),ph);
		  ret += tmpstr;
		}
	}

	ret += "}\n\n";
	return ret;
}

// ---------------------------------------------------------------------
//		functions for creating functor variables.

static string generate_access_vars(col_id_set &cid_set, table_list *schema){
	string ret;
	col_id_set::iterator csi;

	for(csi=cid_set.begin(); csi!=cid_set.end();++csi){
    	int schref = (*csi).schema_ref;
		int tblref = (*csi).tblvar_ref;
		string field = (*csi).field;
		data_type dt(schema->get_type_name(schref,field));
		sprintf(tmpstr,"unpack_var_%s_%d", field.c_str(), tblref);
		ret+="\t"+dt.make_host_cvar(tmpstr)+";\n";
		sprintf(tmpstr,"\tgs_int32_t unpack_offset_%s_%d;\n", field.c_str(), tblref);
		ret.append(tmpstr);
	}
	return(ret);
}

static string generate_partial_fcn_vars(vector<scalarexp_t *> &partial_fcns,
	vector<int> &ref_cnt, vector<bool> &is_partial, bool gen_fcn_cache){
	string ret;
	int p;


	for(p=0;p<partial_fcns.size();++p){
		if(!gen_fcn_cache || is_partial[p] ||  ref_cnt[p]>1){
			sprintf(tmpstr,"partial_fcn_result_%d", p);
			ret+="\t"+partial_fcns[p]->get_data_type()->make_host_cvar(tmpstr)+";\n";
			if(gen_fcn_cache && ref_cnt[p]>1){
				ret+="\tint fcn_ref_cnt_"+int_to_string(p)+";\n";
			}
		}
	}
	return(ret);
}


static string generate_complex_lit_vars(cplx_lit_table *complex_literals){
	string ret;
    int cl;
    for(cl=0;cl<complex_literals->size();cl++){
        literal_t *l = complex_literals->get_literal(cl);
        data_type *dtl = new data_type( l->get_type() );
        sprintf(tmpstr,"complex_literal_%d",cl);
		ret += "\t"+dtl->make_host_cvar(tmpstr)+";\n";
        if(complex_literals->is_handle_ref(cl)){
            sprintf(tmpstr,"\tstruct search_handle *lit_handle_%d;\n",cl);
            ret.append(tmpstr);
        }
    }
	return(ret);
}


static string generate_pass_by_handle_vars(
				vector<handle_param_tbl_entry *> &param_handle_table){
	string ret;
	int p;

	for(p=0;p<param_handle_table.size();++p){
		sprintf(tmpstr,"\tgs_param_handle_t handle_param_%d;\n",p);
		ret += tmpstr;
	}

	return(ret);
}


// ------------------------------------------------------------
//		functions for generating initialization code.

static string gen_access_var_init(col_id_set &cid_set){
	string ret;
	col_id_set::iterator csi;

    for(csi=cid_set.begin(); csi!=cid_set.end();++csi){
        int tblref = (*csi).tblvar_ref;
        string field = (*csi).field;
        sprintf(tmpstr,"\tunpack_offset_%s_%d = ftaschema_get_field_offset_by_name(schema_handle%d, \"%s\");\n", field.c_str(),tblref,tblref,field.c_str());
        ret.append(tmpstr);
    }
	return ret;
}


static string gen_complex_lit_init(cplx_lit_table *complex_literals){
	string ret;

	int cl;
    for(cl=0;cl<complex_literals->size();cl++){
        literal_t *l = complex_literals->get_literal(cl);
//        sprintf(tmpstr,"\tcomplex_literal_%d = ",cl);
//        ret += tmpstr + l->to_hfta_C_code() + ";\n";
        sprintf(tmpstr,"&(complex_literal_%d)",cl);
        ret += "\t" + l->to_hfta_C_code(tmpstr) + ";\n";
//			I think that the code below is obsolete
//			TODO: it is obsolete.  add_cpx_lit is always
//			called with the handle indicator being false.
//			This entire structure should be cleansed.
        if(complex_literals->is_handle_ref(cl)){
            data_type *dt = new data_type( l->get_type() );
            sprintf(tmpstr,"\tlit_handle_%d = %s(&(f->complex_literal_%d));\n",
                cl, dt->hfta_handle_registration_name().c_str(), cl);
            ret += tmpstr;
            delete dt;
       }
    }
	return(ret);
}


static string gen_partial_fcn_init(vector<scalarexp_t *> &partial_fcns){
	string ret;

	int p;
	for(p=0;p<partial_fcns.size();++p){
		data_type *pdt =partial_fcns[p]->get_data_type();
		literal_t empty_lit(pdt->type_indicator());
		if(pdt->is_buffer_type()){
//			sprintf(tmpstr,"\tpartial_fcn_result_%d = %s;\n",
//				 p, empty_lit.to_hfta_C_code().c_str());
			sprintf(tmpstr,"&(partial_fcn_result_%d)",p);
			ret += "\t"+empty_lit.to_hfta_C_code(tmpstr)+";\n";
		}
	}
	return(ret);
}

static string gen_pass_by_handle_init(
				vector<handle_param_tbl_entry *> &param_handle_table){
	string ret;

    int ph;
    for(ph=0;ph<param_handle_table.size();++ph){
		data_type pdt(param_handle_table[ph]->type_name);
		sprintf(tmpstr,"\thandle_param_%d = %s(",ph,param_handle_table[ph]->lfta_registration_fcn().c_str());
		switch(param_handle_table[ph]->val_type){
		case cplx_lit_e:
			ret += tmpstr;
			if(pdt.is_buffer_type()) ret += "&(";
			sprintf(tmpstr,"complex_literal_%d",param_handle_table[ph]->complex_literal_idx);
			ret += tmpstr;
			if(pdt.is_buffer_type()) ret += ")";
			ret += ");\n";
			break;
		case litval_e:
			ret += tmpstr;
			ret += param_handle_table[ph]->litval->to_hfta_C_code("") + ");\n";
//			ret += ");\n";
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
	return(ret);
}

//------------------------------------------------------------
//			functions for destructor and deregistration code

static string gen_complex_lit_dtr(cplx_lit_table *complex_literals){
	string ret;

	int cl;
    for(cl=0;cl<complex_literals->size();cl++){
        literal_t *l = complex_literals->get_literal(cl);
		data_type ldt(  l->get_type() );
        if(ldt.is_buffer_type()){
			sprintf(tmpstr,"\t\t%s(&complex_literal_%d);\n",
			  ldt.get_hfta_buffer_destroy().c_str(), cl );
            ret += tmpstr;
        }
    }
	return(ret);
}


static string gen_pass_by_handle_dtr(
				vector<handle_param_tbl_entry *> &param_handle_table){
	string ret;

	int ph;
    for(ph=0;ph<param_handle_table.size();++ph){
		sprintf(tmpstr, "\t\t%s(handle_param_%d);\n",
			param_handle_table[ph]->lfta_deregistration_fcn().c_str(),ph);
		ret += tmpstr;
	}
	return(ret);
}

//			Destroy all previous results
static string gen_partial_fcn_dtr(vector<scalarexp_t *> &partial_fcns){
	string ret;

	int p;
	for(p=0;p<partial_fcns.size();++p){
		data_type *pdt =partial_fcns[p]->get_data_type();
		if(pdt->is_buffer_type()){
			sprintf(tmpstr,"\t\t%s(&partial_fcn_result_%d);\n",
			  pdt->get_hfta_buffer_destroy().c_str(), p );
			ret += tmpstr;
		}
	}
	return(ret);
}

//		Destroy previsou results of fcns in pfcn_set
static string gen_partial_fcn_dtr(vector<scalarexp_t *> &partial_fcns, set<int> &pfcn_set){
	string ret;
	set<int>::iterator si;

	for(si=pfcn_set.begin(); si!=pfcn_set.end(); ++si){
		data_type *pdt =partial_fcns[(*si)]->get_data_type();
		if(pdt->is_buffer_type()){
			sprintf(tmpstr,"\t\t%s(&partial_fcn_result_%d);\n",
			  pdt->get_hfta_buffer_destroy().c_str(), (*si) );
			ret += tmpstr;
		}
	}
	return(ret);
}


//-------------------------------------------------------------------------
//			Functions related to se generation bookkeeping.

static void get_new_pred_cids(predicate_t *pr, col_id_set &found_cids,
								col_id_set &new_cids, gb_table *gtbl){
	col_id_set this_pred_cids;
	col_id_set::iterator csi;

//				get colrefs in predicate not already found.
   	gather_pr_col_ids(pr,this_pred_cids,gtbl);
	set_difference(this_pred_cids.begin(), this_pred_cids.end(),
					   found_cids.begin(), found_cids.end(),
						inserter(new_cids,new_cids.begin()) );

//				We've found these cids, so update found_cids
	for(csi=new_cids.begin(); csi!=new_cids.end(); ++csi)
		found_cids.insert((*csi));

}

//		after the call, new_cids will have the colrefs in se but not found_cids.
//		update found_cids with the new cids.
static void get_new_se_cids(scalarexp_t *se, col_id_set &found_cids,
								col_id_set &new_cids, gb_table *gtbl){
	col_id_set this_se_cids;
	col_id_set::iterator csi;

//				get colrefs in se not already found.
   	gather_se_col_ids(se,this_se_cids,gtbl);
	set_difference(this_se_cids.begin(), this_se_cids.end(),
					   found_cids.begin(), found_cids.end(),
						inserter(new_cids,new_cids.begin()) );

//				We've found these cids, so update found_cids
	for(csi=new_cids.begin(); csi!=new_cids.end(); ++csi)
		found_cids.insert((*csi));

}

static string gen_unpack_cids(table_list *schema, col_id_set &new_cids, string on_problem, vector<bool> &needs_xform){
	string ret;
	col_id_set::iterator csi;

	for(csi=new_cids.begin(); csi!=new_cids.end(); ++csi){
    	int schref = (*csi).schema_ref;
	    int tblref = (*csi).tblvar_ref;
    	string field = (*csi).field;
		data_type dt(schema->get_type_name(schref,field));
		string unpack_fcn;
		if(needs_xform[tblref]){
			unpack_fcn = dt.get_hfta_unpack_fcn();
		}else{
			unpack_fcn = dt.get_hfta_unpack_fcn_noxf();
		}
		if(dt.is_buffer_type()){
			sprintf(tmpstr,"\t unpack_var_%s_%d = %s(tup%d.data, tup%d.tuple_size, unpack_offset_%s_%d, &problem);\n",field.c_str(), tblref, unpack_fcn.c_str(), tblref, tblref, field.c_str(), tblref);
		}else{
			sprintf(tmpstr,"\t unpack_var_%s_%d = %s_nocheck(tup%d.data,  unpack_offset_%s_%d);\n",field.c_str(), tblref, unpack_fcn.c_str(), tblref,  field.c_str(), tblref);
		}
		ret += tmpstr;
		if(dt.is_buffer_type()){
			ret += "\tif(problem) return "+on_problem+" ;\n";
		}
  	}
	return(ret);
}

// generates the declaration of all the variables related to
// temp tuples generation
static string gen_decl_temp_vars(){
	string ret;

	ret += "\t// variables related to temp tuple generation\n";
	ret += "\tbool temp_tuple_received;\n";

	return(ret);
}

// generates initialization code for variables related to temp tuple processing
static string gen_init_temp_vars(table_list *schema, vector<select_element *>& select_list, gb_table *gtbl){
	string ret;
	col_id_set::iterator csi;
	int s;

//		Initialize internal state
	ret += "\ttemp_tuple_received = false;\n";

	col_id_set temp_cids;	// colrefs unpacked thus far.

	for(s=0;s<select_list.size();s++){
		if (select_list[s]->se->get_data_type()->is_temporal()) {
//			Find the set of	attributes accessed in this SE
			col_id_set new_cids;
			get_new_se_cids(select_list[s]->se,temp_cids, new_cids, gtbl);

			// init these vars
			for(csi=new_cids.begin(); csi!=new_cids.end(); ++csi){
				int schref = (*csi).schema_ref;
				int tblref = (*csi).tblvar_ref;
				string field = (*csi).field;
				data_type dt(schema->get_type_name(schref,field), schema->get_modifier_list(schref,field));

				sprintf(tmpstr,"\t unpack_var_%s_%d = %s;\n", field.c_str(), tblref,
					dt.is_increasing() ? dt.get_min_literal().c_str() : dt.get_max_literal().c_str());
				ret += tmpstr;
			}
		}
	}
	return(ret);
}



// generates a check if tuple is temporal
static string gen_temp_tuple_check(string node_name, int channel) {
	string ret;

	char tmpstr[256];
	sprintf(tmpstr, "tup%d", channel);
	string tup_name = tmpstr;
	sprintf(tmpstr, "schema_handle%d", channel);
	string schema_handle_name = tmpstr;
	string tuple_offset_name = "tuple_metadata_offset"+int_to_string(channel);

//			check if it is a temporary status tuple
	ret += "\t// check if tuple is temp status tuple\n";
//		ret += "\tif (ftaschema_is_temporal_tuple(" + schema_handle_name + ", " + tup_name + ".data)) {\n";
	ret += "\tif (ftaschema_is_temporal_tuple_offset(" + tuple_offset_name + ", " + tup_name + ".data)) {\n";
	ret += "\t\ttemp_tuple_received = true;\n";
	ret += "\t}\n";
	ret += "\telse\n\t\ttemp_tuple_received = false;\n\n";

	return(ret);
}

// generates unpacking code for all temporal attributes referenced in select
static string gen_unpack_temp_vars(table_list *schema, col_id_set& found_cids, vector<select_element *>& select_list, gb_table *gtbl, vector<bool> &needs_xform) {
	string ret;
	int s;

//		Unpack all the temporal attributes references in select list
//		we need it to be able to generate temp status tuples
	for(s=0;s<select_list.size();s++){
		if (select_list[s]->se->get_data_type()->is_temporal()) {
//			Find the set of	attributes accessed in this SE
			col_id_set new_cids;
			get_new_se_cids(select_list[s]->se,found_cids, new_cids, gtbl);
//			Unpack these values.
			ret += gen_unpack_cids(schema, new_cids, "false", needs_xform);
		}
	}

	return(ret);
}


//		Generates temporal tuple generation code (except attribute packing)
static string gen_init_temp_status_tuple(string node_name) {
	string ret;

	ret += "\t// create temp status tuple\n";
	ret += "\tresult.tuple_size = sizeof("+generate_tuple_name( node_name)+") + sizeof(gs_uint8_t);\n";
	ret += "\tresult.data = (gs_sp_t )malloc(result.tuple_size);\n";
	ret += "\tresult.heap_resident = true;\n";
	ret += "\t//		Mark tuple as temporal\n";
	ret += "\t*((gs_sp_t )result.data + sizeof("+generate_tuple_name( node_name)+")) = TEMPORAL_TUPLE;\n";

	ret += "\t"+generate_tuple_name( node_name)+" *tuple = ("+
		generate_tuple_name( node_name) +" *)(result.data);\n";

	return(ret);
}


//		Assume that all colrefs unpacked already ...
static string gen_unpack_partial_fcn(table_list *schema,
					vector<scalarexp_t *> &partial_fcns,set<int> &pfcn_refs,
					string on_problem){
	string ret;
	set<int>::iterator si;

//			Since set<..> is a "Sorted Associative Container",
//			we can walk through it in sorted order by walking from
//			begin() to end().  (and the partial fcns must be
//			evaluated in this order).
	for(si=pfcn_refs.begin();si!=pfcn_refs.end();++si){
		ret += unpack_partial_fcn(partial_fcns[(*si)], (*si), schema);
		ret += "\tif(retval) return "+on_problem+" ;\n";
	}
	return(ret);
}

//		Assume that all colrefs unpacked already ...
//		this time with cached functions.
static string gen_unpack_partial_fcn(table_list *schema,
					vector<scalarexp_t *> &partial_fcns,set<int> &pfcn_refs,
					vector<int> &fcn_ref_cnt, vector<bool> &is_partial_fcn,
					string on_problem){
	string ret;
	set<int>::iterator si;

//			Since set<..> is a "Sorted Associative Container",
//			we can walk through it in sorted order by walking from
//			begin() to end().  (and the partial fcns must be
//			evaluated in this order).
	for(si=pfcn_refs.begin();si!=pfcn_refs.end();++si){
		if(fcn_ref_cnt[(*si)] > 1){
			ret += "\tif(fcn_ref_cnt_"+int_to_string((*si))+"==0){\n";
		}
		if(is_partial_fcn[(*si)]){
			ret += unpack_partial_fcn(partial_fcns[(*si)], (*si), schema);
			ret += "\tif(retval) return "+on_problem+" ;\n";
		}
		if(fcn_ref_cnt[(*si)] > 1){
			if(!is_partial_fcn[(*si)]){
				ret += "\t\tpartial_fcn_result_"+int_to_string((*si))+"="+generate_cached_fcn(partial_fcns[(*si)],(*si),schema)+";\n";
			}
			ret += "\t\tfcn_ref_cnt_"+int_to_string((*si))+"=1;\n";
			ret += "\t}\n";
		}
	}

	return(ret);
}


//		This version finds and unpacks new colrefs.
//		found_cids gets updated with the newly unpacked cids.
static string gen_full_unpack_partial_fcn(table_list *schema,
					vector<scalarexp_t *> &partial_fcns,set<int> &pfcn_refs,
					col_id_set &found_cids, gb_table *gtbl, string on_problem,
					vector<bool> &needs_xform){
	string ret;
	set<int>::iterator slsi;

	for(slsi=pfcn_refs.begin(); slsi!=pfcn_refs.end(); ++slsi){
//			find all new fields ref'd by this partial fcn.
		col_id_set new_cids;
		get_new_se_cids(partial_fcns[(*slsi)], found_cids, new_cids, gtbl);
//			Unpack these values.
		ret += gen_unpack_cids(schema, new_cids, on_problem, needs_xform);

//			Now evaluate the partial fcn.
		ret += unpack_partial_fcn(partial_fcns[(*slsi)], (*slsi), schema);
		ret += "\tif(retval) return "+on_problem+" ;\n";
	}
	return(ret);
}

//		This version finds and unpacks new colrefs.
//		found_cids gets updated with the newly unpacked cids.
//			BUT : only for the partial functions.
static string gen_full_unpack_partial_fcn(table_list *schema,
					vector<scalarexp_t *> &partial_fcns,set<int> &pfcn_refs,
					vector<int> &fcn_ref_cnt, vector<bool> &is_partial_fcn,
					col_id_set &found_cids, gb_table *gtbl, string on_problem,
					vector<bool> &needs_xform){
	string ret;
	set<int>::iterator slsi;

	for(slsi=pfcn_refs.begin(); slsi!=pfcn_refs.end(); ++slsi){
	  if(is_partial_fcn[(*slsi)]){
//			find all new fields ref'd by this partial fcn.
		col_id_set new_cids;
		get_new_se_cids(partial_fcns[(*slsi)], found_cids, new_cids, gtbl);
//			Unpack these values.
		ret += gen_unpack_cids(schema, new_cids, on_problem, needs_xform);

//			Now evaluate the partial fcn.
		if(fcn_ref_cnt[(*slsi)] > 1){
			ret += "\tif(fcn_ref_cnt_"+int_to_string((*slsi))+"==0){\n";
		}
		if(is_partial_fcn[(*slsi)]){
			ret += unpack_partial_fcn(partial_fcns[(*slsi)], (*slsi), schema);
			ret += "\tif(retval) return "+on_problem+" ;\n";
		}
		if(fcn_ref_cnt[(*slsi)] > 1){
			ret += "\t\tfcn_ref_cnt_"+int_to_string((*slsi))+"=1;\n";
			ret += "\t}\n";
		}

	  }
	}
	return(ret);
}

static string gen_remaining_cached_fcns(table_list *schema,
					vector<scalarexp_t *> &partial_fcns,set<int> &pfcn_refs,
					vector<int> &fcn_ref_cnt, vector<bool> &is_partial_fcn){
	string ret;
	set<int>::iterator slsi;

	for(slsi=pfcn_refs.begin(); slsi!=pfcn_refs.end(); ++slsi){
	  if(!is_partial_fcn[(*slsi)] && fcn_ref_cnt[(*slsi)] > 1){

		if(fcn_ref_cnt[(*slsi)] > 1){
			ret += "\tif(fcn_ref_cnt_"+int_to_string((*slsi))+"==0){\n";
			ret += "\t\tpartial_fcn_result_"+int_to_string((*slsi))+"="+generate_cached_fcn(partial_fcns[(*slsi)],(*slsi),schema)+";\n";
			ret += "\t\tfcn_ref_cnt_"+int_to_string((*slsi))+"=1;\n";
			ret += "\t}\n";
		}
	  }
	}
	return(ret);
}


//		unpack the colrefs in cid_set not in found_cids
static string gen_remaining_colrefs(table_list *schema,
			col_id_set &cid_set, col_id_set &found_cids, string on_problem,
			vector<bool> &needs_xform){
	string ret;
	col_id_set::iterator csi;

	for(csi=cid_set.begin(); csi!=cid_set.end();csi++){
		if(found_cids.count( (*csi) ) == 0){
    		int schref = (*csi).schema_ref;
		    int tblref = (*csi).tblvar_ref;
    		string field = (*csi).field;
			data_type dt(schema->get_type_name(schref,field));
			string unpack_fcn;
			if(needs_xform[tblref]){
				unpack_fcn = dt.get_hfta_unpack_fcn();
			}else{
				unpack_fcn = dt.get_hfta_unpack_fcn_noxf();
			}
			if(dt.is_buffer_type()){
				sprintf(tmpstr,"\t unpack_var_%s_%d = %s(tup%d.data, tup%d.tuple_size, unpack_offset_%s_%d, &problem);\n",field.c_str(), tblref, unpack_fcn.c_str(), tblref, tblref, field.c_str(), tblref);
			}else{
				sprintf(tmpstr,"\t unpack_var_%s_%d = %s_nocheck(tup%d.data, unpack_offset_%s_%d);\n",field.c_str(), tblref, unpack_fcn.c_str(), tblref, field.c_str(), tblref);
			}
			ret += tmpstr;
			if(dt.is_buffer_type()){
  				ret.append("\tif(problem) return "+on_problem+" ;\n");
			}
		}
	}
	return(ret);
}

static string gen_buffer_selvars(table_list *schema,
								vector<select_element *> &select_list){
	string ret;
	int s;

    for(s=0;s<select_list.size();s++){
		scalarexp_t *se = select_list[s]->se;
        data_type *sdt = se->get_data_type();
        if(sdt->is_buffer_type() &&
			!( (se->get_operator_type() == SE_COLREF) ||
			   (se->get_operator_type() == SE_FUNC && se->is_partial()) ||
			   (se->get_operator_type() == SE_FUNC && se->get_aggr_ref() >= 0))
		){
            sprintf(tmpstr,"selvar_%d",s);
			ret+="\t"+sdt->make_host_cvar(tmpstr)+" = ";
			ret += generate_se_code(se,schema) +";\n";
        }
    }
	return(ret);
}

static string gen_buffer_selvars_size(vector<select_element *> &select_list,table_list *schema){
	string ret;
	int s;

    for(s=0;s<select_list.size();s++){
		scalarexp_t *se = select_list[s]->se;
        data_type *sdt = se->get_data_type();
        if(sdt->is_buffer_type()){
		  if( !( (se->get_operator_type() == SE_COLREF) ||
			   (se->get_operator_type() == SE_FUNC && se->is_partial()) ||
			   (se->get_operator_type() == SE_FUNC && se->get_aggr_ref() >= 0))
		  ){
            sprintf(tmpstr," + %s(&selvar_%d)", sdt->get_hfta_buffer_size().c_str(),s);
            ret.append(tmpstr);
		  }else{
            sprintf(tmpstr," + %s(&%s)", sdt->get_hfta_buffer_size().c_str(),
				generate_se_code(se,schema).c_str());
            ret.append(tmpstr);
		  }
        }
    }
	return(ret);
}

static string gen_buffer_selvars_dtr(vector<select_element *> &select_list){
	string ret;
	int s;

    for(s=0;s<select_list.size();s++){
		scalarexp_t *se = select_list[s]->se;
        data_type *sdt = se->get_data_type();
        if(sdt->is_buffer_type() &&
			!( (se->get_operator_type() == SE_COLREF) ||
				(se->get_operator_type() == SE_AGGR_STAR) ||
				(se->get_operator_type() == SE_AGGR_SE) ||
			   (se->get_operator_type() == SE_FUNC && se->is_partial()) ||
			   (se->get_operator_type() == SE_FUNC && se->get_aggr_ref() >= 0))
			){
				sprintf(tmpstr,"\t\t%s(&selvar_%d);\n",
			  	  sdt->get_hfta_buffer_destroy().c_str(), s );
            	ret += tmpstr;
        }
    }
	return(ret);
}


static string gen_pack_tuple(table_list *schema, vector<select_element *> &select_list, string node_name, bool temporal_only){
	string ret;
	int s;

	ret += "\tint tuple_pos = sizeof("+generate_tuple_name(node_name)+") + sizeof(gs_uint8_t);\n";
    for(s=0;s<select_list.size();s++){
		scalarexp_t *se  = select_list[s]->se;
        data_type *sdt = se->get_data_type();

        if(!temporal_only && sdt->is_buffer_type()){
		  if( !( (se->get_operator_type() == SE_COLREF) ||
			   (se->get_operator_type() == SE_FUNC && se->is_partial()))
			){
            	sprintf(tmpstr,"\t%s(&(tuple->tuple_var%d), &selvar_%d, ((gs_sp_t )tuple)+tuple_pos, tuple_pos);\n", sdt->get_hfta_buffer_tuple_copy().c_str(),s, s);
            	ret.append(tmpstr);
            	sprintf(tmpstr,"\ttuple_pos += %s(&selvar_%d);\n", sdt->get_hfta_buffer_size().c_str(), s);
            	ret.append(tmpstr);
			}else{
            	sprintf(tmpstr,"\t%s(&(tuple->tuple_var%d), &%s, ((gs_sp_t )tuple)+tuple_pos, tuple_pos);\n", sdt->get_hfta_buffer_tuple_copy().c_str(),s, generate_se_code(se,schema).c_str());
            	ret.append(tmpstr);
            	sprintf(tmpstr,"\ttuple_pos += %s(&%s);\n", sdt->get_hfta_buffer_size().c_str(), generate_se_code(se,schema).c_str());
            	ret.append(tmpstr);
			}
        }else if (!temporal_only || sdt->is_temporal()) {
            sprintf(tmpstr,"\ttuple->tuple_var%d = ",s);
            ret.append(tmpstr);
            ret.append(generate_se_code(se,schema) );
            ret.append(";\n");
        }
    }
	return(ret);
}


//-------------------------------------------------------------------------
//			functor generation methods
//-------------------------------------------------------------------------

/////////////////////////////////////////////////////////
////			File Output Operator
string output_file_qpn::generate_functor_name(){
	return("output_file_functor_" + normalize_name(get_node_name()));
}


string output_file_qpn::generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, vector<bool> &needs_xform){
	string ret = "class " + this->generate_functor_name() + "{\n";

//		Find the temporal field
	int temporal_field_idx;
	data_type *tdt = NULL;
	for(temporal_field_idx=0;temporal_field_idx<fields.size();temporal_field_idx++){
		tdt = new data_type(fields[temporal_field_idx]->get_type(), fields[temporal_field_idx]->get_modifier_list());
		if(tdt->is_temporal()){
			break;
		}else{
			delete tdt;
		}
	}

	if(temporal_field_idx == fields.size()){
		fprintf(stderr,"ERROR, no temporal field for file output operator %s\n",node_name.c_str());
		exit(1);
	}

	ret += "private:\n";

	// var to save the schema handle
	ret += "\tint schema_handle0;\n";
//			tuple metadata offset
	ret += "\tint tuple_metadata_offset0;\n";
	sprintf(tmpstr,"\tgs_int32_t unpack_offset_%s_0;\n",  fields[temporal_field_idx]->get_name().c_str());
	ret.append(tmpstr);

//		For unpacking the hashing fields, if any
	int h;
	for(h=0;h<hash_flds.size();++h){
		sprintf(tmpstr,"unpack_var_%s", fields[hash_flds[h]]->get_name().c_str());
		data_type *hdt = new data_type(fields[hash_flds[h]]->get_type(), fields[hash_flds[h]]->get_modifier_list());
		ret+="\t"+hdt->make_host_cvar(tmpstr)+";\n";
		if(hash_flds[h]!=temporal_field_idx){
			sprintf(tmpstr,"\tgs_int32_t unpack_offset_%s_0;\n",  fields[hash_flds[h]]->get_name().c_str());
			ret.append(tmpstr);
		}
	}
//		Specail case for output file hashing
	if(n_streams>1 && hash_flds.size()==0){
		ret+="\tgs_uint32_t outfl_cnt;\n";
	}

	ret += "//\t\tRemember the last posted timestamp.\n";
	ret+="\t"+tdt->make_host_cvar("timestamp")+";\n";
	ret+="\t"+tdt->make_host_cvar("last_bucket")+";\n";
	ret+="\t"+tdt->make_host_cvar("slack")+";\n";
	ret += "\tbool first_execution;\n";
	ret += "\tbool temp_tuple_received;\n";
	ret += "\tbool is_eof;\n";

	ret += "\tgs_int32_t bucketwidth;\n";

	ret += "public:\n";
//-------------------
//			The functor constructor
//			pass in a schema handle (e.g. for the 1st input stream),
//			use it to determine how to unpack the merge variable.
//			ASSUME that both streams have the same layout,
//			just duplicate it.

//		unpack vars
	ret += "//\t\tFunctor constructor.\n";
	ret +=  this->generate_functor_name()+"(int schema_hndl){\n";

	ret += "\tschema_handle0 = schema_hndl;\n";
//		tuple metadata offset
	ret += "\ttuple_metadata_offset0 = ftaschema_get_tuple_metadata_offset(schema_handle0);\n";

	if(output_spec->bucketwidth == 0)
		ret += "\tbucketwidth = 60;\n";
	else
		ret += "\tbucketwidth = "+int_to_string(output_spec->bucketwidth)+";\n";
	ret += "//\t\tGet offsets for unpacking fields from input tuple.\n";

   sprintf(tmpstr,"\tunpack_offset_%s_0 = ftaschema_get_field_offset_by_name(schema_handle0, \"%s\");\n", fields[temporal_field_idx]->get_name().c_str(), fields[temporal_field_idx]->get_name().c_str());
   ret.append(tmpstr);
//		Hashing field unpacking, if any
	for(h=0;h<hash_flds.size();++h){
		if(hash_flds[h]!=temporal_field_idx){
			sprintf(tmpstr,"\tunpack_offset_%s_0 = ftaschema_get_field_offset_by_name(schema_handle0, \"%s\");\n",  fields[hash_flds[h]]->get_name().c_str(),fields[hash_flds[h]]->get_name().c_str());
			ret.append(tmpstr);
		}
	}

	ret+="\tfirst_execution = true;\n";

//		Initialize internal state
	ret += "\ttemp_tuple_received = false;\n";

	//		Init last timestamp values to minimum value for their type
	if (tdt->is_increasing()){
		ret+="\ttimestamp = " + tdt->get_min_literal() + ";\n";
		ret+="\tlast_bucket = " + tdt->get_min_literal() + ";\n";
	}else{
		ret+="\ttimestamp = " + tdt->get_max_literal() + ";\n";
		ret+="\tlast_bucket = " + tdt->get_max_literal() + ";\n";
	}


	ret += "};\n\n";

	ret += "//\t\tFunctor destructor.\n";
	ret +=  "~"+this->generate_functor_name()+"(){\n";
	ret+="};\n\n";


	ret += "int load_params_"+this->generate_functor_name()+"(gs_int32_t sz, void *value){return 0;}\n";
	ret += "void destroy_params_"+this->generate_functor_name()+"(){}\n";

//			Register new parameter block
	ret += "int set_param_block(gs_int32_t sz, void* value){\n";
	  ret += "\tthis->destroy_params_"+this->generate_functor_name()+"();\n";
	  ret += "\treturn this->load_params_"+this->generate_functor_name()+
				"(sz, value);\n";
	ret += "};\n\n";

	ret+="\nbool temp_status_received(const host_tuple& tup0)/* const*/   {\n";
	ret+="\tgs_int32_t problem;\n";

	ret += "\tvoid *tup_ptr = (void *)(&tup0);\n";
	ret += "\tis_eof = ftaschema_is_eof_tuple(schema_handle0,tup_ptr);\n";

	ret += gen_temp_tuple_check(this->node_name, 0);

	sprintf(tmpstr,"\ttimestamp = %s_nocheck(tup0.data, unpack_offset_%s_%d);\n",  tdt->get_hfta_unpack_fcn_noxf().c_str(), fields[temporal_field_idx]->get_name().c_str(), 0);
	ret += tmpstr;

	for(h=0;h<hash_flds.size();++h){
		data_type *hdt = new data_type(fields[hash_flds[h]]->get_type(), fields[hash_flds[h]]->get_modifier_list());
		sprintf(tmpstr,"\tunpack_var_%s = %s_nocheck(tup0.data, unpack_offset_%s_%d);\n", fields[hash_flds[h]]->get_name().c_str(), hdt->get_hfta_unpack_fcn_noxf().c_str(), fields[hash_flds[h]]->get_name().c_str(), 0);
	ret += tmpstr;
	}
	ret +=
"	return temp_tuple_received;\n"
"}\n"
"\n"
;

	ret +=
"bool new_epoch(){\n"
"	if(first_execution || (last_bucket + 1) * bucketwidth <= timestamp){\n"
"		last_bucket = timestamp / bucketwidth;\n"
"		first_execution = false;\n"
"		return true;\n"
"	}\n"
"	return false;\n"
"}\n"
"\n"
;

	if(n_streams <= 1){
		ret+=
"inline gs_uint32_t output_hash(){return 0;}\n\n";
	}else{
		if(hash_flds.size()==0){
			ret +=
"gs_uint32_t output_hash(){\n"
"	outfl_cnt++;\n"
"	if(outfl_cnt >= "+int_to_string(n_streams)+")\n"
"		outfl_cnt = 0;\n"
"	return outfl_cnt;\n"
"}\n"
"\n"
;
		}else{
			ret +=
"gs_uint32_t output_hash(){\n"
"	gs_uint32_t ret = "
;
			for(h=0;h<hash_flds.size();++h){
				if(h>0) ret += "^";
				data_type *hdt = new data_type(fields[hash_flds[h]]->get_type(), fields[hash_flds[h]]->get_modifier_list());
				if(hdt->use_hashfunc()){
					sprintf(tmpstr,"%s(&(unpack_var_%s))",hdt->get_hfta_hashfunc().c_str(),fields[hash_flds[h]]->get_name().c_str());
				}else{
					sprintf(tmpstr,"unpack_var_%s",fields[hash_flds[h]]->get_name().c_str());
				}
				ret += tmpstr;
			}
			ret +=
";\n"
"	return  ret % "+int_to_string(hash_flds.size())+";\n"
"}\n\n"
;
		}
	}

ret +=
"gs_uint32_t num_file_streams(){\n"
"	return("+int_to_string(n_streams)+");\n"
"}\n\n"
;

	ret +=
"string get_filename_base(){\n"
"	char tmp_fname[500];\n";

	string output_filename_base = hfta_query_name+filestream_id;
/*
	if(n_hfta_clones > 1){
		output_filename_base += "_"+int_to_string(parallel_idx);
	}
*/



	if(output_spec->output_directory == "")
		ret +=
"	sprintf(tmp_fname,\""+output_filename_base+"_%lld\",(gs_int64_t)(last_bucket*bucketwidth));\n";
		else ret +=
"	sprintf(tmp_fname,\""+output_spec->output_directory+"/"+output_filename_base+"_%lld\",(gs_int64_t)(last_bucket*bucketwidth));\n";
ret +=
"	return (string)(tmp_fname);\n"
"}\n"
"\n";


ret+=
"bool do_compression(){\n";
	if(do_gzip)
		ret += "	return true;\n";
	else
		ret += "	return false;\n";
ret+=
"}\n"
"\n"
"bool is_eof_tuple(){\n"
"	return is_eof;\n"
"}\n"
"\n"
"bool propagate_tuple(){\n"
;
if(eat_input)
	ret+="\treturn false;\n";
else
	ret+="\treturn true;\n";
ret+="}\n\n";
//		create a temp status tuple
	ret += "int create_temp_status_tuple(host_tuple& result) {\n\n";

	ret += gen_init_temp_status_tuple(this->hfta_query_name);

	sprintf(tmpstr,"\ttuple->tuple_var%d = timestamp;\n",temporal_field_idx);


	ret += tmpstr;

	ret += "\treturn 0;\n";
	ret += "}\n\n";
	ret += "};\n\n";

	return ret;
}


string output_file_qpn::generate_operator(int i, string params){
	string optype = "file_output_operator";
	switch(compression_type){
	case regular:
		optype = "file_output_operator";
	break;
	case gzip:
		optype = "zfile_output_operator";
	break;
	case bzip:
		optype = "bfile_output_operator";
	break;
	}

		return("	"+optype+"<" +
		generate_functor_name() +
		"> *op"+int_to_string(i)+" = new "+optype+"<"+
		generate_functor_name() +">("+params+", \"" + hfta_query_name + "\""
		+ "," + hfta_query_name + "_schema_definition);\n");
}

/////////////////////////////////////////////////////////
//////			SPX functor


string spx_qpn::generate_functor_name(){
	return("spx_functor_" + normalize_name(normalize_name(this->get_node_name())));
}

string spx_qpn::generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, vector<bool> &needs_xform){
//			Initialize generate utility globals
	segen_gb_tbl = NULL;

	string ret = "class " + this->generate_functor_name() + "{\n";

//			Find variables referenced in this query node.

  col_id_set cid_set;
  col_id_set::iterator csi;

	int w, s, p;
    for(w=0;w<where.size();++w)
    	gather_pr_col_ids(where[w]->pr,cid_set,NULL);
    for(s=0;s<select_list.size();s++){
    	gather_se_col_ids(select_list[s]->se,cid_set,NULL);
    }


//			Private variables : store the state of the functor.
//			1) variables for unpacked attributes
//			2) offsets of the upacked attributes
//			3) storage of partial functions
//			4) storage of complex literals (i.e., require a constructor)

	ret += "private:\n";
	ret += "\tbool first_execution;\t// internal processing state \n";
	ret += "\tint schema_handle0;\n";

	// generate the declaration of all the variables related to
	// temp tuples generation
	ret += gen_decl_temp_vars();


//			unpacked attribute storage, offsets
	ret += "//\t\tstorage and offsets of accessed fields.\n";
	ret += generate_access_vars(cid_set,schema);
//			tuple metadata management
	ret += "\tint tuple_metadata_offset0;\n";

//			Variables to store results of partial functions.
//			WARNING find_partial_functions modifies the SE
//			(it marks the partial function id).
	ret += "//\t\tParital function result storage\n";
	vector<scalarexp_t *> partial_fcns;
	vector<int> fcn_ref_cnt;
	vector<bool> is_partial_fcn;
	for(s=0;s<select_list.size();s++){
		find_partial_fcns(select_list[s]->se, &partial_fcns,&fcn_ref_cnt,&is_partial_fcn, Ext_fcns);
	}
	for(w=0;w<where.size();w++){
		find_partial_fcns_pr(where[w]->pr, &partial_fcns, &fcn_ref_cnt,&is_partial_fcn,Ext_fcns);
	}
//		Unmark non-partial expensive functions referenced only once.
	for(p=0; p<partial_fcns.size();p++){
		if(!is_partial_fcn[p] && fcn_ref_cnt[p] <= 1){
			partial_fcns[p]->set_partial_ref(-1);
		}
	}
	if(partial_fcns.size()>0){
	  ret += generate_partial_fcn_vars(partial_fcns,fcn_ref_cnt,is_partial_fcn,true);
	}

//			Complex literals (i.e., they need constructors)
	ret += "//\t\tComplex literal storage.\n";
	cplx_lit_table *complex_literals = this->get_cplx_lit_tbl(Ext_fcns);
	ret += generate_complex_lit_vars(complex_literals);

//			Pass-by-handle parameters
	ret += "//\t\tPass-by-handle storage.\n";
	vector<handle_param_tbl_entry *> param_handle_table = this->get_handle_param_tbl(Ext_fcns);
	ret += generate_pass_by_handle_vars(param_handle_table);

//			Variables to hold parameters
	ret += "//\tfor query parameters\n";
	ret += generate_param_vars(param_tbl);


//			The publicly exposed functions

	ret += "\npublic:\n";


//-------------------
//			The functor constructor
//			pass in the schema handle.
//			1) make assignments to the unpack offset variables
//			2) initialize the complex literals
//			3) Set the initial values of the temporal attributes
//				referenced in select clause (in case we need to emit
//				temporal tuple before receiving first tuple )

	ret += "//\t\tFunctor constructor.\n";
	ret +=  this->generate_functor_name()+"(int schema_handle0){\n";

//		save schema handle
	ret += "this->schema_handle0 = schema_handle0;\n";

//		unpack vars
	ret += "//\t\tGet offsets for unpacking fields from input tuple.\n";
	ret += gen_access_var_init(cid_set);
//		tuple metadata
	ret += "\ttuple_metadata_offset0 = ftaschema_get_tuple_metadata_offset(schema_handle0);\n";

//		complex literals
	ret += "//\t\tInitialize complex literals.\n";
	ret += gen_complex_lit_init(complex_literals);

//		Initialize partial function results so they can be safely GC'd
	ret += gen_partial_fcn_init(partial_fcns);

//		Initialize non-query-parameter parameter handles
	ret += gen_pass_by_handle_init(param_handle_table);

//		Init temporal attributes referenced in select list
	ret += gen_init_temp_vars(schema, select_list, NULL);

	ret += "};\n\n";


//-------------------
//			Functor destructor
	ret += "//\t\tFunctor destructor.\n";
	ret +=  "~"+this->generate_functor_name()+"(){\n";

//		clean up buffer-type complex literals.
	ret += gen_complex_lit_dtr(complex_literals);

//			Deregister the pass-by-handle parameters
	ret += "/* register and de-register the pass-by-handle parameters */\n";
	ret += gen_pass_by_handle_dtr(param_handle_table);

//			Reclaim buffer space for partial fucntion results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";
	ret += gen_partial_fcn_dtr(partial_fcns);


//			Destroy the parameters, if any need to be destroyed
	ret += "\tthis->destroy_params_"+this->generate_functor_name()+"();\n";

	ret += "};\n\n";


//-------------------
//			Parameter manipulation routines
	ret += generate_load_param_block(this->generate_functor_name(),
									this->param_tbl,param_handle_table );
	ret += generate_delete_param_block(this->generate_functor_name(),
									this->param_tbl,param_handle_table);


//-------------------
//			Register new parameter block
	ret += "int set_param_block(gs_int32_t sz, void* value){\n";
	  ret += "\tthis->destroy_params_"+this->generate_functor_name()+"();\n";
	  ret += "\treturn this->load_params_"+this->generate_functor_name()+
				"(sz, value);\n";
	ret += "};\n\n";


//-------------------
//			The selection predicate.
//			Unpack variables for 1 cnf element
//			at a time, return false immediately if the
//			predicate fails.
//			optimization : evaluate the cheap cnf elements
//			first, the expensive ones last.

	ret += "bool predicate(host_tuple &tup0){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure
//		Initialize cached function indicators.
	for(p=0;p<partial_fcns.size();++p){
		if(fcn_ref_cnt[p]>1){
			ret+="\tfcn_ref_cnt_"+int_to_string(p)+"=0;\n";
		}
	}


	ret += gen_temp_tuple_check(this->node_name, 0);

	if(partial_fcns.size()>0){		// partial fcn access failure
	  ret += "\tgs_retval_t retval = 0;\n";
	  ret += "\n";
	}

//			Reclaim buffer space for partial fucntion results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";
	ret += gen_partial_fcn_dtr(partial_fcns);

	col_id_set found_cids;	// colrefs unpacked thus far.
	ret += gen_unpack_temp_vars(schema, found_cids, select_list, NULL, needs_xform);

//		For temporal status tuple we don't need to do anything else
	ret += "\tif (temp_tuple_received) return false;\n\n";


	for(w=0;w<where.size();++w){
		sprintf(tmpstr,"//\t\tPredicate clause %d.\n",w);
		ret += tmpstr;
//			Find the set of	variables accessed in this CNF elem,
//			but in no previous element.
		col_id_set new_cids;
		get_new_pred_cids(where[w]->pr,found_cids, new_cids, NULL);
//			Unpack these values.
		ret += gen_unpack_cids(schema, new_cids, "false", needs_xform);
//			Find partial fcns ref'd in this cnf element
		set<int> pfcn_refs;
		collect_partial_fcns_pr(where[w]->pr, pfcn_refs);
		ret += gen_unpack_partial_fcn(schema,partial_fcns,pfcn_refs,fcn_ref_cnt, is_partial_fcn, "false");

		ret += "\tif( !("+generate_predicate_code(where[w]->pr,schema)+
				+") ) return(false);\n";
	}

//		The partial functions ref'd in the select list
//		must also be evaluated.  If one returns false,
//		then implicitly the predicate is false.
	set<int> sl_pfcns;
	for(s=0;s<select_list.size();s++){
		collect_partial_fcns(select_list[s]->se, sl_pfcns);
	}
	if(sl_pfcns.size() > 0)
		ret += "//\t\tUnpack remaining partial fcns.\n";
	ret += gen_full_unpack_partial_fcn(schema, partial_fcns, sl_pfcns,
					fcn_ref_cnt, is_partial_fcn,
					found_cids, NULL, "false", needs_xform);

//			Unpack remaining fields
	ret += "//\t\tunpack any remaining fields from the input tuple.\n";
	ret += gen_remaining_colrefs(schema, cid_set, found_cids, "false", needs_xform);


	ret += "\treturn(true);\n";
	ret += "};\n\n";


//-------------------
//			The output tuple function.
//			Unpack the remaining attributes into
//			the placeholder variables, unpack the
//			partial fcn refs, then pack up the tuple.

	ret += "host_tuple create_output_tuple() {\n";
	ret += "\thost_tuple tup;\n";
	ret += "\tgs_retval_t retval = 0;\n";

//			Unpack any remaining cached functions.
	ret += gen_remaining_cached_fcns(schema, partial_fcns, sl_pfcns,
					fcn_ref_cnt, is_partial_fcn);


//          Now, compute the size of the tuple.

//          Unpack any BUFFER type selections into temporaries
//          so that I can compute their size and not have
//          to recompute their value during tuple packing.
//          I can use regular assignment here because
//          these temporaries are non-persistent.

	ret += "//\t\tCompute the size of the tuple.\n";
	ret += "//\t\t\tNote: buffer storage packed at the end of the tuple.\n";

//			Unpack all buffer type selections, to be able to compute their size
	ret += gen_buffer_selvars(schema, select_list);

//      The size of the tuple is the size of the tuple struct plus the
//      size of the buffers to be copied in.


      ret+="\ttup.tuple_size = sizeof(" + generate_tuple_name( this->get_node_name()) +") + sizeof(gs_uint8_t)";
	ret += gen_buffer_selvars_size(select_list,schema);
	ret.append(";\n");

//		Allocate tuple data block.
	ret += "//\t\tCreate the tuple block.\n";
	  ret += "\ttup.data = malloc(tup.tuple_size);\n";
	  ret += "\ttup.heap_resident = true;\n";
//		Mark tuple as regular
	  ret += "\t*((gs_sp_t )tup.data + sizeof(" + generate_tuple_name( this->get_node_name()) +")) = REGULAR_TUPLE;\n";

//	  ret += "\ttup.channel = 0;\n";
	  ret += "\t"+generate_tuple_name( this->get_node_name())+" *tuple = ("+
				generate_tuple_name( this->get_node_name())+" *)(tup.data);\n";

//		Start packing.
//			(Here, offsets are hard-wired.  is this a problem?)

	ret += "//\t\tPack the fields into the tuple.\n";
	ret += gen_pack_tuple(schema,select_list,this->get_node_name(), false );

//			Delete string temporaries
	ret += gen_buffer_selvars_dtr(select_list);

	ret += "\treturn tup;\n";
	ret += "};\n";

//-------------------------------------------------------------------
//		Temporal update functions

	ret += "bool temp_status_received(){return temp_tuple_received;};\n\n";


//		create a temp status tuple
	ret += "int create_temp_status_tuple(host_tuple& result) {\n\n";

	ret += gen_init_temp_status_tuple(this->get_node_name());

//		Start packing.
//			(Here, offsets are hard-wired.  is this a problem?)

	ret += "//\t\tPack the fields into the tuple.\n";
	ret += gen_pack_tuple(schema,select_list,this->get_node_name(), true );

	ret += "\treturn 0;\n";
	ret += "};};\n\n";

	return(ret);
}


string spx_qpn::generate_operator(int i, string params){

		return("	select_project_operator<" +
		generate_functor_name() +
		"> *op"+int_to_string(i)+" = new select_project_operator<"+
		generate_functor_name() +">("+params+", \"" + get_node_name() + "\");\n");
}


////////////////////////////////////////////////////////////////
////	SGAH functor



string sgah_qpn::generate_functor_name(){
	return("sgah_functor_" + normalize_name(this->get_node_name()));
}


string sgah_qpn::generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, vector<bool> &needs_xform){
	int a,g,w,s;


//			Initialize generate utility globals
	segen_gb_tbl = &(gb_tbl);

//		Might need to generate empty values for cube processing.
	map<int, string> structured_types;
	for(g=0;g<gb_tbl.size();++g){
		if(gb_tbl.get_data_type(g)->is_structured_type()){
			structured_types[gb_tbl.get_data_type(g)->type_indicator()] = gb_tbl.get_data_type(g)->get_type_str();
		}
	}

//--------------------------------
//			group definition class
	string ret = "class " + generate_functor_name() + "_groupdef{\n";
	ret += "public:\n";
	for(g=0;g<this->gb_tbl.size();g++){
		sprintf(tmpstr,"gb_var%d",g);
		ret+="\t"+this->gb_tbl.get_data_type(g)->make_host_cvar(tmpstr)+";\n";
	}
//		empty strucutred literals
	map<int, string>::iterator sii;
	for(sii=structured_types.begin();sii!=structured_types.end();++sii){
		data_type dt(sii->second);
		literal_t empty_lit(sii->first);
		ret += "\t"+dt.make_host_cvar(empty_lit.hfta_empty_literal_name())+";\n";
	}
//		Constructors
	if(structured_types.size()==0){
		ret += "\t"+generate_functor_name() + "_groupdef(){};\n";
	}else{
		ret += "\t"+generate_functor_name() + "_groupdef(){}\n";
	}


	ret += "\t"+generate_functor_name() + "_groupdef("+
		this->generate_functor_name() + "_groupdef *gd){\n";
	for(g=0;g<gb_tbl.size();g++){
		data_type *gdt = gb_tbl.get_data_type(g);
		if(gdt->is_buffer_type()){
			sprintf(tmpstr,"\t\t%s(&gb_var%d, &(gd->gb_var%d));\n",
			  gdt->get_hfta_buffer_assign_copy().c_str(),g,g );
			ret += tmpstr;
		}else{
			sprintf(tmpstr,"\t\tgb_var%d = gd->gb_var%d;\n",g,g);
			ret += tmpstr;
		}
	}
	ret += "\t}\n";
	ret += "\t"+generate_functor_name() + "_groupdef("+
		this->generate_functor_name() + "_groupdef *gd, bool *pattern){\n";
	for(sii=structured_types.begin();sii!=structured_types.end();++sii){
		literal_t empty_lit(sii->first);
		ret += "\t\t"+empty_lit.to_hfta_C_code("&"+empty_lit.hfta_empty_literal_name())+";\n";
	}
	for(g=0;g<gb_tbl.size();g++){
		data_type *gdt = gb_tbl.get_data_type(g);
		ret += "\t\tif(pattern["+int_to_string(g)+"]){\n";
		if(gdt->is_buffer_type()){
			sprintf(tmpstr,"\t\t\t%s(&gb_var%d, &(gd->gb_var%d));\n",
			  gdt->get_hfta_buffer_assign_copy().c_str(),g,g );
			ret += tmpstr;
		}else{
			sprintf(tmpstr,"\t\t\tgb_var%d = gd->gb_var%d;\n",g,g);
			ret += tmpstr;
		}
		ret += "\t\t}else{\n";
		literal_t empty_lit(gdt->type_indicator());
		if(empty_lit.is_cpx_lit()){
			ret +="\t\t\tgb_var"+int_to_string(g)+"= "+empty_lit.hfta_empty_literal_name()+";\n";
		}else{
			ret +="\t\t\tgb_var"+int_to_string(g)+"="+empty_lit.to_hfta_C_code("")+";\n";
		}
		ret += "\t\t}\n";
	}
	ret += "\t};\n";
//		destructor
	ret += "\t~"+ generate_functor_name() + "_groupdef(){\n";
	for(g=0;g<gb_tbl.size();g++){
		data_type *gdt = gb_tbl.get_data_type(g);
		if(gdt->is_buffer_type()){
			sprintf(tmpstr,"\t\t%s(&gb_var%d);\n",
			  gdt->get_hfta_buffer_destroy().c_str(), g );
			ret += tmpstr;
		}
	}
	ret += "\t};\n";

	data_type *tgdt;
	for(g=0;g<gb_tbl.size();g++){
  		data_type *gdt = gb_tbl.get_data_type(g);
  		if(gdt->is_temporal()){
			tgdt = gdt;
			break;
		}
	}
	ret += tgdt->get_host_cvar_type()+" get_curr_gb(){\n";
	ret+="\treturn gb_var"+int_to_string(g)+";\n";
	ret+="}\n";

	ret +="};\n\n";

//--------------------------------
//			aggr definition class
	ret += "class " + this->generate_functor_name() + "_aggrdef{\n";
	ret += "public:\n";
	for(a=0;a<aggr_tbl.size();a++){
aggr_table_entry *ate = aggr_tbl.agr_tbl[a];
		sprintf(tmpstr,"aggr_var%d",a);
		if(aggr_tbl.is_builtin(a))
		  ret+="\t"+ aggr_tbl.get_data_type(a)->make_host_cvar(tmpstr)+";\n";
		else
		  ret+="\t"+ aggr_tbl.get_storage_type(a)->make_host_cvar(tmpstr)+";\n";
	}
//		Constructors
	ret += "\t"+this->generate_functor_name() + "_aggrdef(){};\n";
//		destructor
	ret += "\t~"+this->generate_functor_name() + "_aggrdef(){\n";
	for(a=0;a<aggr_tbl.size();a++){
		if(aggr_tbl.is_builtin(a)){
			data_type *adt = aggr_tbl.get_data_type(a);
			if(adt->is_buffer_type()){
				sprintf(tmpstr,"\t\t%s(&aggr_var%d);\n",
			  	adt->get_hfta_buffer_destroy().c_str(), a );
				ret += tmpstr;
			}
		}else{
			ret+="\t\t"+aggr_tbl.get_op(a)+"_HFTA_AGGR_DESTROY_(";
			if(aggr_tbl.get_storage_type(a)->get_type() != fstring_t) ret+="&";
			ret+="(aggr_var"+int_to_string(a)+"));\n";
		}
	}
	ret += "\t};\n";
	ret +="};\n\n";

//-------------------------------------------
//		group-by patterns for the functor,
//		initialization within the class is cumbersome.
	int n_patterns = gb_tbl.gb_patterns.size();
	int i,j;
	ret += "bool "+this->generate_functor_name()+"_gb_patterns["+int_to_string(n_patterns)+
			"]["+int_to_string(gb_tbl.size())+"] = {\n";
	if(n_patterns == 0){
		for(i=0;i<gb_tbl.size();++i){
			if(i>0) ret += ",";
			ret += "true";
		}
	}else{
		for(i=0;i<n_patterns;++i){
			if(i>0) ret += ",\n";
			ret += "\t{";
			for(j=0;j<gb_tbl.size();j++){
				if(j>0) ret += ", ";
				if(gb_tbl.gb_patterns[i][j]){
					ret += "true";
				}else{
					ret += "false";
				}
			}
			ret += "}";
		}
		ret += "\n";
	}
	ret += "};\n";


//--------------------------------
//			gb functor class
	ret += "class " + this->generate_functor_name() + "{\n";

//			Find variables referenced in this query node.

  col_id_set cid_set;
  col_id_set::iterator csi;

    for(w=0;w<where.size();++w)
    	gather_pr_col_ids(where[w]->pr,cid_set,segen_gb_tbl);
    for(w=0;w<having.size();++w)
    	gather_pr_col_ids(having[w]->pr,cid_set,segen_gb_tbl);
	for(g=0;g<gb_tbl.size();g++)
		gather_se_col_ids(gb_tbl.get_def(g),cid_set,segen_gb_tbl);

    for(s=0;s<select_list.size();s++){
    	gather_se_col_ids(select_list[s]->se,cid_set,segen_gb_tbl);	// descends into aggregates
    }


//			Private variables : store the state of the functor.
//			1) variables for unpacked attributes
//			2) offsets of the upacked attributes
//			3) storage of partial functions
//			4) storage of complex literals (i.e., require a constructor)

	ret += "private:\n";

	// var to save the schema handle
	ret += "\tint schema_handle0;\n";
	// metadata from schema handle
	ret += "\tint tuple_metadata_offset0;\n";

	// generate the declaration of all the variables related to
	// temp tuples generation
	ret += gen_decl_temp_vars();

//			unpacked attribute storage, offsets
	ret += "//\t\tstorage and offsets of accessed fields.\n";
	ret += generate_access_vars(cid_set, schema);

//			Variables to store results of partial functions.
//			WARNING find_partial_functions modifies the SE
//			(it marks the partial function id).
	ret += "//\t\tParital function result storage\n";
	vector<scalarexp_t *> partial_fcns;
	vector<int> fcn_ref_cnt;
	vector<bool> is_partial_fcn;
	for(s=0;s<select_list.size();s++){
		find_partial_fcns(select_list[s]->se, &partial_fcns,NULL,NULL,  Ext_fcns);
	}
	for(w=0;w<where.size();w++){
		find_partial_fcns_pr(where[w]->pr, &partial_fcns,NULL,NULL,  Ext_fcns);
	}
	for(w=0;w<having.size();w++){
		find_partial_fcns_pr(having[w]->pr, &partial_fcns,NULL,NULL,  Ext_fcns);
	}
	for(g=0;g<gb_tbl.size();g++){
		find_partial_fcns(gb_tbl.get_def(g), &partial_fcns,NULL,NULL,  Ext_fcns);
	}
	for(a=0;a<aggr_tbl.size();a++){
		find_partial_fcns(aggr_tbl.get_aggr_se(a), &partial_fcns,NULL,NULL,  Ext_fcns);
	}
	if(partial_fcns.size()>0){
	  ret += "/*\t\tVariables for storing results of partial functions. \t*/\n";
	  ret += generate_partial_fcn_vars(partial_fcns,fcn_ref_cnt,is_partial_fcn,false);
	}

//			Complex literals (i.e., they need constructors)
	ret += "//\t\tComplex literal storage.\n";
	cplx_lit_table *complex_literals = this->get_cplx_lit_tbl(Ext_fcns);
	ret += generate_complex_lit_vars(complex_literals);

//			Pass-by-handle parameters
	ret += "//\t\tPass-by-handle storage.\n";
	vector<handle_param_tbl_entry *> param_handle_table = this->get_handle_param_tbl(Ext_fcns);
	ret += generate_pass_by_handle_vars(param_handle_table);


//			variables to hold parameters.
	ret += "//\tfor query parameters\n";
	ret += generate_param_vars(param_tbl);

//		Is there a temporal flush?  If so create flush temporaries,
//		create flush indicator.
	bool uses_temporal_flush = false;
	for(g=0;g<gb_tbl.size();g++){
		data_type *gdt = gb_tbl.get_data_type(g);
		if(gdt->is_temporal())
			uses_temporal_flush = true;
	}

	if(uses_temporal_flush){
		ret += "//\t\tFor temporal flush\n";
		for(g=0;g<gb_tbl.size();g++){
			data_type *gdt = gb_tbl.get_data_type(g);
			if(gdt->is_temporal()){
			  sprintf(tmpstr,"last_gb%d",g);
			  ret+="\t"+gb_tbl.get_data_type(g)->make_host_cvar(tmpstr)+";\n";
			  sprintf(tmpstr,"last_flushed_gb%d",g);
			  ret+="\t"+gb_tbl.get_data_type(g)->make_host_cvar(tmpstr)+";\n";
			}
		}
		ret += "\tbool needs_temporal_flush;\n";
	}


//			The publicly exposed functions

	ret += "\npublic:\n";


//-------------------
//			The functor constructor
//			pass in the schema handle.
//			1) make assignments to the unpack offset variables
//			2) initialize the complex literals

	ret += "//\t\tFunctor constructor.\n";
	ret +=  this->generate_functor_name()+"(int schema_handle0){\n";

	// save the schema handle
	ret += "\t\tthis->schema_handle0 = schema_handle0;\n";

//		unpack vars
	ret += "//\t\tGet offsets for unpacking fields from input tuple.\n";
	ret += gen_access_var_init(cid_set);
//		tuple metadata
	ret += "tuple_metadata_offset0 = ftaschema_get_tuple_metadata_offset(schema_handle0);\n";

//		complex literals
	ret += "//\t\tInitialize complex literals.\n";
	ret += gen_complex_lit_init(complex_literals);

//		Initialize partial function results so they can be safely GC'd
	ret += gen_partial_fcn_init(partial_fcns);

//		Initialize non-query-parameter parameter handles
	ret += gen_pass_by_handle_init(param_handle_table);

//		temporal flush variables
//		ASSUME that structured values won't be temporal.
	if(uses_temporal_flush){
		ret += "//\t\tInitialize temporal flush variables.\n";
		for(g=0;g<gb_tbl.size();g++){
			data_type *gdt = gb_tbl.get_data_type(g);
			if(gdt->is_temporal()){
				literal_t gl(gdt->type_indicator());
				sprintf(tmpstr,"\tlast_gb%d = %s;\n",g, gl.to_hfta_C_code("").c_str());
				ret.append(tmpstr);
				sprintf(tmpstr,"\tlast_flushed_gb%d = %s;\n",g, gl.to_hfta_C_code("").c_str());
				ret.append(tmpstr);
			}
		}
		ret += "\tneeds_temporal_flush = false;\n";
	}

	//		Init temporal attributes referenced in select list
	ret += gen_init_temp_vars(schema, select_list, segen_gb_tbl);

	ret += "}\n\n";

//-------------------
//			Functor destructor
	ret += "//\t\tFunctor destructor.\n";
	ret +=  "~"+this->generate_functor_name()+"(){\n";

//			clean up buffer type complex literals
	ret += gen_complex_lit_dtr(complex_literals);

//			Deregister the pass-by-handle parameters
	ret += "/* register and de-register the pass-by-handle parameters */\n";
	ret += gen_pass_by_handle_dtr(param_handle_table);

//			clean up partial function results.
	ret += "/* clean up partial function storage	*/\n";
	ret += gen_partial_fcn_dtr(partial_fcns);

//			Destroy the parameters, if any need to be destroyed
	ret += "\tthis->destroy_params_"+this->generate_functor_name()+"();\n";

	ret += "};\n\n";


//-------------------
//			Parameter manipulation routines
	ret += generate_load_param_block(this->generate_functor_name(),
									this->param_tbl,param_handle_table);
	ret += generate_delete_param_block(this->generate_functor_name(),
									this->param_tbl,param_handle_table);

//-------------------
//			Register new parameter block

	ret += "int set_param_block(gs_int32_t sz, void* value){\n";
	  ret += "\tthis->destroy_params_"+this->generate_functor_name()+"();\n";
	  ret += "\treturn this->load_params_"+this->generate_functor_name()+
				"(sz, value);\n";
	ret += "};\n\n";

// -----------------------------------
//			group-by pattern support

	ret +=
"int n_groupby_patterns(){\n"
"	return "+int_to_string(gb_tbl.gb_patterns.size())+";\n"
"}\n"
"bool *get_pattern(int p){\n"
"	return "+this->generate_functor_name()+"_gb_patterns[p];\n"
"}\n\n"
;




//-------------------
//		the create_group method.
//		This method creates a group in a buffer passed in
//		(to allow for creation on the stack).
//		There are also a couple of side effects:
//		1) evaluate the WHERE clause (and therefore, unpack all partial fcns)
//		2) determine if a temporal flush is required.

	ret += this->generate_functor_name()+"_groupdef *create_group(host_tuple &tup0, gs_sp_t buffer){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

	if(partial_fcns.size()>0){		// partial fcn access failure
	  ret += "\tgs_retval_t retval = 0;\n";
	  ret += "\n";
	}
//		return value
	ret += "\t"+generate_functor_name()+"_groupdef *gbval = ("+generate_functor_name()+
			"_groupdef *) buffer;\n";

//		Start by cleaning up partial function results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";
	set<int> w_pfcns;	// partial fcns in where clause
	for(w=0;w<where.size();++w)
		collect_partial_fcns_pr(where[w]->pr, w_pfcns);

	set<int> ag_gb_pfcns;	// partial fcns in gbdefs, aggr se's
	for(g=0;g<gb_tbl.size();g++){
		collect_partial_fcns(gb_tbl.get_def(g), ag_gb_pfcns);
	}
	for(a=0;a<aggr_tbl.size();a++){
		collect_partial_fcns(aggr_tbl.get_aggr_se(a), ag_gb_pfcns);
	}
	ret += gen_partial_fcn_dtr(partial_fcns,w_pfcns);
	ret += gen_partial_fcn_dtr(partial_fcns,ag_gb_pfcns);
//	ret += gen_partial_fcn_dtr(partial_fcns);


	ret += gen_temp_tuple_check(this->node_name, 0);
	col_id_set found_cids;	// colrefs unpacked thus far.
	ret += gen_unpack_temp_vars(schema, found_cids, select_list, segen_gb_tbl, needs_xform);


//			Save temporal group-by variables


	ret.append("\n\t//\t\tCompute temporal groupby attributes\n\n");

	  for(g=0;g<gb_tbl.size();g++){

			data_type *gdt = gb_tbl.get_data_type(g);

			if(gdt->is_temporal()){
				sprintf(tmpstr,"\tgbval->gb_var%d = %s;\n",
					g, generate_se_code(gb_tbl.get_def(g),schema).c_str() );
				ret.append(tmpstr);
			}
		}
		ret.append("\n");



//			Compare the temporal GB vars with the stored ones,
//			set flush indicator and update stored GB vars if there is any change.

ret += "// hfta_disorder = "+int_to_string(hfta_disorder)+"\n";
	if(hfta_disorder < 2){
		if(uses_temporal_flush){
			ret+= "\tif( !( (";
			bool first_one = true;
			for(g=0;g<gb_tbl.size();g++){
				data_type *gdt = gb_tbl.get_data_type(g);

				if(gdt->is_temporal()){
			  	sprintf(tmpstr,"last_gb%d",g);   string lhs_op = tmpstr;
			  	sprintf(tmpstr,"gbval->gb_var%d",g);   string rhs_op = tmpstr;
			  	if(first_one){first_one = false;} else {ret += ") && (";}
			  	ret += generate_equality_test(lhs_op, rhs_op, gdt);
				}
			}
			ret += ") ) ){\n";
			for(g=0;g<gb_tbl.size();g++){
		  	data_type *gdt = gb_tbl.get_data_type(g);
		  	if(gdt->is_temporal()){
			  	if(gdt->is_buffer_type()){
					sprintf(tmpstr,"\t\t%s(&(gbval->gb_var%d),&last_gb%d);\n",gdt->get_hfta_buffer_replace().c_str(),g,g);
			  	}else{
					sprintf(tmpstr,"\t\tlast_flushed_gb%d = last_gb%d;\n",g,g);
					ret += tmpstr;
					sprintf(tmpstr,"\t\tlast_gb%d = gbval->gb_var%d;\n",g,g);
			  	}
			  	ret += tmpstr;
				}
			}
			ret += "\t\tneeds_temporal_flush=true;\n";
			ret += "\t\t}else{\n"
				"\t\t\tneeds_temporal_flush=false;\n"
				"\t\t}\n";
		}
	}else{
		ret+= "\tif(temp_tuple_received && !( (";
		bool first_one = true;
		for(g=0;g<gb_tbl.size();g++){
			data_type *gdt = gb_tbl.get_data_type(g);

			if(gdt->is_temporal()){
			  	sprintf(tmpstr,"last_gb%d",g);   string lhs_op = tmpstr;
		  		sprintf(tmpstr,"gbval->gb_var%d",g);   string rhs_op = tmpstr;
		  		if(first_one){first_one = false;} else {ret += ") && (";}
		  		ret += generate_equality_test(lhs_op, rhs_op, gdt);
				break;
			}
		}
		ret += ") ) ){\n";
		int temporal_g = 0;
		for(g=0;g<gb_tbl.size();g++){
	  		data_type *gdt = gb_tbl.get_data_type(g);
	  		if(gdt->is_temporal()){
				temporal_g = g;
		  		if(gdt->is_buffer_type()){
					sprintf(tmpstr,"\t\t%s(&(gbval->gb_var%d),&last_gb%d);\n",gdt->get_hfta_buffer_replace().c_str(),g,g);
		  		}else{
					sprintf(tmpstr,"\t\tlast_flushed_gb%d = last_gb%d;\n",g,g);
					ret += tmpstr;
					sprintf(tmpstr,"\t\tlast_gb%d = gbval->gb_var%d;\n",g,g);
		  		}
		  		ret += tmpstr;
				break;
			}
		}
		data_type *tgdt = gb_tbl.get_data_type(temporal_g);
		literal_t gl(tgdt->type_indicator());
		ret += "\t\tif(last_flushed_gb"+int_to_string(temporal_g)+">"+gl.to_hfta_C_code("")+")\n";
		ret += "\t\t\tneeds_temporal_flush=true;\n";
		ret += "\t\t}else{\n"
			"\t\t\tneeds_temporal_flush=false;\n"
			"\t\t}\n";
	}


//		For temporal status tuple we don't need to do anything else
	ret += "\tif (temp_tuple_received) return NULL;\n\n";

	for(w=0;w<where.size();++w){
		sprintf(tmpstr,"//\t\tPredicate clause %d.\n",w);
		ret += tmpstr;
//			Find the set of	variables accessed in this CNF elem,
//			but in no previous element.
		col_id_set new_cids;
		get_new_pred_cids(where[w]->pr, found_cids, new_cids, segen_gb_tbl);

//			Unpack these values.
		ret += gen_unpack_cids(schema, new_cids, "NULL", needs_xform);
//			Find partial fcns ref'd in this cnf element
		set<int> pfcn_refs;
		collect_partial_fcns_pr(where[w]->pr, pfcn_refs);
		ret += gen_unpack_partial_fcn(schema, partial_fcns, pfcn_refs,"NULL");

		ret += "\tif( !("+generate_predicate_code(where[w]->pr,schema)+
				+") ) return(NULL);\n";
	}

//		The partial functions ref'd in the group-by var and aggregate
//		definitions must also be evaluated.  If one returns false,
//		then implicitly the predicate is false.
	set<int>::iterator pfsi;

	if(ag_gb_pfcns.size() > 0)
		ret += "//\t\tUnpack remaining partial fcns.\n";
	ret += gen_full_unpack_partial_fcn(schema, partial_fcns, ag_gb_pfcns,
										found_cids, segen_gb_tbl, "NULL", needs_xform);

//			Unpack the group-by variables

	  for(g=0;g<gb_tbl.size();g++){
		data_type *gdt = gb_tbl.get_data_type(g);

		if(!gdt->is_temporal()){
//			Find the new fields ref'd by this GBvar def.
			col_id_set new_cids;
			get_new_se_cids(gb_tbl.get_def(g), found_cids, new_cids, segen_gb_tbl);
//			Unpack these values.
			ret += gen_unpack_cids(schema, new_cids, "NULL", needs_xform);

			sprintf(tmpstr,"\tgbval->gb_var%d = %s;\n",
				g, generate_se_code(gb_tbl.get_def(g),schema).c_str() );
/*
//				There seems to be no difference between the two
//				branches of the IF statement.
		data_type *gdt = gb_tbl.get_data_type(g);
		  if(gdt->is_buffer_type()){
//				Create temporary copy.
			sprintf(tmpstr,"\tgbval->gb_var%d = %s;\n",
				g, generate_se_code(gb_tbl.get_def(g),schema).c_str() );
		  }else{
			scalarexp_t *gse = gb_tbl.get_def(g);
			sprintf(tmpstr,"\tgbval->gb_var%d = %s;\n",
					g,generate_se_code(gse,schema).c_str());
		  }
*/

		  	ret.append(tmpstr);
		}
	  }
	  ret.append("\n");

	ret+= "\treturn gbval;\n";
	ret += "};\n\n\n";

//--------------------------------------------------------
//			Create and initialize an aggregate object

	ret += this->generate_functor_name()+"_aggrdef *create_aggregate(host_tuple &tup0, gs_sp_t buffer){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

//		return value
	ret += "\t"+generate_functor_name()+"_aggrdef *aggval = ("+generate_functor_name()+
			"_aggrdef *)buffer;\n";

	for(a=0;a<aggr_tbl.size();a++){
		if(aggr_tbl.is_builtin(a)){
//			Create temporaries for buffer return values
		  data_type *adt = aggr_tbl.get_data_type(a);
		  if(adt->is_buffer_type()){
			sprintf(tmpstr,"aggr_tmp_%d", a);
			ret+=adt->make_host_cvar(tmpstr)+";\n";
		  }
		}
	}

//		Unpack all remaining attributes
	ret += gen_remaining_colrefs(schema, cid_set, found_cids, "NULL", needs_xform);
	for(a=0;a<aggr_tbl.size();a++){
	  sprintf(tmpstr,"aggval->aggr_var%d",a);
	  string assignto_var = tmpstr;
	  ret += "\t"+generate_aggr_init(assignto_var,&aggr_tbl,a, schema);
	}

	ret += "\treturn aggval;\n";
	ret += "};\n\n";

//--------------------------------------------------------
//			update an aggregate object

	ret += "void update_aggregate(host_tuple &tup0, "
		+generate_functor_name()+"_groupdef *gbval, "+
		generate_functor_name()+"_aggrdef *aggval){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

//			use of temporaries depends on the aggregate,
//			generate them in generate_aggr_update


//		Unpack all remaining attributes
	ret += gen_remaining_colrefs(schema, cid_set, found_cids, "", needs_xform);
	for(a=0;a<aggr_tbl.size();a++){
	  sprintf(tmpstr,"aggval->aggr_var%d",a);
	  string varname = tmpstr;
	  ret.append(generate_aggr_update(varname,&aggr_tbl,a, schema));
	}

	ret += "\treturn;\n";
	ret += "};\n";

//---------------------------------------------------
//			Flush test

	ret += "\tbool flush_needed(){\n";
	if(uses_temporal_flush){
		ret += "\t\treturn needs_temporal_flush;\n";
	}else{
		ret += "\t\treturn false;\n";
	}
	ret += "\t};\n";

//---------------------------------------------------
//			create output tuple
//			Unpack the partial functions ref'd in the where clause,
//			select clause.  Evaluate the where clause.
//			Finally, pack the tuple.

//			I need to use special code generation here,
//			so I'll leave it in longhand.

	ret += "host_tuple create_output_tuple("
		+generate_functor_name()+"_groupdef *gbval, "+
		generate_functor_name()+"_aggrdef *aggval, bool &failed){\n";

	ret += "\thost_tuple tup;\n";
	ret += "\tfailed = false;\n";
	ret += "\tgs_retval_t retval = 0;\n";

	string gbvar = "gbval->gb_var";
	string aggvar = "aggval->";

//			Create cached temporaries for UDAF return values.
   	for(a=0;a<aggr_tbl.size();a++){
		if(! aggr_tbl.is_builtin(a)){
			int afcn_id = aggr_tbl.get_fcn_id(a);
			data_type *adt = Ext_fcns->get_fcn_dt(afcn_id);
			sprintf(tmpstr,"udaf_ret_%d", a);
			ret+="\t"+adt->make_host_cvar(tmpstr)+";\n";
		}
	}


//			First, get the return values from the UDAFS
   	for(a=0;a<aggr_tbl.size();a++){
		if(! aggr_tbl.is_builtin(a)){
			ret += "\t"+aggr_tbl.get_op(a)+"_HFTA_AGGR_OUTPUT_(&(udaf_ret_"+int_to_string(a)+"),";
			if(aggr_tbl.get_storage_type(a)->get_type() != fstring_t) ret+="&";
			ret+="("+aggvar+"aggr_var"+int_to_string(a)+"));\n";
		}
	}

	set<int> hv_sl_pfcns;
	for(w=0;w<having.size();w++){
		collect_partial_fcns_pr(having[w]->pr, hv_sl_pfcns);
	}
	for(s=0;s<select_list.size();s++){
		collect_partial_fcns(select_list[s]->se, hv_sl_pfcns);
	}

//		clean up the partial fcn results from any previous execution
	ret += gen_partial_fcn_dtr(partial_fcns,hv_sl_pfcns);

//		Unpack them now
	for(pfsi=hv_sl_pfcns.begin();pfsi!=hv_sl_pfcns.end();++pfsi){
		ret += unpack_partial_fcn_fm_aggr(partial_fcns[(*pfsi)], (*pfsi), gbvar, aggvar, schema);
		ret += "\tif(retval){ failed = true; return(tup);}\n";
	}

//		Evalaute the HAVING clause
//		TODO: this seems to have a ++ operator rather than a + operator.
	for(w=0;w<having.size();++w){
		ret += "\tif( !("+generate_predicate_code_fm_aggr(having[w]->pr,gbvar, aggvar, schema) +") ) { failed = true; return(tup);}\n";
	}

//          Now, compute the size of the tuple.

//          Unpack any BUFFER type selections into temporaries
//          so that I can compute their size and not have
//          to recompute their value during tuple packing.
//          I can use regular assignment here because
//          these temporaries are non-persistent.
//			TODO: should I be using the selvar generation routine?

	ret += "//\t\tCompute the size of the tuple.\n";
	ret += "//\t\t\tNote: buffer storage packed at the end of the tuple.\n";
      for(s=0;s<select_list.size();s++){
		scalarexp_t *se = select_list[s]->se;
        data_type *sdt = se->get_data_type();
        if(sdt->is_buffer_type() &&
			 !( (se->get_operator_type() == SE_COLREF) ||
				(se->get_operator_type() == SE_AGGR_STAR) ||
				(se->get_operator_type() == SE_AGGR_SE) ||
			   (se->get_operator_type() == SE_FUNC && se->is_partial()) ||
			   (se->get_operator_type() == SE_FUNC && se->get_aggr_ref() >= 0))
		){
            sprintf(tmpstr,"selvar_%d",s);
			ret+="\t"+sdt->make_host_cvar(tmpstr)+" = ";
			ret += generate_se_code_fm_aggr(se,gbvar, aggvar, schema) +";\n";
        }
      }

//      The size of the tuple is the size of the tuple struct plus the
//      size of the buffers to be copied in.

      ret+="\ttup.tuple_size = sizeof(" + generate_tuple_name( this->get_node_name()) +") + sizeof(gs_uint8_t)";
      for(s=0;s<select_list.size();s++){
//		if(s>0) ret += "+";
		scalarexp_t *se = select_list[s]->se;
        data_type *sdt = select_list[s]->se->get_data_type();
        if(sdt->is_buffer_type()){
		  if(!( (se->get_operator_type() == SE_COLREF) ||
				(se->get_operator_type() == SE_AGGR_STAR) ||
				(se->get_operator_type() == SE_AGGR_SE) ||
			   (se->get_operator_type() == SE_FUNC && se->is_partial()) ||
			   (se->get_operator_type() == SE_FUNC && se->get_aggr_ref() >= 0))
		  ){
            sprintf(tmpstr," + %s(&selvar_%d)", sdt->get_hfta_buffer_size().c_str(),s);
            ret.append(tmpstr);
		  }else{
            sprintf(tmpstr," + %s(&%s)", sdt->get_hfta_buffer_size().c_str(),generate_se_code_fm_aggr(se,gbvar, aggvar, schema).c_str());
            ret.append(tmpstr);
		  }
        }
      }
      ret.append(";\n");

//		Allocate tuple data block.
	ret += "//\t\tCreate the tuple block.\n";
	  ret += "\ttup.data = malloc(tup.tuple_size);\n";
	  ret += "\ttup.heap_resident = true;\n";

//		Mark tuple as regular
	  ret += "\t*((gs_sp_t )tup.data + sizeof(" + generate_tuple_name( this->get_node_name()) +")) = REGULAR_TUPLE;\n";

//	  ret += "\ttup.channel = 0;\n";
	  ret += "\t"+generate_tuple_name( this->get_node_name())+" *tuple = ("+
				generate_tuple_name( this->get_node_name())+" *)(tup.data);\n";

//		Start packing.
//			(Here, offsets are hard-wired.  is this a problem?)

	ret += "//\t\tPack the fields into the tuple.\n";
	  ret += "\tint tuple_pos = sizeof("+generate_tuple_name( this->get_node_name()) +") + sizeof(gs_uint8_t);\n";
      for(s=0;s<select_list.size();s++){
		scalarexp_t *se = select_list[s]->se;
        data_type *sdt = se->get_data_type();
        if(sdt->is_buffer_type()){
		  if(!( (se->get_operator_type() == SE_COLREF) ||
				(se->get_operator_type() == SE_AGGR_STAR) ||
				(se->get_operator_type() == SE_AGGR_SE) ||
			   (se->get_operator_type() == SE_FUNC && se->is_partial()) ||
			   (se->get_operator_type() == SE_FUNC && se->get_aggr_ref() >= 0))
		  ){
            sprintf(tmpstr,"\t%s(&(tuple->tuple_var%d), &selvar_%d,  ((gs_sp_t)tuple)+tuple_pos, tuple_pos);\n", sdt->get_hfta_buffer_tuple_copy().c_str(),s, s);
            ret.append(tmpstr);
            sprintf(tmpstr,"\ttuple_pos += %s(&selvar_%d);\n", sdt->get_hfta_buffer_size().c_str(), s);
            ret.append(tmpstr);
		  }else{
            sprintf(tmpstr,"\t%s(&(tuple->tuple_var%d), &%s,  ((gs_sp_t)tuple)+tuple_pos, tuple_pos);\n", sdt->get_hfta_buffer_tuple_copy().c_str(),s, generate_se_code_fm_aggr(se,gbvar, aggvar, schema).c_str());
            ret.append(tmpstr);
            sprintf(tmpstr,"\ttuple_pos += %s(&%s);\n", sdt->get_hfta_buffer_size().c_str(), generate_se_code_fm_aggr(se,gbvar, aggvar, schema).c_str());
            ret.append(tmpstr);
		  }
        }else{
            sprintf(tmpstr,"\ttuple->tuple_var%d = ",s);
            ret.append(tmpstr);
            ret.append(generate_se_code_fm_aggr(se,gbvar, aggvar, schema) );
            ret.append(";\n");
        }
      }

//			Destroy string temporaries
	  ret += gen_buffer_selvars_dtr(select_list);
//			Destroy string return vals of UDAFs
   	for(a=0;a<aggr_tbl.size();a++){
		if(! aggr_tbl.is_builtin(a)){
			int afcn_id = aggr_tbl.get_fcn_id(a);
			data_type *adt = Ext_fcns->get_fcn_dt(afcn_id);
			if(adt->is_buffer_type()){
				sprintf(tmpstr,"\t%s(&udaf_ret_%d);\n",
			  	adt->get_hfta_buffer_destroy().c_str(), a );
				ret += tmpstr;
			}
		}
	}


	  ret += "\treturn tup;\n";
	  ret += "};\n";


//-------------------------------------------------------------------
//		Temporal update functions

	ret+="bool temp_status_received(){return temp_tuple_received;};\n\n";

	for(g=0;g<gb_tbl.size();g++){
  		data_type *gdt = gb_tbl.get_data_type(g);
  		if(gdt->is_temporal()){
			tgdt = gdt;
			break;
		}
	}
	ret += tgdt->get_host_cvar_type()+" get_last_flushed_gb(){\n";
	ret+="\treturn last_flushed_gb"+int_to_string(g)+";\n";
	ret+="}\n";
	ret += tgdt->get_host_cvar_type()+" get_last_gb(){\n";
	ret+="\treturn last_gb"+int_to_string(g)+";\n";
	ret+="}\n";




//		create a temp status tuple
	ret += "int create_temp_status_tuple(host_tuple& result, bool flush_finished) {\n\n";

	ret += gen_init_temp_status_tuple(this->get_node_name());

//		Start packing.
//			(Here, offsets are hard-wired.  is this a problem?)

	ret += "//\t\tPack the fields into the tuple.\n";
	for(s=0;s<select_list.size();s++){
		data_type *sdt = select_list[s]->se->get_data_type();
		if(sdt->is_temporal()){
			sprintf(tmpstr,"\ttuple->tuple_var%d = ",s);
	  		ret += tmpstr;

			sprintf(tmpstr,"(flush_finished) ? %s : %s ", generate_se_code(select_list[s]->se,schema).c_str(), generate_se_code_fm_aggr(select_list[s]->se,"last_flushed_gb", "", schema).c_str());
			ret += tmpstr;
	  		ret += ";\n";
		}
	}


	ret += "\treturn 0;\n";
	ret += "};};\n\n\n";


//----------------------------------------------------------
//			The hash function

	ret += "struct "+generate_functor_name()+"_hash_func{\n";
	ret += "\tgs_uint32_t operator()(const "+generate_functor_name()+
				"_groupdef *grp) const{\n";
	ret += "\t\treturn( (";
	for(g=0;g<gb_tbl.size();g++){
		if(g>0) ret += "^";
		data_type *gdt = gb_tbl.get_data_type(g);
		if(gdt->use_hashfunc()){
			if(gdt->is_buffer_type())
				sprintf(tmpstr,"(%s*%s(&(grp->gb_var%d)))",hash_nums[g%NRANDS].c_str(),gdt->get_hfta_hashfunc().c_str(),g);
			else
				sprintf(tmpstr,"(%s*%s(grp->gb_var%d))",hash_nums[g%NRANDS].c_str(),gdt->get_hfta_hashfunc().c_str(),g);
		}else{
			sprintf(tmpstr,"(%s*grp->gb_var%d)",hash_nums[g%NRANDS].c_str(),g);
		}
		ret += tmpstr;
	}
	ret += ") >> 32);\n";
	ret += "\t}\n";
	ret += "};\n\n";

//----------------------------------------------------------
//			The comparison function

	ret += "struct "+generate_functor_name()+"_equal_func{\n";
	ret += "\tbool operator()(const "+generate_functor_name()+"_groupdef *grp1, "+
			generate_functor_name()+"_groupdef *grp2) const{\n";
	ret += "\t\treturn( (";

	for(g=0;g<gb_tbl.size();g++){
		if(g>0) ret += ") && (";
		data_type *gdt = gb_tbl.get_data_type(g);
		if(gdt->complex_comparison(gdt)){
	  	if(gdt->is_buffer_type())
			sprintf(tmpstr,"(%s(&(grp1->gb_var%d), &(grp2->gb_var%d))==0)",
				gdt->get_hfta_comparison_fcn(gdt).c_str(),g,g);
	  	else
			sprintf(tmpstr,"(%s((grp1->gb_var%d), (grp2->gb_var%d))==0)",
				gdt->get_hfta_comparison_fcn(gdt).c_str(),g,g);
		}else{
			sprintf(tmpstr,"grp1->gb_var%d == grp2->gb_var%d",g,g);
		}
		ret += tmpstr;
	}
	ret += ") );\n";
	ret += "\t}\n";
	ret += "};\n\n";


	return(ret);
}

string sgah_qpn::generate_operator(int i, string params){

	if(hfta_disorder < 2){
		return(
			"	groupby_operator<" +
			generate_functor_name()+","+
			generate_functor_name() + "_groupdef, " +
			generate_functor_name() + "_aggrdef, " +
			generate_functor_name()+"_hash_func, "+
			generate_functor_name()+"_equal_func "
			"> *op"+int_to_string(i)+" = new groupby_operator<"+
			generate_functor_name()+","+
			generate_functor_name() + "_groupdef, " +
			generate_functor_name() + "_aggrdef, " +
			generate_functor_name()+"_hash_func, "+
			generate_functor_name()+"_equal_func "
			">("+params+", \"" + get_node_name() +
"\");\n"
		);
	}
	data_type *tgdt;
	for(int g=0;g<gb_tbl.size();g++){
  		data_type *gdt = gb_tbl.get_data_type(g);
  		if(gdt->is_temporal()){
			tgdt = gdt;
			break;
		}
	}

	return(
			"	groupby_operator_oop<" +
			generate_functor_name()+","+
			generate_functor_name() + "_groupdef, " +
			generate_functor_name() + "_aggrdef, " +
			generate_functor_name()+"_hash_func, "+
			generate_functor_name()+"_equal_func, " +
            tgdt->get_host_cvar_type() +
			"> *op"+int_to_string(i)+" = new groupby_operator_oop<"+
			generate_functor_name()+","+
			generate_functor_name() + "_groupdef, " +
			generate_functor_name() + "_aggrdef, " +
			generate_functor_name()+"_hash_func, "+
			generate_functor_name()+"_equal_func, " +
            tgdt->get_host_cvar_type() +
			">("+params+", \"" + get_node_name() +
"\");\n"
		);
}


////////////////////////////////////////////////
///		MERGE operator
///		MRG functor
////////////////////////////////////////////

string mrg_qpn::generate_functor_name(){
	return("mrg_functor_" + normalize_name(this->get_node_name()));
}

string mrg_qpn::generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, vector<bool> &needs_xform){
	int tblref;


//		Sanity check
	if(fm.size() != mvars.size()){
		fprintf(stderr,"INTERNAL ERROR in mrg_qpn::generate_functor fm.size=%lu, mvars.size=%lu\n",fm.size(),mvars.size());
		exit(1);
	}
	if(fm.size() != 2){
		fprintf(stderr,"INTERNAL ERROR in mrg_qpn::generate_functor fm.size=mvars.size=%lu\n",fm.size());
		exit(1);
	}


//			Initialize generate utility globals
	segen_gb_tbl = NULL;

	string ret = "class " + this->generate_functor_name() + "{\n";

//		Private variable:
//		1) Vars for unpacked attrs.
//		2) offsets ofthe unpakced attrs
//		3) last_posted_timestamp

	data_type dta(
		schema->get_type_name(mvars[0]->get_schema_ref(), mvars[0]->get_field()),
		schema->get_modifier_list(mvars[0]->get_schema_ref(), mvars[0]->get_field())
	);
	data_type dtb(
		schema->get_type_name(mvars[1]->get_schema_ref(), mvars[1]->get_field()),
		schema->get_modifier_list(mvars[1]->get_schema_ref(), mvars[1]->get_field())
	);

	ret += "private:\n";

	// var to save the schema handle
	ret += "\tint schema_handle0;\n";

	// generate the declaration of all the variables related to
	// temp tuples generation
	ret += gen_decl_temp_vars();

//			unpacked attribute storage, offsets
	ret += "//\t\tstorage and offsets of accessed fields.\n";
	ret += "\tint tuple_metadata_offset0, tuple_metadata_offset1;\n";
	tblref = 0;
	sprintf(tmpstr,"unpack_var_%s_%d", mvars[0]->get_field().c_str(), tblref);
	ret+="\t"+dta.make_host_cvar(tmpstr)+";\n";
	sprintf(tmpstr,"\tgs_int32_t unpack_offset_%s_%d;\n", mvars[0]->get_field().c_str(), tblref);
	ret.append(tmpstr);
	tblref = 1;
	sprintf(tmpstr,"unpack_var_%s_%d", mvars[1]->get_field().c_str(), tblref);
	ret+="\t"+dtb.make_host_cvar(tmpstr)+";\n";
	sprintf(tmpstr,"\tgs_int32_t unpack_offset_%s_%d;\n", mvars[1]->get_field().c_str(), tblref);
	ret.append(tmpstr);

	ret += "//\t\tRemember the last posted timestamp.\n";
	ret+="\t"+dta.make_host_cvar("last_posted_timestamp_0")+";\n";
	ret+="\t"+dta.make_host_cvar("last_posted_timestamp_1")+";\n";
	ret+="\t"+dta.make_host_cvar("timestamp")+";\n";
	ret+="\t"+dta.make_host_cvar("slack")+";\n";
//	ret += "\t bool first_execution_0, first_execution_1;\n";

//			variables to hold parameters.
	ret += "//\tfor query parameters\n";
	ret += generate_param_vars(param_tbl);

	ret += "public:\n";
//-------------------
//			The functor constructor
//			pass in a schema handle (e.g. for the 1st input stream),
//			use it to determine how to unpack the merge variable.
//			ASSUME that both streams have the same layout,
//			just duplicate it.

//		unpack vars
	ret += "//\t\tFunctor constructor.\n";
	ret +=  this->generate_functor_name()+"(int schema_handle0){\n";

	// var to save the schema handle
	ret += "\tthis->schema_handle0 = schema_handle0;\n";
	ret += "\ttuple_metadata_offset0=ftaschema_get_tuple_metadata_offset(schema_handle0);\n";
	ret += "\ttuple_metadata_offset1=ftaschema_get_tuple_metadata_offset(schema_handle0);\n";

	ret += "//\t\tGet offsets for unpacking fields from input tuple.\n";

   sprintf(tmpstr,"\tunpack_offset_%s_%d = ftaschema_get_field_offset_by_name(schema_handle0, \"%s\");\n", mvars[0]->get_field().c_str(), 0,mvars[0]->get_field().c_str());
   ret.append(tmpstr);
	sprintf(tmpstr,"\tunpack_offset_%s_%d = unpack_offset_%s_%d;\n",mvars[1]->get_field().c_str(), 1,mvars[0]->get_field().c_str(), 0);
	ret.append(tmpstr);
//	ret+="\tfirst_execution_0 = first_execution_1 = true;\n";
	if(slack)
		ret+="\tslack = "+generate_se_code(slack,schema)+";\n";
	else
		ret+="\tslack = 0;\n";

//		Initialize internal state
	ret += "\ttemp_tuple_received = false;\n";

	//		Init last timestamp values to minimum value for their type
	if (dta.is_increasing())
		ret+="\tlast_posted_timestamp_0 = last_posted_timestamp_1 = " + dta.get_min_literal() + ";\n";
	else
		ret+="\tlast_posted_timestamp_0 = last_posted_timestamp_1 = " + dta.get_max_literal() + ";\n";


	ret += "};\n\n";

	ret += "//\t\tFunctor destructor.\n";
	ret +=  "~"+this->generate_functor_name()+"(){\n";

//			Destroy the parameters, if any need to be destroyed
	ret += "\tthis->destroy_params_"+this->generate_functor_name()+"();\n";

	ret+="};\n\n";


// 			no pass-by-handle params.
	vector<handle_param_tbl_entry *> param_handle_table;

//			Parameter manipulation routines
	ret += generate_load_param_block(this->generate_functor_name(),
									this->param_tbl,param_handle_table);
	ret += generate_delete_param_block(this->generate_functor_name(),
									this->param_tbl,param_handle_table);

//			Register new parameter block

	ret += "int set_param_block(gs_int32_t sz, void* value){\n";
	  ret += "\tthis->destroy_params_"+this->generate_functor_name()+"();\n";
	  ret += "\treturn this->load_params_"+this->generate_functor_name()+
				"(sz, value);\n";
	ret += "};\n\n";


//	-----------------------------------
//			Compare method

	string unpack_fcna;
	if(needs_xform[0]) unpack_fcna = dta.get_hfta_unpack_fcn();
	else unpack_fcna = dta.get_hfta_unpack_fcn_noxf();
	string unpack_fcnb;
	if(needs_xform[1]) unpack_fcnb = dtb.get_hfta_unpack_fcn();
	else unpack_fcnb = dtb.get_hfta_unpack_fcn_noxf();

/*
	ret+="\tint compare(const host_tuple& tup1, const host_tuple& tup2) const{ \n";
	ret+="\t"+dta.make_host_cvar("timestamp1")+";\n";
	ret+="\t"+dta.make_host_cvar("timestamp2")+";\n";
	ret+="\tgs_int32_t problem;\n";
	ret+="\tif (tup1.channel == 0)  {\n";
	sprintf(tmpstr,"\t\ttimestamp1 = %s(tup1.data, tup1.tuple_size, unpack_offset_%s_%d, &problem);\n",  unpack_fcna.c_str(), mvars[0]->get_field().c_str(), 0);
	ret += tmpstr;
	sprintf(tmpstr,"\t\ttimestamp2 = %s(tup2.data, tup2.tuple_size, unpack_offset_%s_%d, &problem);\n",  unpack_fcnb.c_str(), mvars[1]->get_field().c_str(), 1);
	ret += tmpstr;
	ret+="\t}else{\n";
	sprintf(tmpstr,"\t\ttimestamp1 = %s(tup1.data, tup1.tuple_size, unpack_offset_%s_%d, &problem);\n",  unpack_fcna.c_str(), mvars[0]->get_field().c_str(), 1);
	ret += tmpstr;
	sprintf(tmpstr,"\t\ttimestamp2 = %s(tup2.data, tup2.tuple_size, unpack_offset_%s_%d, &problem);\n",  unpack_fcnb.c_str(), mvars[1]->get_field().c_str(), 0);
	ret += tmpstr;
	ret+="\t}\n";
	ret+=
"        if (timestamp1 > timestamp2+slack)\n"
"            return 1;\n"
"        else if (timestamp1 < timestamp2)\n"
"            return -1;\n"
"        else\n"
"            return 0;\n"
"\n"
"    }\n\n";
*/

ret +=
"	void get_timestamp(const host_tuple& tup0){\n"
"		gs_int32_t problem;\n"
;
	sprintf(tmpstr,"\t\ttimestamp = %s_nocheck(tup0.data, unpack_offset_%s_%d);\n",  unpack_fcna.c_str(), mvars[0]->get_field().c_str(), 0);
	ret += tmpstr;
ret +=
"	}\n"
"\n"
;



//			Compare to temp status.
	ret+=
"	int compare_with_temp_status(int channel)   {\n"
"	// check if tuple is temp status tuple\n"
"\n"
"	if (channel == 0)  {\n"
//"	if(first_execution_0) return 1;\n"
"        if (timestamp == last_posted_timestamp_0)\n"
"            return 0;\n"
"        else if (timestamp < last_posted_timestamp_0)\n"
"            return -1;\n"
"        else\n"
"            return 1;\n"
"	}\n"
//"	if(first_execution_1) return 1;\n"
"        if (timestamp == last_posted_timestamp_1)\n"
"            return 0;\n"
"        else if (timestamp < last_posted_timestamp_1)\n"
"            return -1;\n"
"        else\n"
"            return 1;\n"
"\n"
"    }\n"
;

	ret +=
"	int compare_stored_with_temp_status(const host_tuple& tup0, int channel)/* const*/   {\n"
;
	ret+="\t"+dta.make_host_cvar("l_timestamp")+";\n";
	ret+="\tgs_int32_t problem;\n";

	sprintf(tmpstr,"\t\tl_timestamp = %s_nocheck(tup0.data, unpack_offset_%s_%d);\n",  unpack_fcna.c_str(), mvars[0]->get_field().c_str(), 0);
	ret += tmpstr;
	ret+="\tif (channel == 0)  {\n";
//		ret+="\tif(first_execution_0) return 1;\n";
	ret+=
"        if (l_timestamp == last_posted_timestamp_0)\n"
"            return 0;\n"
"        else if (l_timestamp < last_posted_timestamp_0)\n"
"            return -1;\n"
"        else\n"
"            return 1;\n"
"	}\n";
//		ret+="\tif(first_execution_1) return 1;\n";
	ret+=
"        if (l_timestamp == last_posted_timestamp_1)\n"
"            return 0;\n"
"        else if (l_timestamp < last_posted_timestamp_1)\n"
"            return -1;\n"
"        else\n"
"            return 1;\n"
"\n"
"    }\n\n";


//			update temp status.
	ret+=
"	int update_temp_status(const host_tuple& tup) {\n"
"		if (tup.channel == 0)  {\n"
"			last_posted_timestamp_0=timestamp;\n"
//"			first_execution_0 = false;\n"
"		}else{\n"
"			last_posted_timestamp_1=timestamp;\n"
//"			first_execution_1 = false;\n"
"		}\n"
"		return 0\n"
"   }\n"
;
	ret+=
"	int update_stored_temp_status(const host_tuple& tup, int channel) {\n"
;
	ret+="\t"+dta.make_host_cvar("l_timestamp")+";\n";
	ret+="\tgs_int32_t problem;\n";
	sprintf(tmpstr,"\t\tl_timestamp = %s_nocheck(tup.data, unpack_offset_%s_%d);\n",  unpack_fcna.c_str(), mvars[0]->get_field().c_str(), 0);
	ret += tmpstr;
ret+=
"		if (tup.channel == 0)  {\n"
"			last_posted_timestamp_0=l_timestamp;\n"
//"			first_execution_0 = false;\n"
"		}else{\n"
"			last_posted_timestamp_1=l_timestamp;\n"
//"			first_execution_1 = false;\n"
"		}\n"
"		return 0;\n"
"   }\n"
;
/*
	ret+="\t"+dta.make_host_cvar("timestamp")+";\n";
	ret+="\tgs_int32_t problem;\n";
	ret+="\tif (tup.channel == 0)  {\n";
	sprintf(tmpstr,"\t\ttimestamp = %s(tup.data, tup.tuple_size, unpack_offset_%s_%d, &problem);\n",  unpack_fcna.c_str(), mvars[0]->get_field().c_str(), 0);
	ret += tmpstr;
	ret+="\t}else{\n";
	sprintf(tmpstr,"\t\ttimestamp = %s(tup.data, tup.tuple_size, unpack_offset_%s_%d, &problem);\n",  unpack_fcnb.c_str(), mvars[1]->get_field().c_str(), 1);
	ret += tmpstr;
	ret+="\t}\n";
	ret+="\tif (tup.channel == 0)  {\n";
	ret+="\tlast_posted_timestamp_0=timestamp;\n";
	ret +="\tfirst_execution_0 = false;\n";
	ret+="\t}else{\n";
	ret+="\tlast_posted_timestamp_1=timestamp;\n";
	ret +="\tfirst_execution_1 = false;\n";
	ret+="\t}\n";
	ret+=
"    }\n\n";
*/


//			update temp status modulo slack.
	ret+="\tint update_temp_status_by_slack(const host_tuple& tup, int channel) {\n";
    if(slack){
	ret+="\t"+dta.make_host_cvar("timestamp")+";\n";
	ret+="\tgs_int32_t problem;\n";
	ret+="\tif (tup.channel == 0)  {\n";
	sprintf(tmpstr,"\t\ttimestamp = %s_nocheck(tup.data, unpack_offset_%s_%d);\n",  unpack_fcna.c_str(), mvars[0]->get_field().c_str(), 0);
	ret += tmpstr;
	ret+="\t}else{\n";
	sprintf(tmpstr,"\t\ttimestamp = %s_nocheck(tup.data, unpack_offset_%s_%d);\n",  unpack_fcnb.c_str(), mvars[1]->get_field().c_str(), 1);
	ret += tmpstr;
	ret+="\t}\n";
ret +=
"	if (channel == 0)  {\n"
"		if(first_execution_0){\n"
"			last_posted_timestamp_0=timestamp - slack;\n"
"			first_execution_0 = false;\n"
"		}else{\n"
"			if(last_posted_timestamp_0 < timestamp-slack)\n"
"				last_posted_timestamp_0 = timestamp-slack;\n"
"		}\n"
"	}else{\n"
"		if(first_execution_1){\n"
"			last_posted_timestamp_1=timestamp - slack;\n"
"			first_execution_1 = false;\n"
"		}else{\n"
"			if(last_posted_timestamp_1 < timestamp-slack)\n"
"				last_posted_timestamp_1 = timestamp-slack;\n"
"		}\n"
"	}\n"
"	return 0\n"
"    }\n\n";
	}else{
	ret +=
"	return 0;\n"
"	}\n\n";
	}


//
	ret+=
"bool temp_status_received(const host_tuple& tup0){\n"
"	return ftaschema_is_temporal_tuple_offset(tuple_metadata_offset0, tup0.data);\n"
"};\n"
;
//"bool temp_status_received(){return temp_tuple_received;};\n\n";


//		create a temp status tuple
	ret += "int create_temp_status_tuple(host_tuple& result) {\n\n";

	ret += gen_init_temp_status_tuple(this->get_node_name());

//		Start packing.
	ret += "//\t\tPack the fields into the tuple.\n";

	string fld_name = mvars[0]->get_field();
	int idx = table_layout->get_field_idx(fld_name);
	field_entry* fld = table_layout->get_field(idx);
 	data_type dt(fld->get_type());

//	if (needs_xform[0] && needs_xform[1] && dt.needs_hn_translation())
//		sprintf(tmpstr,"\ttuple->tuple_var%d = %s((last_posted_timestamp_0 < last_posted_timestamp_1) ? last_posted_timestamp_0 : last_posted_timestamp_1);\n",idx, dt.hton_translation().c_str());
//	else
		sprintf(tmpstr,"\ttuple->tuple_var%d = (last_posted_timestamp_0 < last_posted_timestamp_1 ? last_posted_timestamp_0 : last_posted_timestamp_1);\n",idx);

	ret += tmpstr;

	ret += "\treturn 0;\n";
	ret += "}\n\n";

//			Transform tuple (before output)


 ret += "void xform_tuple(host_tuple &tup){\n";
 if((needs_xform[0] && !needs_xform[1]) || (needs_xform[1] && !needs_xform[0])){
  ret += "\tstruct "+generate_tuple_name(this->get_node_name())+" *tuple = ("+
  		generate_tuple_name(this->get_node_name())+" *)(tup.data);\n";

  vector<field_entry *> flds = table_layout->get_fields();

  ret+="\tif(tup.channel == 0){\n";
  if(needs_xform[0] && !needs_xform[1]){
  	int f;
  	for(f=0;f<flds.size();f++){
  	 	ret.append("\t");
  	 	data_type dt(flds[f]->get_type());
  	 	if(dt.get_type() == v_str_t){
//  	 		sprintf(tmpstr,"\ttuple->tuple_var%d.offset = htonl(tuple->tuple_var%d.offset);\n",f,f);
//			ret += tmpstr;
//  	 		sprintf(tmpstr,"\ttuple->tuple_var%d.length = htonl(tuple->tuple_var%d.length);\n",f,f);
//			ret += tmpstr;
//  	 		sprintf(tmpstr,"\ttuple->tuple_var%d.reserved = htonl(tuple->tuple_var%d.reserved);\n",f,f);
//			ret += tmpstr;
  	 	}else{
  	 		if(dt.needs_hn_translation()){
//  	 			sprintf(tmpstr,"\ttuple->tuple_var%d = %s(tuple->tuple_var%d);\n",
//  	 				f, dt.hton_translation().c_str(), f);
//  	 			ret += tmpstr;
  	 		}
  	 	}
  	}
  }else{
	ret += "\t\treturn;\n";
  }
  ret.append("\t}\n");


  ret+="\tif(tup.channel == 1){\n";
  if(needs_xform[1] && !needs_xform[0]){
  	int f;
  	for(f=0;f<flds.size();f++){
  	 	ret.append("\t");
  	 	data_type dt(flds[f]->get_type());
  	 	if(dt.get_type() == v_str_t){
//  	 		sprintf(tmpstr,"\ttuple->tuple_var%d.offset = htonl(tuple->tuple_var%d.offset);\n",f,f);
//			ret += tmpstr;
//  	 		sprintf(tmpstr,"\ttuple->tuple_var%d.length = htonl(tuple->tuple_var%d.length);\n",f,f);
//			ret += tmpstr;
//  	 		sprintf(tmpstr,"\ttuple->tuple_var%d.reserved = htonl(tuple->tuple_var%d.reserved);\n",f,f);
//			ret += tmpstr;
  	 	}else{
  	 		if(dt.needs_hn_translation()){
//  	 			sprintf(tmpstr,"\ttuple->tuple_var%d = %s(tuple->tuple_var%d);\n",
//  	 				f, dt.hton_translation().c_str(), f);
//  	 			ret += tmpstr;
  	 		}
  	 	}
  	}
  }else{
	ret += "\t\treturn;\n";
  }
  ret.append("\t}\n");
 }

  ret.append("};\n\n");

//		print_warnings() : tell the functor if the user wants to print warnings.
  ret += "bool print_warnings(){\n";
  if(definitions.count("print_warnings") && (
		definitions["print_warnings"] == "yes" ||
		definitions["print_warnings"] == "Yes" ||
		definitions["print_warnings"] == "YES" )) {
	ret += "return true;\n";
  }else{
	ret += "return false;\n";
  }
  ret.append("};\n\n");


//		Done with methods.
	ret+="\n};\n\n";


	return(ret);
}

string mrg_qpn::generate_operator(int i, string params){

	if(disorder < 2){
		return(
			"	merge_operator<" +
			generate_functor_name()+
			"> *op"+int_to_string(i)+" = new merge_operator<"+
			generate_functor_name()+
			">("+params+",10000,\"" + get_node_name() + "\");\n"
		);
	}
	return(
			"	merge_operator_oop<" +
			generate_functor_name()+
			"> *op"+int_to_string(i)+" = new merge_operator_oop<"+
			generate_functor_name()+
			">("+params+",10000,\"" + get_node_name() + "\");\n"
	);
}


/////////////////////////////////////////////////////////
//////			JOIN_EQ_HASH functor


string join_eq_hash_qpn::generate_functor_name(){
	return("join_eq_hash_functor_" + normalize_name(this->get_node_name()));
}

string join_eq_hash_qpn::generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, vector<bool> &needs_xform){
	int p,s;
	vector<data_type *> hashkey_dt;		// data types in the hash key
	vector<data_type *> temporal_dt;	// data types in the temporal key
	map<string,scalarexp_t *> l_equiv, r_equiv;	// field equivalences
	set<int> pfcn_refs;
	col_id_set new_cids, local_cids;

//--------------------------------
//		Global init

	string plus_op = "+";

//--------------------------------
//			key definition class
	string ret = "class " + generate_functor_name() + "_keydef{\n";
	ret += "public:\n";
//			Collect attributes from hash join predicates.
//			ASSUME equality predicate.
//			Use the upwardly compatible data type
//			(infer from '+' operator if possible, else use left type)
	for(p=0;p<this->hash_eq.size();++p){
		scalarexp_t *lse = 	hash_eq[p]->pr->get_left_se();
		scalarexp_t *rse = 	hash_eq[p]->pr->get_right_se();
		data_type *hdt = new data_type(
			lse->get_data_type(), rse->get_data_type(), plus_op );
		if(hdt->get_type() == undefined_t){
			hashkey_dt.push_back(lse->get_data_type()->duplicate());
			delete hdt;
		}else{
			hashkey_dt.push_back(hdt);
		}
		sprintf(tmpstr,"hashkey_var%d",p);
		ret+="\t"+hashkey_dt[p]->make_host_cvar(tmpstr)+";\n";

//			find equivalences
//			NOTE: this code needs to be synched with the temporality
//			checking done at join_eq_hash_qpn::get_fields
		if(lse->get_operator_type()==SE_COLREF){
			l_equiv[lse->get_colref()->get_field()] = rse;
		}
		if(rse->get_operator_type()==SE_COLREF){
			r_equiv[rse->get_colref()->get_field()] = lse;
		}
	}
	ret += "\tbool touched;\n";

//		Constructors
	ret += "\t"+generate_functor_name() + "_keydef(){touched=false;};\n";
//		destructor
	ret += "\t~"+ generate_functor_name() + "_keydef(){\n";
	for(p=0;p<hashkey_dt.size();p++){
		if(hashkey_dt[p]->is_buffer_type()){
			sprintf(tmpstr,"\t\t%s(&hashkey_var%d);\n",
			  hashkey_dt[p]->get_hfta_buffer_destroy().c_str(), p );
			ret += tmpstr;
		}
	}
	ret += "\t};\n";
	ret+="\tvoid touch(){touched = true;};\n";
	ret+="\tbool is_touched(){return touched;};\n";
	ret +="};\n\n";


//--------------------------------
//		temporal equality definition class
	ret += "class " + generate_functor_name() + "_tempeqdef{\n";
	ret += "public:\n";
//			Collect attributes from hash join predicates.
//			ASSUME equality predicate.
//			Use the upwardly compatible date type
//			(infer from '+' operator if possible, else use left type)
	for(p=0;p<this->temporal_eq.size();++p){
		scalarexp_t *lse = 	temporal_eq[p]->pr->get_left_se();
		scalarexp_t *rse = 	temporal_eq[p]->pr->get_right_se();
		data_type *hdt = new data_type(
			lse->get_data_type(), rse->get_data_type(), plus_op );
		if(hdt->get_type() == undefined_t){
			temporal_dt.push_back(hash_eq[p]->pr->get_left_se()->get_data_type()->duplicate());
			delete hdt;
		}else{
			temporal_dt.push_back(hdt);
		}
		sprintf(tmpstr,"tempeq_var%d",p);
		ret+="\t"+temporal_dt[p]->make_host_cvar(tmpstr)+";\n";
//			find equivalences
		if(lse->get_operator_type()==SE_COLREF){
			l_equiv[lse->get_colref()->get_field()] = rse;
		}
		if(rse->get_operator_type()==SE_COLREF){
			r_equiv[rse->get_colref()->get_field()] = lse;
		}
	}

//		Constructors
	ret += "\t"+generate_functor_name() + "_tempeqdef(){};\n";
//		destructor
	ret += "\t~"+ generate_functor_name() + "_tempeqdef(){\n";
	for(p=0;p<temporal_dt.size();p++){
		if(temporal_dt[p]->is_buffer_type()){
			sprintf(tmpstr,"\t\t%s(&tempeq_var%d);\n",
			  temporal_dt[p]->get_hfta_buffer_destroy().c_str(), p );
			ret += tmpstr;
		}
	}
	ret += "\t};\n";
	ret +="};\n\n";


//--------------------------------
//			temporal eq, hash join functor class
	ret += "class " + this->generate_functor_name() + "{\n";

//			Find variables referenced in this query node.

	col_id_set cid_set;
	col_id_set::iterator csi;

    for(p=0;p<where.size();++p)
    	gather_pr_col_ids(where[p]->pr,cid_set,NULL);
    for(s=0;s<select_list.size();s++)
    	gather_se_col_ids(select_list[s]->se,cid_set,NULL);

//			Private variables : store the state of the functor.
//			1) variables for unpacked attributes
//			2) offsets of the upacked attributes
//			3) storage of partial functions
//			4) storage of complex literals (i.e., require a constructor)

	ret += "private:\n";

	// var to save the schema handles
	ret += "\tint schema_handle0;\n";
	ret += "\tint schema_handle1;\n";

	// generate the declaration of all the variables related to
	// temp tuples generation
	ret += gen_decl_temp_vars();
	// tuple metadata offsets
	ret += "\tint tuple_metadata_offset0, tuple_metadata_offset1;\n";

//			unpacked attribute storage, offsets
	ret += "//\t\tstorage and offsets of accessed fields.\n";
	ret += generate_access_vars(cid_set, schema);


//			Variables to store results of partial functions.
//			WARNING find_partial_functions modifies the SE
//			(it marks the partial function id).
	ret += "//\t\tParital function result storage\n";
	vector<scalarexp_t *> partial_fcns;
	vector<int> fcn_ref_cnt;
	vector<bool> is_partial_fcn;
	for(s=0;s<select_list.size();s++){
		find_partial_fcns(select_list[s]->se, &partial_fcns,NULL,NULL,  Ext_fcns);
	}
	for(p=0;p<where.size();p++){
		find_partial_fcns_pr(where[p]->pr, &partial_fcns,NULL,NULL,  Ext_fcns);
	}
	if(partial_fcns.size()>0){
	  ret += "/*\t\tVariables for storing results of partial functions. \t*/\n";
	  ret += generate_partial_fcn_vars(partial_fcns,fcn_ref_cnt,is_partial_fcn,false);
	}

//			Complex literals (i.e., they need constructors)
	ret += "//\t\tComplex literal storage.\n";
	cplx_lit_table *complex_literals = this->get_cplx_lit_tbl(Ext_fcns);
	ret += generate_complex_lit_vars(complex_literals);
//			We need the following to handle strings in outer joins.
//				NEED AN EMPTY LITERAL FOR EAcH STRUCTURED LITERAL
	ret += "\tstruct vstring EmptyString;\n";
	ret += "\tstruct hfta_ipv6_str EmptyIp6;\n";

//			Pass-by-handle parameters
	ret += "//\t\tPass-by-handle storage.\n";
	vector<handle_param_tbl_entry *> param_handle_table = this->get_handle_param_tbl(Ext_fcns);
	ret += generate_pass_by_handle_vars(param_handle_table);


//			variables to hold parameters.
	ret += "//\tfor query parameters\n";
	ret += generate_param_vars(param_tbl);


	ret += "\npublic:\n";
//-------------------
//			The functor constructor
//			pass in the schema handle.
//			1) make assignments to the unpack offset variables
//			2) initialize the complex literals

	ret += "//\t\tFunctor constructor.\n";
	ret +=  this->generate_functor_name()+"(int schema_handle0, int schema_handle1){\n";

	ret += "\t\tthis->schema_handle0 = schema_handle0;\n";
	ret += "\t\tthis->schema_handle1 = schema_handle1;\n";
//		metadata offsets
	ret += "\ttuple_metadata_offset0 = ftaschema_get_tuple_metadata_offset(schema_handle0);\n";
	ret += "\ttuple_metadata_offset1 = ftaschema_get_tuple_metadata_offset(schema_handle1);\n";

//		unpack vars
	ret += "//\t\tGet offsets for unpacking fields from input tuple.\n";
	ret += gen_access_var_init(cid_set);

//		complex literals
	ret += "//\t\tInitialize complex literals.\n";
	ret += gen_complex_lit_init(complex_literals);
//		Initialize EmptyString to the ... empty string
//				NEED AN EMPTY LITERAL FOR EAcH STRUCTURED LITERAL
	literal_t mtstr_lit("");
	ret += "\t" + mtstr_lit.to_hfta_C_code("&EmptyString")+";\n";
	literal_t mip6_lit("0:0:0:0:0:0:0:0",LITERAL_IPV6);
	ret += "\t" + mip6_lit.to_hfta_C_code("&EmptyIp6")+";\n";

//		Initialize partial function results so they can be safely GC'd
	ret += gen_partial_fcn_init(partial_fcns);

//		Initialize non-query-parameter parameter handles
	ret += gen_pass_by_handle_init(param_handle_table);

//		Init temporal attributes referenced in select list
	ret += gen_init_temp_vars(schema, select_list, NULL);


	ret += "};\n";



//-------------------
//			Functor destructor
	ret += "//\t\tFunctor destructor.\n";
	ret +=  "~"+this->generate_functor_name()+"(){\n";

//			clean up buffer type complex literals
	ret += gen_complex_lit_dtr(complex_literals);

//			Deregister the pass-by-handle parameters
	ret += "/* register and de-register the pass-by-handle parameters */\n";
	ret += gen_pass_by_handle_dtr(param_handle_table);

//			clean up partial function results.
	ret += "/* clean up partial function storage	*/\n";
	ret += gen_partial_fcn_dtr(partial_fcns);

//			Destroy the parameters, if any need to be destroyed
	ret += "\tthis->destroy_params_"+this->generate_functor_name()+"();\n";

	ret += "};\n\n";


//-------------------
//			Parameter manipulation routines
	ret += generate_load_param_block(this->generate_functor_name(),
									this->param_tbl,param_handle_table);
	ret += generate_delete_param_block(this->generate_functor_name(),
									this->param_tbl,param_handle_table);

//-------------------
//			Register new parameter block

	ret += "int set_param_block(gs_int32_t sz, void* value){\n";
	  ret += "\tthis->destroy_params_"+this->generate_functor_name()+"();\n";
	  ret += "\treturn this->load_params_"+this->generate_functor_name()+
				"(sz, value);\n";
	ret += "};\n\n";


//-------------------
//			The create_key method.
//			Perform heap allocation.
//			ASSUME : the LHS of the preds reference channel 0 attributes
//			NOTE : it may fail if a partial function fails.

	ret += this->generate_functor_name()+"_keydef *create_key(host_tuple &tup, bool &failed){\n";
//		Variables for execution of the function.
	ret+="\t"+this->generate_functor_name()+"_keydef *retval = NULL;\n";
	ret+="\tgs_int32_t problem = 0;\n";

//		Assume unsuccessful completion
	ret+= "\tfailed = true;\n";

//		Switch the processing based on the channel
	ret+="\tif(tup.channel == 0){\n";
	ret+="// ------------ processing for channel 0\n";
	ret+="\t\thost_tuple &tup0 = tup;\n";
//		Gather partial fcns and colids ref'd by this branch
	pfcn_refs.clear();
	new_cids.clear(); local_cids.clear();
	for(p=0;p<hash_eq.size();p++){
		collect_partial_fcns(hash_eq[p]->pr->get_left_se(), pfcn_refs);
		gather_se_col_ids(hash_eq[p]->pr->get_left_se(),local_cids,NULL);
	}

//		Start by cleaning up partial function results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";
	ret += gen_partial_fcn_dtr(partial_fcns,pfcn_refs);

//			Evaluate the partial functions
	ret += gen_full_unpack_partial_fcn(schema, partial_fcns, pfcn_refs,
				new_cids, NULL, "NULL", needs_xform);
//			test passed -- unpack remaining cids.
	ret += gen_remaining_colrefs(schema, local_cids, new_cids, "NULL", needs_xform);

//			Alloc and load a key object
	ret += "\t\tretval = new "+this->generate_functor_name()+"_keydef();\n";
	for(p=0;p<hash_eq.size();p++){
		data_type *hdt = hash_eq[p]->pr->get_left_se()->get_data_type();
		if(hdt->is_buffer_type()){
			string vname = "tmp_keyvar"+int_to_string(p);
			ret += "\t\t"+hdt->make_host_cvar(vname)+" = "+generate_se_code(hash_eq[p]->pr->get_left_se(),schema)+";\n";
			ret += "\t\t"+hdt->get_hfta_buffer_assign_copy()+"(&(retval->hashkey_var"+int_to_string(p)+"),&"+vname+");\n";
		}else{
		  sprintf(tmpstr,"\t\tretval->hashkey_var%d = %s;\n",
			p,generate_se_code(hash_eq[p]->pr->get_left_se(),schema).c_str() );
		  ret += tmpstr;
		}
	}
	ret += "\t}else{\n";

	ret+="// ------------ processing for channel 1\n";
	ret+="\t\thost_tuple &tup1 = tup;\n";
//		Gather partial fcns and colids ref'd by this branch
	pfcn_refs.clear();
	new_cids.clear(); local_cids.clear();
	for(p=0;p<hash_eq.size();p++){
		collect_partial_fcns(hash_eq[p]->pr->get_right_se(), pfcn_refs);
		gather_se_col_ids(hash_eq[p]->pr->get_right_se(),local_cids,NULL);
	}

//		Start by cleaning up partial function results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";
	ret += gen_partial_fcn_dtr(partial_fcns,pfcn_refs);

//			Evaluate the partial functions
	ret += gen_full_unpack_partial_fcn(schema, partial_fcns, pfcn_refs,
				new_cids, NULL, "NULL", needs_xform);

//			test passed -- unpack remaining cids.
	ret += gen_remaining_colrefs(schema, local_cids, new_cids, "NULL", needs_xform);

//			Alloc and load a key object
	ret += "\t\tretval = new "+this->generate_functor_name()+"_keydef();\n";
	for(p=0;p<hash_eq.size();p++){
		data_type *hdt = hash_eq[p]->pr->get_right_se()->get_data_type();
		if(hdt->is_buffer_type()){
			string vname = "tmp_keyvar"+int_to_string(p);
			ret += "\t\t"+hdt->make_host_cvar(vname)+" = "+generate_se_code(hash_eq[p]->pr->get_right_se(),schema)+";\n";
			ret += "\t\t"+hdt->get_hfta_buffer_assign_copy()+"(&(retval->hashkey_var"+int_to_string(p)+"),&"+vname+");\n";
		}else{
		  sprintf(tmpstr,"\t\tretval->hashkey_var%d = %s;\n",
			p,generate_se_code(hash_eq[p]->pr->get_right_se(),schema).c_str() );
		  ret += tmpstr;
		}
	}
	ret += "\t}\n";

	ret += "\tfailed = false;\n";
	ret += "\t return retval;\n";
	ret += "}\n";


//-------------------
//			The load_ts method.
//			load into an allocated buffer.
//			ASSUME : the LHS of the preds reference channel 0 attributes
//			NOTE : it may fail if a partial function fails.
//			NOTE : cann't handle buffer attributes

	ret += "bool load_ts_from_tup("+this->generate_functor_name()+"_tempeqdef *ts, host_tuple &tup){\n";
//		Variables for execution of the function.
	ret+="\tgs_int32_t problem = 0;\n";

//		Switch the processing based on the channel
	ret+="\tif(tup.channel == 0){\n";
	ret+="// ------------ processing for channel 0\n";
	ret+="\t\thost_tuple &tup0 = tup;\n";

//		Gather partial fcns and colids ref'd by this branch
	pfcn_refs.clear();
	new_cids.clear(); local_cids.clear();
	for(p=0;p<temporal_eq.size();p++){
		collect_partial_fcns(temporal_eq[p]->pr->get_left_se(), pfcn_refs);
		gather_se_col_ids(temporal_eq[p]->pr->get_left_se(),local_cids,NULL);
	}

//		Start by cleaning up partial function results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";
	ret += gen_partial_fcn_dtr(partial_fcns,pfcn_refs);

//			Evaluate the partial functions
	ret += gen_full_unpack_partial_fcn(schema, partial_fcns, pfcn_refs,
				new_cids, NULL, "false", needs_xform);

//			test passed -- unpack remaining cids.
	ret += gen_remaining_colrefs(schema, local_cids, new_cids, "false", needs_xform);

//			load the temporal key object
	for(p=0;p<temporal_eq.size();p++){
		sprintf(tmpstr,"\t\tts->tempeq_var%d = %s;\n",
			p,generate_se_code(temporal_eq[p]->pr->get_left_se(),schema).c_str() );
		ret += tmpstr;
	}

	ret += "\t}else{\n";

	ret+="// ------------ processing for channel 1\n";
	ret+="\t\thost_tuple &tup1 = tup;\n";

//		Gather partial fcns and colids ref'd by this branch
	pfcn_refs.clear();
	new_cids.clear(); local_cids.clear();
	for(p=0;p<temporal_eq.size();p++){
		collect_partial_fcns(temporal_eq[p]->pr->get_right_se(), pfcn_refs);
		gather_se_col_ids(temporal_eq[p]->pr->get_right_se(),local_cids,NULL);
	}

//		Start by cleaning up partial function results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";
	ret += gen_partial_fcn_dtr(partial_fcns,pfcn_refs);

//			Evaluate the partial functions
	ret += gen_full_unpack_partial_fcn(schema, partial_fcns, pfcn_refs,
				new_cids, NULL, "false", needs_xform);

//			test passed -- unpack remaining cids.
	ret += gen_remaining_colrefs(schema, local_cids, new_cids, "false", needs_xform);

//			load the key object
	for(p=0;p<temporal_eq.size();p++){
		sprintf(tmpstr,"\t\tts->tempeq_var%d = %s;\n",
			p,generate_se_code(temporal_eq[p]->pr->get_right_se(),schema).c_str() );
		ret += tmpstr;
	}

	ret += "\t}\n";

	ret += "\t return true;\n";
	ret += "}\n";


//	------------------------------
//		Load ts from ts
//		(i.e make a copy)

	ret += "bool load_ts_from_ts("+this->generate_functor_name()+"_tempeqdef *lts,"+this->generate_functor_name()+"_tempeqdef *rts){\n";
	for(p=0;p<temporal_eq.size();p++){
		sprintf(tmpstr,"\tlts->tempeq_var%d = rts->tempeq_var%d;\n",p,p);
		ret += tmpstr;
	}
	ret += "}\n";

//	-------------------------------------
//		compare_ts_to_ts
//		There should be only one variable to compare.
//		If there is more, assume an arbitrary lexicographic order.

	ret += "int compare_ts_with_ts("+this->generate_functor_name()+"_tempeqdef *lts,"+this->generate_functor_name()+"_tempeqdef *rts){\n";
	for(p=0;p<temporal_eq.size();p++){
		sprintf(tmpstr,"\tif(lts->tempeq_var%d < rts->tempeq_var%d) return(-1);\n",p,p);
		ret += tmpstr;
		sprintf(tmpstr,"\tif(lts->tempeq_var%d > rts->tempeq_var%d) return(1);\n",p,p);
		ret += tmpstr;
	}
	ret += "\treturn(0);\n";
	ret += "}\n";

//	------------------------------------------
//		apply_prefilter
//		apply the prefilter

	ret += "bool apply_prefilter(host_tuple &tup){\n";

//		Variables for this procedure
	ret+="\tgs_int32_t problem = 0;\n";
	ret+="\tgs_retval_t retval;\n";

//		Switch the processing based on the channel
	ret+="\tif(tup.channel == 0){\n";
	ret+="// ------------ processing for channel 0\n";
	ret+="\t\thost_tuple &tup0 = tup;\n";
//		Gather partial fcns and colids ref'd by this branch
	pfcn_refs.clear();
	new_cids.clear(); local_cids.clear();
	for(p=0;p<prefilter[0].size();p++){
		collect_partial_fcns_pr((prefilter[0])[p]->pr, pfcn_refs);
	}

//		Start by cleaning up partial function results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";
	ret += gen_partial_fcn_dtr(partial_fcns,pfcn_refs);

	for(p=0;p<(prefilter[0]).size();++p){
		sprintf(tmpstr,"//\t\tPredicate clause %d.\n",p);
		ret += tmpstr;
//			Find the set of	variables accessed in this CNF elem,
//			but in no previous element.
		col_id_set new_pr_cids;
		get_new_pred_cids((prefilter[0])[p]->pr,local_cids,new_pr_cids, NULL);
//			Unpack these values.
		ret += gen_unpack_cids(schema, new_pr_cids, "false", needs_xform);
//			Find partial fcns ref'd in this cnf element
		set<int> pr_pfcn_refs;
		collect_partial_fcns_pr((prefilter[0])[p]->pr, pr_pfcn_refs);
		ret += gen_unpack_partial_fcn(schema,partial_fcns,pr_pfcn_refs,"false");

		ret += "\t\tif( !("+generate_predicate_code((prefilter[0])[p]->pr,schema)+") ) return(false);\n";
	}
	ret += "\t}else{\n";
	ret+="// ------------ processing for channel 1\n";
	ret+="\t\thost_tuple &tup1 = tup;\n";
//		Gather partial fcns and colids ref'd by this branch
	pfcn_refs.clear();
	new_cids.clear(); local_cids.clear();
	for(p=0;p<prefilter[1].size();p++){
		collect_partial_fcns_pr((prefilter[1])[p]->pr, pfcn_refs);
	}

//		Start by cleaning up partial function results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";
	ret += gen_partial_fcn_dtr(partial_fcns,pfcn_refs);

	for(p=0;p<(prefilter[1]).size();++p){
		sprintf(tmpstr,"//\t\tPredicate clause %d.\n",p);
		ret += tmpstr;
//			Find the set of	variables accessed in this CNF elem,
//			but in no previous element.
		col_id_set pr_new_cids;
		get_new_pred_cids((prefilter[1])[p]->pr,local_cids, pr_new_cids, NULL);
//			Unpack these values.
		ret += gen_unpack_cids(schema, pr_new_cids, "false", needs_xform);
//			Find partial fcns ref'd in this cnf element
		set<int> pr_pfcn_refs;
		collect_partial_fcns_pr((prefilter[1])[p]->pr, pr_pfcn_refs);
		ret += gen_unpack_partial_fcn(schema,partial_fcns,pr_pfcn_refs,"false");

		ret += "\t\tif( !("+generate_predicate_code((prefilter[1])[p]->pr,schema)+ ") ) return(false);\n";
	}

	ret += "\t}\n";
	ret+="\treturn true;\n";
	ret += "}\n";


//	-------------------------------------
//			create_output_tuple
//			If the postfilter on the pair of tuples passes,
//			create an output tuple from the combined information.
//			(Plus, outer join processing)

	ret += "host_tuple create_output_tuple(const host_tuple &tup0, const host_tuple &tup1, bool &failed){\n";

	ret += "\thost_tuple tup;\n";
	ret += "\tfailed = true;\n";
	ret += "\tgs_retval_t retval = 0;\n";
	ret += "\tgs_int32_t problem = 0;\n";

//		Start by cleaning up partial function results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";
	pfcn_refs.clear();
	new_cids.clear(); local_cids.clear();
	for(p=0;p<postfilter.size();p++){
		collect_partial_fcns_pr(postfilter[p]->pr, pfcn_refs);
	}
	for(s=0;s<select_list.size();s++){
		collect_partial_fcns(select_list[s]->se, pfcn_refs);
	}
	ret += gen_partial_fcn_dtr(partial_fcns,pfcn_refs);


	ret+="\tif(tup0.data && tup1.data){\n";
//			Evaluate the postfilter
	new_cids.clear(); local_cids.clear();
	for(p=0;p<postfilter.size();p++){
		sprintf(tmpstr,"//\t\tPredicate clause %d.\n",p);
		ret += tmpstr;
//			Find the set of	variables accessed in this CNF elem,
//			but in no previous element.
		col_id_set pr_new_cids;
		get_new_pred_cids(postfilter[p]->pr,local_cids, pr_new_cids, NULL);
//			Unpack these values.
		ret += gen_unpack_cids(schema, pr_new_cids, "tup", needs_xform);
//			Find partial fcns ref'd in this cnf element
		set<int> pr_pfcn_refs;
		collect_partial_fcns_pr(postfilter[p]->pr, pr_pfcn_refs);
		ret += gen_unpack_partial_fcn(schema,partial_fcns,pr_pfcn_refs,"tup");

		ret += "\t\tif( !("+generate_predicate_code(postfilter[p]->pr,schema)+ ") ) return(tup);\n";
	}


//		postfilter passed, evaluate partial functions for select list

	set<int> sl_pfcns;
	col_id_set se_cids;
	for(s=0;s<select_list.size();s++){
		collect_partial_fcns(select_list[s]->se, sl_pfcns);
	}

	if(sl_pfcns.size() > 0)
		ret += "//\t\tUnpack remaining partial fcns.\n";
	ret += gen_full_unpack_partial_fcn(schema, partial_fcns, sl_pfcns,
					local_cids, NULL, "tup", needs_xform);

//			Unpack remaining fields
	ret += "//\t\tunpack any remaining fields from the input tuples.\n";
	for(s=0;s<select_list.size();s++)
		get_new_se_cids(select_list[s]->se, local_cids,se_cids,NULL);
	ret += gen_unpack_cids(schema,  se_cids,"tup", needs_xform);


//			Deal with outer join stuff
	col_id_set l_cids, r_cids;
	col_id_set::iterator ocsi;
	for(ocsi=local_cids.begin();ocsi!=local_cids.end();++ocsi){
		if((*ocsi).tblvar_ref == 0) l_cids.insert((*ocsi));
		else						r_cids.insert((*ocsi));
	}
	for(ocsi=se_cids.begin();ocsi!=se_cids.end();++ocsi){
		if((*ocsi).tblvar_ref == 0) l_cids.insert((*ocsi));
		else						r_cids.insert((*ocsi));
	}

	ret += "\t}else if(tup0.data){\n";
	string unpack_null = ""; col_id_set extra_cids;
	for(ocsi=r_cids.begin();ocsi!=r_cids.end();++ocsi){
		string field = (*ocsi).field;
		if(r_equiv.count(field)){
			unpack_null+="\t\tunpack_var_"+field+"_1="+generate_se_code(r_equiv[field],schema)+";\n";
			get_new_se_cids(r_equiv[field],l_cids,new_cids,NULL);
		}else{
	    	int schref = (*ocsi).schema_ref;
			data_type dt(schema->get_type_name(schref,field));
			literal_t empty_lit(dt.type_indicator());
			if(empty_lit.is_cpx_lit()){
//				sprintf(tmpstr,"&(unpack_var_%s_1)",field.c_str());
//				unpack_null += "\t"+empty_lit.to_hfta_C_code(tmpstr)+";\n";
//					NB : works for string type only
//					NNB: installed fix for ipv6, more of this should be pushed
//						into the literal_t code.
				unpack_null+="\tunpack_var_"+field+"_1= "+empty_lit.hfta_empty_literal_name()+";\n";
			}else{
				unpack_null+="\tunpack_var_"+field+"_1="+empty_lit.to_hfta_C_code("")+";\n";
			}
		}
	}
	ret += gen_unpack_cids(schema,  l_cids, "tup", needs_xform);
	ret += gen_unpack_cids(schema,  extra_cids, "tup", needs_xform);
	ret += unpack_null;
	ret += gen_unpack_partial_fcn(schema, partial_fcns, sl_pfcns, "tup");

	ret+="\t}else{\n";
	unpack_null = ""; extra_cids.clear();
	for(ocsi=l_cids.begin();ocsi!=l_cids.end();++ocsi){
		string field = (*ocsi).field;
		if(l_equiv.count(field)){
			unpack_null+="\t\tunpack_var_"+field+"_0="+generate_se_code(l_equiv[field],schema)+";\n";
			get_new_se_cids(l_equiv[field],r_cids,new_cids,NULL);
		}else{
	    	int schref = (*ocsi).schema_ref;
			data_type dt(schema->get_type_name(schref,field));
			literal_t empty_lit(dt.type_indicator());
			if(empty_lit.is_cpx_lit()){
//				sprintf(tmpstr,"&(unpack_var_%s_0)",field.c_str());
//				unpack_null += "\t"+empty_lit.to_hfta_C_code(tmpstr)+";\n";
//					NB : works for string type only
//					NNB: installed fix for ipv6, more of this should be pushed
//						into the literal_t code.
				unpack_null+="\tunpack_var_"+field+"_0= "+empty_lit.hfta_empty_literal_name()+";\n";
			}else{
				unpack_null+="\tunpack_var_"+field+"_0="+empty_lit.to_hfta_C_code("")+";\n";
			}
		}
	}
	ret += gen_unpack_cids(schema,  r_cids, "tup", needs_xform);
	ret += gen_unpack_cids(schema,  extra_cids, "tup", needs_xform);
	ret += unpack_null;
	ret += gen_unpack_partial_fcn(schema, partial_fcns, sl_pfcns, "tup");
	ret+="\t}\n";



//          Unpack any BUFFER type selections into temporaries
//          so that I can compute their size and not have
//          to recompute their value during tuple packing.
//          I can use regular assignment here because
//          these temporaries are non-persistent.

	ret += "//\t\tCompute the size of the tuple.\n";
	ret += "//\t\t\tNote: buffer storage packed at the end of the tuple.\n";

//			Unpack all buffer type selections, to be able to compute their size
	ret += gen_buffer_selvars(schema, select_list);

//      The size of the tuple is the size of the tuple struct plus the
//      size of the buffers to be copied in.

    ret+="\ttup.tuple_size = sizeof(" + generate_tuple_name( this->get_node_name()) +") + sizeof(gs_uint8_t)";
	ret += gen_buffer_selvars_size(select_list,schema);
      ret.append(";\n");

//		Allocate tuple data block.
	ret += "//\t\tCreate the tuple block.\n";
	  ret += "\ttup.data = malloc(tup.tuple_size);\n";
	  ret += "\ttup.heap_resident = true;\n";
//	  ret += "\ttup.channel = 0;\n";

//		Mark tuple as regular
	  ret += "\t*((gs_sp_t )tup.data + sizeof(" + generate_tuple_name( this->get_node_name()) +")) = REGULAR_TUPLE;\n";


	  ret += "\t"+generate_tuple_name( this->get_node_name())+" *tuple = ("+
				generate_tuple_name( this->get_node_name())+" *)(tup.data);\n";

//		Start packing.
//			(Here, offsets are hard-wired.  is this a problem?)

	ret += "//\t\tPack the fields into the tuple.\n";
	ret += gen_pack_tuple(schema,select_list,this->get_node_name(), false );

//			Delete string temporaries
	ret += gen_buffer_selvars_dtr(select_list);

	ret += "\tfailed = false;\n";
	ret += "\treturn tup;\n";
	ret += "};\n";



//-----------------------------
//			Method for checking whether tuple is temporal

	ret += "bool temp_status_received(host_tuple &tup){\n";

//		Switch the processing based on the channel
	ret+="\tif(tup.channel == 0){\n";
	ret+="\t\thost_tuple &tup0 = tup;\n";
	ret += gen_temp_tuple_check(this->node_name, 0);
	ret += "\t}else{\n";
	ret+="\t\thost_tuple &tup1 = tup;\n";
	ret += gen_temp_tuple_check(this->node_name, 1);
	ret += "\t}\n";
	ret += "\treturn temp_tuple_received;\n};\n\n";


//-------------------------------------------------------------------
//		Temporal update functions


//		create a temp status tuple
	ret += "int create_temp_status_tuple(const host_tuple &tup0, const host_tuple &tup1, host_tuple& result) {\n\n";

	ret += "\tgs_retval_t retval = 0;\n";
	ret += "\tgs_int32_t problem = 0;\n";

	ret += "\tif(tup0.data){\n";

//		Unpack all the temporal attributes references in select list
	col_id_set found_cids;

	for(s=0;s<select_list.size();s++){
		if (select_list[s]->se->get_data_type()->is_temporal()) {
//			Find the set of	attributes accessed in this SE
			col_id_set new_cids;
			get_new_se_cids(select_list[s]->se,found_cids, new_cids, NULL);
		}
	}

	//			Deal with outer join stuff
	l_cids.clear(), r_cids.clear();
	for(ocsi=found_cids.begin();ocsi!=found_cids.end();++ocsi){
		if((*ocsi).tblvar_ref == 0) l_cids.insert((*ocsi));
		else						r_cids.insert((*ocsi));
	}
	unpack_null = "";
	extra_cids.clear();
	for(ocsi=r_cids.begin();ocsi!=r_cids.end();++ocsi){
		string field = (*ocsi).field;
		if(r_equiv.count(field)){
			unpack_null+="\t\tunpack_var_"+field+"_1="+generate_se_code(r_equiv[field],schema)+";\n";
			col_id_set addnl_cids;
			get_new_se_cids(r_equiv[field],l_cids,addnl_cids,NULL);
		}else{
	    	int schref = (*ocsi).schema_ref;
			data_type dt(schema->get_type_name(schref,field));
			literal_t empty_lit(dt.type_indicator());
			if(empty_lit.is_cpx_lit()){
				sprintf(tmpstr,"&(unpack_var_%s_1)",field.c_str());
				unpack_null += "\t"+empty_lit.to_hfta_C_code(tmpstr)+";\n";
			}else{
				unpack_null+="\tunpack_var_"+field+"_1="+empty_lit.to_hfta_C_code("")+";\n";
			}
		}
	}
	ret += gen_unpack_cids(schema,  l_cids, "1", needs_xform);
	ret += gen_unpack_cids(schema,  extra_cids, "1", needs_xform);
	ret += unpack_null;

	ret+="\t}else if (tup1.data) {\n";
	unpack_null = ""; extra_cids.clear();
	for(ocsi=l_cids.begin();ocsi!=l_cids.end();++ocsi){
		string field = (*ocsi).field;
		if(l_equiv.count(field)){
			unpack_null+="\t\tunpack_var_"+field+"_0="+generate_se_code(l_equiv[field],schema)+";\n";
			col_id_set addnl_cids;
			get_new_se_cids(l_equiv[field],r_cids,addnl_cids,NULL);
		}else{
	    	int schref = (*ocsi).schema_ref;
			data_type dt(schema->get_type_name(schref,field));
			literal_t empty_lit(dt.type_indicator());
			if(empty_lit.is_cpx_lit()){
				sprintf(tmpstr,"&(unpack_var_%s_0)",field.c_str());
				unpack_null += "\t"+empty_lit.to_hfta_C_code(tmpstr)+";\n";
			}else{
				unpack_null+="\tunpack_var_"+field+"_0="+empty_lit.to_hfta_C_code("")+";\n";
			}
		}
	}
	ret += gen_unpack_cids(schema,  r_cids, "1", needs_xform);
	ret += gen_unpack_cids(schema,  extra_cids, "1", needs_xform);
	ret += unpack_null;
	ret+="\t}\n";

	ret += gen_init_temp_status_tuple(this->get_node_name());

//		Start packing.
	ret += "//\t\tPack the fields into the tuple.\n";
	ret += gen_pack_tuple(schema,select_list,this->get_node_name(), true );


	ret += "\treturn 0;\n";
	ret += "};\n\n";


	ret += "};\n\n\n";

//----------------------------------------------------------
//			The hash function

	ret += "struct "+generate_functor_name()+"_hash_func{\n";
	ret += "\tgs_uint32_t operator()(const "+generate_functor_name()+
				"_keydef *key) const{\n";
	ret += "\t\treturn( (";
	if(hashkey_dt.size() > 0){
	  for(p=0;p<hashkey_dt.size();p++){
		if(p>0) ret += "^";
		if(hashkey_dt[p]->use_hashfunc()){
//			sprintf(tmpstr,"%s(&(key->hashkey_var%d))",hashkey_dt[p]->get_hfta_hashfunc().c_str(),p);
			if(hashkey_dt[p]->is_buffer_type())
				sprintf(tmpstr,"(%s*%s(&(key->hashkey_var%d)))",hash_nums[p%NRANDS].c_str(),hashkey_dt[p]->get_hfta_hashfunc().c_str(),p);
			else
				sprintf(tmpstr,"(%s*%s(key->hashkey_var%d))",hash_nums[p%NRANDS].c_str(),hashkey_dt[p]->get_hfta_hashfunc().c_str(),p);
		}else{
			sprintf(tmpstr,"(%s*key->hashkey_var%d)",hash_nums[p%NRANDS].c_str(),p);
		}
		ret += tmpstr;
	  }
	}else{
		ret += "0";
	}
	ret += ") >> 32);\n";
	ret += "\t}\n";
	ret += "};\n\n";

//----------------------------------------------------------
//			The comparison function

	ret += "struct "+generate_functor_name()+"_equal_func{\n";
	ret += "\tbool operator()(const "+generate_functor_name()+"_keydef *key1, "+
			generate_functor_name()+"_keydef *key2) const{\n";
	ret += "\t\treturn( (";
	if(hashkey_dt.size() > 0){
	  for(p=0;p<hashkey_dt.size();p++){
		if(p>0) ret += ") && (";
		if(hashkey_dt[p]->complex_comparison(hashkey_dt[p])){
		  if(hashkey_dt[p]->is_buffer_type())
			sprintf(tmpstr,"(%s(&(key1->hashkey_var%d), &(key2->hashkey_var%d))==0)",
				hashkey_dt[p]->get_hfta_comparison_fcn(hashkey_dt[p]).c_str(),p,p);
		  else
			sprintf(tmpstr,"(%s((key1->hashkey_var%d), (key2->hashkey_var%d))==0)",
				hashkey_dt[p]->get_hfta_comparison_fcn(hashkey_dt[p]).c_str(),p,p);
		}else{
			sprintf(tmpstr,"key1->hashkey_var%d == key2->hashkey_var%d",p,p);
		}
		ret += tmpstr;
	  }
	}else{
		ret += "1";
	}
	ret += ") );\n";
	ret += "\t}\n";
	ret += "};\n\n";


	return(ret);
}



string join_eq_hash_qpn::generate_operator(int i, string params){

		return(
			"	join_eq_hash_operator<" +
			generate_functor_name()+ ","+
			generate_functor_name() + "_tempeqdef,"+
			generate_functor_name() + "_keydef,"+
			generate_functor_name()+"_hash_func,"+
			generate_functor_name()+"_equal_func"
			"> *op"+int_to_string(i)+" = new join_eq_hash_operator<"+
			generate_functor_name()+","+
			generate_functor_name() + "_tempeqdef,"+
			generate_functor_name() + "_keydef,"+
			generate_functor_name()+"_hash_func,"+
			generate_functor_name()+"_equal_func"
			">("+params+", "+
			int_to_string(from[0]->get_property()+2*from[1]->get_property())+", \"" + get_node_name() +
"\");\n"
		);
}



////////////////////////////////////////////////////////////////
////	SGAHCWCB functor



string sgahcwcb_qpn::generate_functor_name(){
	return("sgahcwcb_functor_" + normalize_name(this->get_node_name()));
}


string sgahcwcb_qpn::generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, vector<bool> &needs_xform){
	int a,g,w,s;


//			Initialize generate utility globals
	segen_gb_tbl = &(gb_tbl);


//--------------------------------
//			group definition class
	string ret = "class " + generate_functor_name() + "_groupdef{\n";
	ret += "public:\n";
	ret += "\tbool valid;\n";
	for(g=0;g<this->gb_tbl.size();g++){
		sprintf(tmpstr,"gb_var%d",g);
		ret+="\t"+this->gb_tbl.get_data_type(g)->make_host_cvar(tmpstr)+";\n";
	}
//		Constructors
	ret += "\t"+generate_functor_name() + "_groupdef(){valid=true;};\n";
	ret += "\t"+generate_functor_name() + "_groupdef("+
		this->generate_functor_name() + "_groupdef *gd){\n";
	for(g=0;g<gb_tbl.size();g++){
		data_type *gdt = gb_tbl.get_data_type(g);
		if(gdt->is_buffer_type()){
			sprintf(tmpstr,"\t\t%s(&gb_var%d, &(gd->gb_var%d));\n",
			  gdt->get_hfta_buffer_assign_copy().c_str(),g,g );
			ret += tmpstr;
		}else{
			sprintf(tmpstr,"\t\tgb_var%d = gd->gb_var%d;\n",g,g);
			ret += tmpstr;
		}
	}
	ret += "\tvalid=true;\n";
	ret += "\t};\n";
//		destructor
	ret += "\t~"+ generate_functor_name() + "_groupdef(){\n";
	for(g=0;g<gb_tbl.size();g++){
		data_type *gdt = gb_tbl.get_data_type(g);
		if(gdt->is_buffer_type()){
			sprintf(tmpstr,"\t\t%s(&gb_var%d);\n",
			  gdt->get_hfta_buffer_destroy().c_str(), g );
			ret += tmpstr;
		}
	}
	ret += "\t};\n";
	ret +="};\n\n";

//--------------------------------
//			aggr definition class
	ret += "class " + this->generate_functor_name() + "_aggrdef{\n";
	ret += "public:\n";
	for(a=0;a<aggr_tbl.size();a++){
aggr_table_entry *ate = aggr_tbl.agr_tbl[a];
		sprintf(tmpstr,"aggr_var%d",a);
		if(aggr_tbl.is_builtin(a))
	  	ret+="\t"+ aggr_tbl.get_data_type(a)->make_host_cvar(tmpstr)+";\n";
		else
	  	ret+="\t"+ aggr_tbl.get_storage_type(a)->make_host_cvar(tmpstr)+";\n";
	}
//		Constructors
	ret += "\t"+this->generate_functor_name() + "_aggrdef(){};\n";
//		destructor
	ret += "\t~"+this->generate_functor_name() + "_aggrdef(){\n";
	for(a=0;a<aggr_tbl.size();a++){
aggr_table_entry *ate = aggr_tbl.agr_tbl[a];
		if(aggr_tbl.is_builtin(a)){
			data_type *adt = aggr_tbl.get_data_type(a);
			if(adt->is_buffer_type()){
				sprintf(tmpstr,"\t\t%s(&aggr_var%d);\n",
		  		adt->get_hfta_buffer_destroy().c_str(), a );
				ret += tmpstr;
			}
		}else{
			ret+="\t\t"+aggr_tbl.get_op(a)+"_HFTA_AGGR_DESTROY_(";
			if(aggr_tbl.get_storage_type(a)->get_type() != fstring_t) ret+="&";
			ret+="(aggr_var"+int_to_string(a)+"));\n";
		}
	}
	ret += "\t};\n";
	ret +="};\n\n";

//--------------------------------
//			superaggr definition class
	ret += "class " + this->generate_functor_name() + "_statedef{\n";
	ret += "public:\n";
	for(a=0;a<aggr_tbl.size();a++){
aggr_table_entry *ate = aggr_tbl.agr_tbl[a];
		if(ate->is_superaggr()){
			sprintf(tmpstr,"aggr_var%d",a);
			if(aggr_tbl.is_builtin(a))
		  	ret+="\t"+ aggr_tbl.get_data_type(a)->make_host_cvar(tmpstr)+";\n";
			else
		  	ret+="\t"+ aggr_tbl.get_storage_type(a)->make_host_cvar(tmpstr)+";\n";
		}
	}
	set<string>::iterator ssi;
	for(ssi=states_refd.begin(); ssi!=states_refd.end(); ++ssi){
		string state_nm = (*ssi);
		int state_id = Ext_fcns->lookup_state(state_nm);
		data_type *dt = Ext_fcns->get_storage_dt(state_id);
		string state_var = "state_var_"+state_nm;
		ret += "\t"+dt->make_host_cvar(state_var)+";\n";
	}
//		Constructors
	ret += "\t"+this->generate_functor_name() + "_statedef(){};\n";
//		destructor
	ret += "\t~"+this->generate_functor_name() + "_statedef(){\n";
	for(a=0;a<aggr_tbl.size();a++){
aggr_table_entry *ate = aggr_tbl.agr_tbl[a];
		if(ate->is_superaggr()){
			if(aggr_tbl.is_builtin(a)){
				data_type *adt = aggr_tbl.get_data_type(a);
				if(adt->is_buffer_type()){
					sprintf(tmpstr,"\t\t%s(&aggr_var%d);\n",
			  		adt->get_hfta_buffer_destroy().c_str(), a );
					ret += tmpstr;
				}
			}else{
				ret+="\t\t"+aggr_tbl.get_op(a)+"_HFTA_AGGR_DESTROY_(";
				if(aggr_tbl.get_storage_type(a)->get_type() != fstring_t) ret+="&";
				ret+="(aggr_var"+int_to_string(a)+"));\n";
			}
		}
	}
	for(ssi=states_refd.begin(); ssi!=states_refd.end(); ++ssi){
		string state_nm = (*ssi);
		int state_id = Ext_fcns->lookup_state(state_nm);
		string state_var = "state_var_"+state_nm;
		ret += "\t_sfun_state_destroy_"+state_nm+"(&"+state_var+");\n";
	}

	ret += "\t};\n";
	ret +="};\n\n";


//--------------------------------
//			gb functor class
	ret += "class " + this->generate_functor_name() + "{\n";

//			Find variables referenced in this query node.

  col_id_set cid_set;
  col_id_set::iterator csi;

    for(w=0;w<where.size();++w)
    	gather_pr_col_ids(where[w]->pr,cid_set,segen_gb_tbl);
    for(w=0;w<having.size();++w)
    	gather_pr_col_ids(having[w]->pr,cid_set,segen_gb_tbl);
    for(w=0;w<cleanby.size();++w)
    	gather_pr_col_ids(cleanby[w]->pr,cid_set,segen_gb_tbl);
    for(w=0;w<cleanwhen.size();++w)
    	gather_pr_col_ids(cleanwhen[w]->pr,cid_set,segen_gb_tbl);
	for(g=0;g<gb_tbl.size();g++)
		gather_se_col_ids(gb_tbl.get_def(g),cid_set,segen_gb_tbl);

    for(s=0;s<select_list.size();s++){
    	gather_se_col_ids(select_list[s]->se,cid_set,segen_gb_tbl);	// descends into aggregates
    }


//			Private variables : store the state of the functor.
//			1) variables for unpacked attributes
//			2) offsets of the upacked attributes
//			3) storage of partial functions
//			4) storage of complex literals (i.e., require a constructor)

	ret += "private:\n";

	// var to save the schema handle
	ret += "\tint schema_handle0;\n";

	// generate the declaration of all the variables related to
	// temp tuples generation
	ret += gen_decl_temp_vars();

//			unpacked attribute storage, offsets
	ret += "//\t\tstorage and offsets of accessed fields.\n";
	ret += generate_access_vars(cid_set, schema);
//		tuple metadata offset
	ret += "\ttuple_metadata_offset0;\n";

//			Variables to store results of partial functions.
//			WARNING find_partial_functions modifies the SE
//			(it marks the partial function id).
	ret += "//\t\tParital function result storage\n";
	vector<scalarexp_t *> partial_fcns;
	vector<int> fcn_ref_cnt;
	vector<bool> is_partial_fcn;
	for(s=0;s<select_list.size();s++){
		find_partial_fcns(select_list[s]->se, &partial_fcns, NULL,NULL, Ext_fcns);
	}
	for(w=0;w<where.size();w++){
		find_partial_fcns_pr(where[w]->pr, &partial_fcns, NULL,NULL, Ext_fcns);
	}
	for(w=0;w<having.size();w++){
		find_partial_fcns_pr(having[w]->pr, &partial_fcns, NULL,NULL, Ext_fcns);
	}
	for(w=0;w<cleanby.size();w++){
		find_partial_fcns_pr(cleanby[w]->pr, &partial_fcns, NULL,NULL, Ext_fcns);
	}
	for(w=0;w<cleanwhen.size();w++){
		find_partial_fcns_pr(cleanwhen[w]->pr, &partial_fcns, NULL,NULL, Ext_fcns);
	}
	for(g=0;g<gb_tbl.size();g++){
		find_partial_fcns(gb_tbl.get_def(g), &partial_fcns, NULL,NULL, Ext_fcns);
	}
	for(a=0;a<aggr_tbl.size();a++){
		find_partial_fcns(aggr_tbl.get_aggr_se(a), &partial_fcns, NULL,NULL, Ext_fcns);
	}
	if(partial_fcns.size()>0){
	  ret += "/*\t\tVariables for storing results of partial functions. \t*/\n";
	  ret += generate_partial_fcn_vars(partial_fcns,fcn_ref_cnt,is_partial_fcn,false);
	}

//			Complex literals (i.e., they need constructors)
	ret += "//\t\tComplex literal storage.\n";
	cplx_lit_table *complex_literals = this->get_cplx_lit_tbl(Ext_fcns);
	ret += generate_complex_lit_vars(complex_literals);

//			Pass-by-handle parameters
	ret += "//\t\tPass-by-handle storage.\n";
	vector<handle_param_tbl_entry *> param_handle_table = this->get_handle_param_tbl(Ext_fcns);
	ret += generate_pass_by_handle_vars(param_handle_table);

//			Create cached temporaries for UDAF return values.
	ret += "//\t\tTemporaries for UDAF return values.\n";
   	for(a=0;a<aggr_tbl.size();a++){
		if(! aggr_tbl.is_builtin(a)){
			int afcn_id = aggr_tbl.get_fcn_id(a);
			data_type *adt = Ext_fcns->get_fcn_dt(afcn_id);
			sprintf(tmpstr,"udaf_ret_%d", a);
			ret+="\t"+adt->make_host_cvar(tmpstr)+";\n";
		}
	}



//			variables to hold parameters.
	ret += "//\tfor query parameters\n";
	ret += generate_param_vars(param_tbl);

//		Is there a temporal flush?  If so create flush temporaries,
//		create flush indicator.
	bool uses_temporal_flush = false;
	for(g=0;g<gb_tbl.size();g++){
		data_type *gdt = gb_tbl.get_data_type(g);
		if(gdt->is_temporal())
			uses_temporal_flush = true;
	}

	if(uses_temporal_flush){
		ret += "//\t\tFor temporal flush\n";
		for(g=0;g<gb_tbl.size();g++){
			data_type *gdt = gb_tbl.get_data_type(g);
			if(gdt->is_temporal()){
			  sprintf(tmpstr,"last_gb%d",g);
			  ret+="\t"+gb_tbl.get_data_type(g)->make_host_cvar(tmpstr)+";\n";
			  sprintf(tmpstr,"last_flushed_gb%d",g);
			  ret+="\t"+gb_tbl.get_data_type(g)->make_host_cvar(tmpstr)+";\n";
			}
		}
		ret += "\tbool needs_temporal_flush;\n";
	}

//			The publicly exposed functions

	ret += "\npublic:\n";


//-------------------
//			The functor constructor
//			pass in the schema handle.
//			1) make assignments to the unpack offset variables
//			2) initialize the complex literals

	ret += "//\t\tFunctor constructor.\n";
	ret +=  this->generate_functor_name()+"(int schema_handle0){\n";

	// save the schema handle
	ret += "\t\tthis->schema_handle0 = schema_handle0;\n";
//		tuple metadata offset
	ret += "\ttuple_metadata_offset0 = ftaschema_get_tuple_metadata_offset(schema_handle0);\n";

//		unpack vars
	ret += "//\t\tGet offsets for unpacking fields from input tuple.\n";
	ret += gen_access_var_init(cid_set);

//		aggregate return vals : refd in both final_sample
//		and create_output_tuple
//			Create cached temporaries for UDAF return values.
   	for(a=0;a<aggr_tbl.size();a++){
		if(! aggr_tbl.is_builtin(a)){
			int afcn_id = aggr_tbl.get_fcn_id(a);
			data_type *adt = Ext_fcns->get_fcn_dt(afcn_id);
			sprintf(tmpstr,"udaf_ret_%d", a);
			ret+="\t"+adt->make_host_cvar(tmpstr)+";\n";
		}
	}

//		complex literals
	ret += "//\t\tInitialize complex literals.\n";
	ret += gen_complex_lit_init(complex_literals);

//		Initialize partial function results so they can be safely GC'd
	ret += gen_partial_fcn_init(partial_fcns);

//		Initialize non-query-parameter parameter handles
	ret += gen_pass_by_handle_init(param_handle_table);

//		temporal flush variables
//		ASSUME that structured values won't be temporal.
	if(uses_temporal_flush){
		ret += "//\t\tInitialize temporal flush variables.\n";
		for(g=0;g<gb_tbl.size();g++){
			data_type *gdt = gb_tbl.get_data_type(g);
			if(gdt->is_temporal()){
				literal_t gl(gdt->type_indicator());
				sprintf(tmpstr,"\tlast_gb%d = %s;\n",g, gl.to_hfta_C_code("").c_str());
				ret.append(tmpstr);
			}
		}
		ret += "\tneeds_temporal_flush = false;\n";
	}

	//		Init temporal attributes referenced in select list
	ret += gen_init_temp_vars(schema, select_list, segen_gb_tbl);

	ret += "};\n";


//-------------------
//			Functor destructor
	ret += "//\t\tFunctor destructor.\n";
	ret +=  "~"+this->generate_functor_name()+"(){\n";

//			clean up buffer type complex literals
	ret += gen_complex_lit_dtr(complex_literals);

//			Deregister the pass-by-handle parameters
	ret += "/* register and de-register the pass-by-handle parameters */\n";
	ret += gen_pass_by_handle_dtr(param_handle_table);

//			clean up partial function results.
	ret += "/* clean up partial function storage	*/\n";
	ret += gen_partial_fcn_dtr(partial_fcns);

//			Destroy the parameters, if any need to be destroyed
	ret += "\tthis->destroy_params_"+this->generate_functor_name()+"();\n";

	ret += "};\n\n";


//-------------------
//			Parameter manipulation routines
	ret += generate_load_param_block(this->generate_functor_name(),
									this->param_tbl,param_handle_table);
	ret += generate_delete_param_block(this->generate_functor_name(),
									this->param_tbl,param_handle_table);

//-------------------
//			Register new parameter block

	ret += "int set_param_block(gs_int32_t sz, void* value){\n";
	  ret += "\tthis->destroy_params_"+this->generate_functor_name()+"();\n";
	  ret += "\treturn this->load_params_"+this->generate_functor_name()+
				"(sz, value);\n";
	ret += "};\n\n";

//-------------------
//		the create_group method.
//		This method creates a group in a buffer passed in
//		(to allow for creation on the stack).
//		There are also a couple of side effects:
//		1) evaluate the WHERE clause (and therefore, unpack all partial fcns)
//		2) determine if a temporal flush is required.

	ret += this->generate_functor_name()+"_groupdef *create_group(host_tuple &tup0, gs_sp_t buffer){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

	if(partial_fcns.size()>0){		// partial fcn access failure
	  ret += "\tgs_retval_t retval = 0;\n";
	  ret += "\n";
	}
//		return value
	ret += "\t"+generate_functor_name()+"_groupdef *gbval = ("+generate_functor_name()+
			"_groupdef *) buffer;\n";

//		Start by cleaning up partial function results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";

	set<int> gb_pfcns;	// partial fcns in gbdefs, aggr se's
	for(g=0;g<gb_tbl.size();g++){
		collect_partial_fcns(gb_tbl.get_def(g), gb_pfcns);
	}
	ret += gen_partial_fcn_dtr(partial_fcns,gb_pfcns);
//	ret += gen_partial_fcn_dtr(partial_fcns);


	ret += gen_temp_tuple_check(this->node_name, 0);
	col_id_set found_cids;	// colrefs unpacked thus far.
	ret += gen_unpack_temp_vars(schema, found_cids, select_list, segen_gb_tbl, needs_xform);



//			Save temporal group-by variables


	ret.append("\n\t//\t\tCompute temporal groupby attributes\n\n");

	  for(g=0;g<gb_tbl.size();g++){

			data_type *gdt = gb_tbl.get_data_type(g);

			if(gdt->is_temporal()){
				sprintf(tmpstr,"\tgbval->gb_var%d = %s;\n",
					g, generate_se_code(gb_tbl.get_def(g),schema).c_str() );
				ret.append(tmpstr);
			}
		}
		ret.append("\n");



//			Compare the temporal GB vars with the stored ones,
//			set flush indicator and update stored GB vars if there is any change.

	if(uses_temporal_flush){
		ret+= "\tif( !( (";
		bool first_one = true;
		for(g=0;g<gb_tbl.size();g++){
			data_type *gdt = gb_tbl.get_data_type(g);

			if(gdt->is_temporal()){
			  sprintf(tmpstr,"last_gb%d",g);   string lhs_op = tmpstr;
			  sprintf(tmpstr,"gbval->gb_var%d",g);   string rhs_op = tmpstr;
			  if(first_one){first_one = false;} else {ret += ") && (";}
			  ret += generate_equality_test(lhs_op, rhs_op, gdt);
			}
		}
		ret += ") ) ){\n";
		for(g=0;g<gb_tbl.size();g++){
		  data_type *gdt = gb_tbl.get_data_type(g);
		  if(gdt->is_temporal()){
			  if(gdt->is_buffer_type()){
				sprintf(tmpstr,"\t\t%s(&(gbval->gb_var%d),&last_gb%d);\n",gdt->get_hfta_buffer_replace().c_str(),g,g);
			  }else{
				sprintf(tmpstr,"\t\tlast_flushed_gb%d = last_gb%d;\n",g,g);
				ret += tmpstr;
				sprintf(tmpstr,"\t\tlast_gb%d = gbval->gb_var%d;\n",g,g);
			  }
			  ret += tmpstr;
			}
		}
/*
		if(uses_temporal_flush){
			for(g=0;g<gb_tbl.size();g++){
				data_type *gdt = gb_tbl.get_data_type(g);
				if(gdt->is_temporal()){
					ret+="if(last_flushed_gb"+int_to_string(g)+">0)\n";
					break;
				}
			}
		}
*/
		ret += "\t\tneeds_temporal_flush=true;\n";
		ret += "\t\t}else{\n"
			"\t\t\tneeds_temporal_flush=false;\n"
			"\t\t}\n";
	}


//		For temporal status tuple we don't need to do anything else
	ret += "\tif (temp_tuple_received) return NULL;\n\n";


//		The partial functions ref'd in the group-by var
//		definitions must be evaluated.  If one returns false,
//		then implicitly the predicate is false.
	set<int>::iterator pfsi;

	if(gb_pfcns.size() > 0)
		ret += "//\t\tUnpack partial fcns.\n";
	ret += gen_full_unpack_partial_fcn(schema, partial_fcns, gb_pfcns,
										found_cids, segen_gb_tbl, "NULL", needs_xform);

//			Unpack the group-by variables

	  for(g=0;g<gb_tbl.size();g++){
//			Find the new fields ref'd by this GBvar def.
		col_id_set new_cids;
		get_new_se_cids(gb_tbl.get_def(g), found_cids, new_cids, segen_gb_tbl);
//			Unpack these values.
		ret += gen_unpack_cids(schema, new_cids, "NULL", needs_xform);

		sprintf(tmpstr,"\tgbval->gb_var%d = %s;\n",
				g, generate_se_code(gb_tbl.get_def(g),schema).c_str() );
/*
//				There seems to be no difference between the two
//				branches of the IF statement.
		data_type *gdt = gb_tbl.get_data_type(g);
		  if(gdt->is_buffer_type()){
//				Create temporary copy.
			sprintf(tmpstr,"\tgbval->gb_var%d = %s;\n",
				g, generate_se_code(gb_tbl.get_def(g),schema).c_str() );
		  }else{
			scalarexp_t *gse = gb_tbl.get_def(g);
			sprintf(tmpstr,"\tgbval->gb_var%d = %s;\n",
					g,generate_se_code(gse,schema).c_str());
		  }
*/
		  ret.append(tmpstr);
	  }
	  ret.append("\n");


	ret+= "\treturn gbval;\n";
	ret += "};\n\n\n";



//-------------------
//		the create_group method.
//		This method creates a group in a buffer passed in
//		(to allow for creation on the stack).
//		There are also a couple of side effects:
//		1) evaluate the WHERE clause (and therefore, unpack all partial fcns)
//		2) determine if a temporal flush is required.

	ret += "bool evaluate_predicate(host_tuple &tup0, "+generate_functor_name()+"_groupdef *gbval, "+generate_functor_name()+"_statedef *stval, int cd){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

	if(partial_fcns.size()>0){		// partial fcn access failure
	  ret += "\tgs_retval_t retval = 0;\n";
	  ret += "\n";
	}

//		Start by cleaning up partial function results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";
	set<int> w_pfcns;	// partial fcns in where clause
	for(w=0;w<where.size();++w)
		collect_partial_fcns_pr(where[w]->pr, w_pfcns);

	set<int> ag_pfcns;	// partial fcns in gbdefs, aggr se's
	for(a=0;a<aggr_tbl.size();a++){
		collect_partial_fcns(aggr_tbl.get_aggr_se(a), ag_pfcns);
	}
	ret += gen_partial_fcn_dtr(partial_fcns,w_pfcns);
	ret += gen_partial_fcn_dtr(partial_fcns,ag_pfcns);

	ret+="//\t\tEvaluate clauses which don't reference stateful fcns first \n";
	for(w=0;w<where.size();++w){
		if(! pred_refs_sfun(where[w]->pr)){
			sprintf(tmpstr,"//\t\tPredicate clause %d.\n",w);
			ret += tmpstr;
//			Find the set of	variables accessed in this CNF elem,
//			but in no previous element.
			col_id_set new_cids;
			get_new_pred_cids(where[w]->pr, found_cids, new_cids, segen_gb_tbl);

//			Unpack these values.
			ret += gen_unpack_cids(schema, new_cids, "false", needs_xform);
//			Find partial fcns ref'd in this cnf element
			set<int> pfcn_refs;
			collect_partial_fcns_pr(where[w]->pr, pfcn_refs);
			ret += gen_unpack_partial_fcn(schema, partial_fcns, pfcn_refs,"false");

			ret += "\tif( !("+generate_predicate_code(where[w]->pr,schema)+
				+") ) return(false);\n";
		}
	}


//		The partial functions ref'd in the and aggregate
//		definitions must also be evaluated.  If one returns false,
//		then implicitly the predicate is false.
//		ASSUME that aggregates cannot reference stateful fcns.

	if(ag_pfcns.size() > 0)
		ret += "//\t\tUnpack remaining partial fcns.\n";
	ret += gen_full_unpack_partial_fcn(schema, partial_fcns, ag_pfcns,
										found_cids, segen_gb_tbl, "false", needs_xform);

	ret+="//\t\tEvaluate all remaining where clauses.\n";
	ret+="\tbool retval = true;\n";
	for(w=0;w<where.size();++w){
		if( pred_refs_sfun(where[w]->pr)){
			sprintf(tmpstr,"//\t\tPredicate clause %d.\n",w);
			ret += tmpstr;
//			Find the set of	variables accessed in this CNF elem,
//			but in no previous element.
			col_id_set new_cids;
			get_new_pred_cids(where[w]->pr, found_cids, new_cids, segen_gb_tbl);

//			Unpack these values.
			ret += gen_unpack_cids(schema, new_cids, "false", needs_xform);
//			Find partial fcns ref'd in this cnf element
			set<int> pfcn_refs;
			collect_partial_fcns_pr(where[w]->pr, pfcn_refs);
			ret += gen_unpack_partial_fcn(schema, partial_fcns, pfcn_refs,"false");

			ret += "\tif( !("+generate_predicate_code(where[w]->pr,schema)+
				+") ) retval = false;\n";
		}
	}

	ret+="//		Unpack all remaining attributes\n";
	ret += gen_remaining_colrefs(schema, cid_set, found_cids, "false", needs_xform);

    ret += "\n\treturn retval;\n";
	ret += "};\n\n\n";

//--------------------------------------------------------
//			Create and initialize an aggregate object

	ret += this->generate_functor_name()+"_aggrdef *create_aggregate(host_tuple &tup0, "+generate_functor_name()+"_groupdef *gbval, gs_sp_t a,"+generate_functor_name()+"_statedef *stval, int cd){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

//		return value
	ret += "\t"+generate_functor_name()+"_aggrdef *aggval = ("+generate_functor_name()+ "_aggrdef *)a;\n";

	for(a=0;a<aggr_tbl.size();a++){
		if(aggr_tbl.is_builtin(a)){
//			Create temporaries for buffer return values
		  data_type *adt = aggr_tbl.get_data_type(a);
		  if(adt->is_buffer_type()){
			sprintf(tmpstr,"aggr_tmp_%d", a);
			ret+=adt->make_host_cvar(tmpstr)+";\n";
		  }
		}
	}

	for(a=0;a<aggr_tbl.size();a++){
		sprintf(tmpstr,"aggval->aggr_var%d",a);
	  	string assignto_var = tmpstr;
	  	ret += "\t"+generate_aggr_init(assignto_var,&aggr_tbl,a, schema);
	}

	ret += "\treturn aggval;\n";
	ret += "};\n\n";


//--------------------------------------------------------
//			initialize an aggregate object inplace

	ret += "void create_aggregate(host_tuple &tup0, "+this->generate_functor_name()+"_aggrdef *aggval,"+generate_functor_name()+"_statedef *stval, int cd){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

//		return value

	for(a=0;a<aggr_tbl.size();a++){
		if(aggr_tbl.is_builtin(a)){
//			Create temporaries for buffer return values
		  data_type *adt = aggr_tbl.get_data_type(a);
		  if(adt->is_buffer_type()){
			sprintf(tmpstr,"aggr_tmp_%d", a);
			ret+=adt->make_host_cvar(tmpstr)+";\n";
		  }
		}
	}

	for(a=0;a<aggr_tbl.size();a++){
		sprintf(tmpstr,"aggval->aggr_var%d",a);
	  	string assignto_var = tmpstr;
	  	ret += "\t"+generate_aggr_init(assignto_var,&aggr_tbl,a, schema);
	}

	ret += "};\n\n";


//--------------------------------------------------------
//			Create and clean-initialize an state object

	ret += "void initialize_state(host_tuple &tup0, "+generate_functor_name()+"_groupdef *gbval, "+generate_functor_name()+"_statedef *stval){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

//		return value
//	ret += "\t"+generate_functor_name()+"_statedef *stval = ("+generate_functor_name()+ "_statedef *)s;\n";

	for(a=0;a<aggr_tbl.size();a++){
		if( aggr_tbl.is_superaggr(a)){
			if(aggr_tbl.is_builtin(a)){
//			Create temporaries for buffer return values
			  data_type *adt = aggr_tbl.get_data_type(a);
			  if(adt->is_buffer_type()){
				sprintf(tmpstr,"aggr_tmp_%d", a);
				ret+=adt->make_host_cvar(tmpstr)+";\n";
			  }
			}
		}
	}

	for(a=0;a<aggr_tbl.size();a++){
		if( aggr_tbl.is_superaggr(a)){
	  		sprintf(tmpstr,"stval->aggr_var%d",a);
	  		string assignto_var = tmpstr;
	  		ret += "\t"+generate_aggr_init(assignto_var,&aggr_tbl,a, schema);
		}
	}

	for(ssi=states_refd.begin(); ssi!=states_refd.end();++ssi){
		string state_nm = (*ssi);
		ret += "_sfun_state_clean_init_"+state_nm+"(&(stval->state_var_"+state_nm+"));\n";
	}

	ret += "};\n\n";


//--------------------------------------------------------
//			Create and dirty-initialize an state object

	ret += "void reinitialize_state(host_tuple &tup0, "+generate_functor_name()+"_groupdef *gbval, "+generate_functor_name()+"_statedef *stval, "+generate_functor_name()+"_statedef *old_stval, int cd){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

//		return value
//	ret += "\t"+generate_functor_name()+"_statedef *stval = ("+generate_functor_name()+ "_statedef *)s;\n";

	for(a=0;a<aggr_tbl.size();a++){
		if( aggr_tbl.is_superaggr(a)){
			if(aggr_tbl.is_builtin(a)){
//			Create temporaries for buffer return values
			  data_type *adt = aggr_tbl.get_data_type(a);
			  if(adt->is_buffer_type()){
				sprintf(tmpstr,"aggr_tmp_%d", a);
				ret+=adt->make_host_cvar(tmpstr)+";\n";
			  }
			}
		}
	}

//		initialize superaggregates
	for(a=0;a<aggr_tbl.size();a++){
		if( aggr_tbl.is_superaggr(a)){
	  		sprintf(tmpstr,"stval->aggr_var%d",a);
	  		string assignto_var = tmpstr;
	  		ret += "\t"+generate_aggr_init(assignto_var,&aggr_tbl,a, schema);
		}
	}

	for(ssi=states_refd.begin(); ssi!=states_refd.end();++ssi){
		string state_nm = (*ssi);
		ret += "_sfun_state_dirty_init_"+state_nm+"(&(stval->state_var_"+state_nm+"),&(old_stval->state_var_"+state_nm+"), cd );\n";
	}

	ret += "};\n\n";

//--------------------------------------------------------
//		Finalize_state : call the finalize fcn on all states


	ret += "void finalize_state( "+generate_functor_name()+"_statedef *stval, int cd){\n";

	for(ssi=states_refd.begin(); ssi!=states_refd.end();++ssi){
		string state_nm = (*ssi);
		ret += "_sfun_state_final_init_"+state_nm+"(&(stval->state_var_"+state_nm+"), cd);\n";
	}

	ret += "};\n\n";




//--------------------------------------------------------
//			update (plus) a superaggregate object

	ret += "void update_plus_superaggr(host_tuple &tup0, " +
		generate_functor_name()+"_groupdef *gbval, "+
		generate_functor_name()+"_statedef *stval){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

//			use of temporaries depends on the aggregate,
//			generate them in generate_aggr_update


	for(a=0;a<aggr_tbl.size();a++){
	  if(aggr_tbl.is_superaggr(a)){
	  	sprintf(tmpstr,"stval->aggr_var%d",a);
	  	string varname = tmpstr;
	  	ret.append(generate_aggr_update(varname,&aggr_tbl,a, schema));
	  }
	}

	ret += "\treturn;\n";
	ret += "};\n";



//--------------------------------------------------------
//			update (minus) a superaggregate object

	ret += "void update_minus_superaggr( "+
		generate_functor_name()+"_groupdef *gbval, "+
		generate_functor_name()+"_aggrdef *aggval,"+
		generate_functor_name()+"_statedef *stval"+
		"){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

//			use of temporaries depends on the aggregate,
//			generate them in generate_aggr_update


	for(a=0;a<aggr_tbl.size();a++){
	  if(aggr_tbl.is_superaggr(a)){
	  	sprintf(tmpstr,"stval->aggr_var%d",a);
	  	string super_varname = tmpstr;
	  	sprintf(tmpstr,"aggval->aggr_var%d",a);
	  	string sub_varname = tmpstr;
	  	ret.append(generate_superaggr_minus(sub_varname, super_varname,&aggr_tbl,a, schema));
	  }
	}

	ret += "\treturn;\n";
	ret += "};\n";


//--------------------------------------------------------
//			update an aggregate object

	ret += "void update_aggregate(host_tuple &tup0, "
		+generate_functor_name()+"_groupdef *gbval, "+
		generate_functor_name()+"_aggrdef *aggval,"+generate_functor_name()+"_statedef *stval, int cd){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

//			use of temporaries depends on the aggregate,
//			generate them in generate_aggr_update


	for(a=0;a<aggr_tbl.size();a++){
	  sprintf(tmpstr,"aggval->aggr_var%d",a);
	  string varname = tmpstr;
	  ret.append(generate_aggr_update(varname,&aggr_tbl,a, schema));
	}

	ret += "\treturn;\n";
	ret += "};\n";

//---------------------------------------------------
//			Flush test

	ret += "\tbool flush_needed(){\n";
	if(uses_temporal_flush){
		ret += "\t\treturn needs_temporal_flush;\n";
	}else{
		ret += "\t\treturn false;\n";
	}
	ret += "\t};\n";


//------------------------------------------------------
//			THe cleaning_when predicate

	string gbvar = "gbval->gb_var";
	string aggvar = "aggval->";

	ret += "bool need_to_clean( "
		+generate_functor_name()+"_groupdef *gbval, "+
		generate_functor_name()+"_statedef *stval, int cd"+
		"){\n";

	if(cleanwhen.size()>0)
		ret += "\tbool predval = true;\n";
	else
		ret += "\tbool predval = false;\n";

//			Find the udafs ref'd in the having clause
	set<int> cw_aggs;
	for(w=0;w<cleanwhen.size();++w)
		collect_aggr_refs_pr(cleanwhen[w]->pr, cw_aggs);


//			get the return values from the UDAFS
   	for(a=0;a<aggr_tbl.size();a++){
		if(! aggr_tbl.is_builtin(a) && cw_aggs.count(a)){
			ret += "\t"+aggr_tbl.get_op(a)+"_HFTA_AGGR_OUTPUT_(&(udaf_ret_"+int_to_string(a)+"),";
			if(aggr_tbl.get_storage_type(a)->get_type() != fstring_t) ret+="&";
			ret+="("+aggvar+"aggr_var"+int_to_string(a)+"));\n";
		}
	}


//		Start by cleaning up partial function results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";
	set<int> cw_pfcns;	// partial fcns in where clause
	for(w=0;w<cleanwhen.size();++w)
		collect_partial_fcns_pr(cleanwhen[w]->pr, cw_pfcns);

	ret += gen_partial_fcn_dtr(partial_fcns,cw_pfcns);


	for(w=0;w<cleanwhen.size();++w){
		sprintf(tmpstr,"//\t\tPredicate clause %d.\n",w);
		ret += tmpstr;
//			Find partial fcns ref'd in this cnf element
		set<int> pfcn_refs;
		collect_partial_fcns_pr(cleanwhen[w]->pr, pfcn_refs);
		for(pfsi=pfcn_refs.begin();pfsi!=pfcn_refs.end();++pfsi){
			ret += unpack_partial_fcn_fm_aggr(partial_fcns[(*pfsi)], (*pfsi), gbvar, aggvar, schema);
			ret += "\tif(retval){ return false;}\n";
		}
//		ret += unpack_partial_fcn_fm_aggr(schema, partial_fcns, pfcn_refs,"false");

		ret += "\tif( !("+generate_predicate_code_fm_aggr(cleanwhen[w]->pr,gbvar, aggvar, schema)+
				") ) predval = false;\n";
	}

	ret += "\treturn predval;\n";
	ret += "\t};\n";

//------------------------------------------------------
//			THe cleaning_by predicate

	ret += "bool sample_group("
		+generate_functor_name()+"_groupdef *gbval, "+
		generate_functor_name()+"_aggrdef *aggval,"+
		generate_functor_name()+"_statedef *stval, int cd"+
		"){\n";

	if(cleanby.size()>0)
		ret += "\tbool retval = true;\n";
	else
		ret += "\tbool retval = false;\n";

//			Find the udafs ref'd in the having clause
	set<int> cb_aggs;
	for(w=0;w<cleanby.size();++w)
		collect_aggr_refs_pr(cleanby[w]->pr, cb_aggs);


//			get the return values from the UDAFS
   	for(a=0;a<aggr_tbl.size();a++){
		if(! aggr_tbl.is_builtin(a) && cb_aggs.count(a)){
			ret += "\t"+aggr_tbl.get_op(a)+"_HFTA_AGGR_OUTPUT_(&(udaf_ret_"+int_to_string(a)+"),";
			if(aggr_tbl.get_storage_type(a)->get_type() != fstring_t) ret+="&";
			ret+="("+aggvar+"aggr_var"+int_to_string(a)+"));\n";
		}
	}


//		Start by cleaning up partial function results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";
	set<int> cb_pfcns;	// partial fcns in where clause
	for(w=0;w<cleanby.size();++w)
		collect_partial_fcns_pr(cleanby[w]->pr, cb_pfcns);

	ret += gen_partial_fcn_dtr(partial_fcns,cb_pfcns);


	for(w=0;w<cleanwhen.size();++w){
		sprintf(tmpstr,"//\t\tPredicate clause %d.\n",w);
		ret += tmpstr;

/*
//			Find the set of	variables accessed in this CNF elem,
//			but in no previous element.
		col_id_set new_cids;
		get_new_pred_cids(cleanby[w]->pr, found_cids, new_cids, segen_gb_tbl);

//			Unpack these values.
		ret += gen_unpack_cids(schema, new_cids, "false", needs_xform);
*/

//			Find partial fcns ref'd in this cnf element
		set<int> pfcn_refs;
		collect_partial_fcns_pr(cleanby[w]->pr, pfcn_refs);
		for(pfsi=pfcn_refs.begin();pfsi!=pfcn_refs.end();++pfsi){
			ret += unpack_partial_fcn_fm_aggr(partial_fcns[(*pfsi)], (*pfsi), gbvar, aggvar, schema);
			ret += "\tif(retval){ return false;}\n";
		}
//		ret += gen_unpack_partial_fcn(schema, partial_fcns, pfcn_refs,"false");

		ret += "\tif( !("+generate_predicate_code_fm_aggr(cleanby[w]->pr,gbvar, aggvar, schema)+
			+") ) retval = false;\n";
	}

	ret += "\treturn retval;\n";
	ret += "\t};\n";


//-----------------------------------------------------
//
	ret += "bool final_sample_group("
		+generate_functor_name()+"_groupdef *gbval, "+
		generate_functor_name()+"_aggrdef *aggval,"+
		generate_functor_name()+"_statedef *stval,"+
		"int cd){\n";

	ret += "\tgs_retval_t retval = 0;\n";

//			Find the udafs ref'd in the having clause
	set<int> hv_aggs;
	for(w=0;w<having.size();++w)
		collect_aggr_refs_pr(having[w]->pr, hv_aggs);


//			get the return values from the UDAFS
   	for(a=0;a<aggr_tbl.size();a++){
		if(! aggr_tbl.is_builtin(a) && hv_aggs.count(a)){
			ret += "\t"+aggr_tbl.get_op(a)+"_HFTA_AGGR_OUTPUT_(&(udaf_ret_"+int_to_string(a)+"),";
			if(aggr_tbl.get_storage_type(a)->get_type() != fstring_t) ret+="&";
			ret+="("+aggvar+"aggr_var"+int_to_string(a)+"));\n";
		}
	}


	set<int> hv_sl_pfcns;
	for(w=0;w<having.size();w++){
		collect_partial_fcns_pr(having[w]->pr, hv_sl_pfcns);
	}

//		clean up the partial fcn results from any previous execution
	ret += gen_partial_fcn_dtr(partial_fcns,hv_sl_pfcns);

//		Unpack them now
	for(pfsi=hv_sl_pfcns.begin();pfsi!=hv_sl_pfcns.end();++pfsi){
		ret += unpack_partial_fcn_fm_aggr(partial_fcns[(*pfsi)], (*pfsi), gbvar, aggvar, schema);
		ret += "\tif(retval){ return false;}\n";
	}

//		Evalaute the HAVING clause
//		TODO: this seems to have a ++ operator rather than a + operator.
	for(w=0;w<having.size();++w){
		ret += "\tif( !("+generate_predicate_code_fm_aggr(having[w]->pr,gbvar, aggvar, schema) +") ) { return false;}\n";
	}

	ret += "\treturn true;\n";
	ret+="}\n\n";

//---------------------------------------------------
//			create output tuple
//			Unpack the partial functions ref'd in the where clause,
//			select clause.  Evaluate the where clause.
//			Finally, pack the tuple.

//			I need to use special code generation here,
//			so I'll leave it in longhand.

	ret += "host_tuple create_output_tuple("
		+generate_functor_name()+"_groupdef *gbval, "+
		generate_functor_name()+"_aggrdef *aggval,"+
		generate_functor_name()+"_statedef *stval,"+
		"int cd, bool &failed){\n";

	ret += "\thost_tuple tup;\n";
	ret += "\tfailed = false;\n";
	ret += "\tgs_retval_t retval = 0;\n";


//			Find the udafs ref'd in the select clause
	set<int> sl_aggs;
	for(s=0;s<select_list.size();s++)
		collect_agg_refs(select_list[s]->se, sl_aggs);


//			get the return values from the UDAFS
   	for(a=0;a<aggr_tbl.size();a++){
		if(! aggr_tbl.is_builtin(a) && sl_aggs.count(a)){
			ret += "\t"+aggr_tbl.get_op(a)+"_HFTA_AGGR_OUTPUT_(&(udaf_ret_"+int_to_string(a)+"),";
			if(aggr_tbl.get_storage_type(a)->get_type() != fstring_t) ret+="&";
			ret+="("+aggvar+"aggr_var"+int_to_string(a)+"));\n";
		}
	}


//			I can't cache partial fcn results from the having
//			clause because evaluation is separated.
	set<int> sl_pfcns;
	for(s=0;s<select_list.size();s++){
		collect_partial_fcns(select_list[s]->se, sl_pfcns);
	}
//		Unpack them now
	for(pfsi=sl_pfcns.begin();pfsi!=sl_pfcns.end();++pfsi){
		ret += unpack_partial_fcn_fm_aggr(partial_fcns[(*pfsi)], (*pfsi), gbvar, aggvar, schema);
		ret += "\tif(retval){ failed=true; return tup;}\n";
	}


//          Now, compute the size of the tuple.

//          Unpack any BUFFER type selections into temporaries
//          so that I can compute their size and not have
//          to recompute their value during tuple packing.
//          I can use regular assignment here because
//          these temporaries are non-persistent.
//			TODO: should I be using the selvar generation routine?

	ret += "//\t\tCompute the size of the tuple.\n";
	ret += "//\t\t\tNote: buffer storage packed at the end of the tuple.\n";
      for(s=0;s<select_list.size();s++){
		scalarexp_t *se = select_list[s]->se;
        data_type *sdt = se->get_data_type();
        if(sdt->is_buffer_type() &&
			 !( (se->get_operator_type() == SE_COLREF) ||
				(se->get_operator_type() == SE_AGGR_STAR) ||
				(se->get_operator_type() == SE_AGGR_SE) ||
			   (se->get_operator_type() == SE_FUNC && se->is_partial()) ||
			   (se->get_operator_type() == SE_FUNC && se->get_aggr_ref() >= 0))
		){
            sprintf(tmpstr,"selvar_%d",s);
			ret+="\t"+sdt->make_host_cvar(tmpstr)+" = ";
			ret += generate_se_code_fm_aggr(se,gbvar, aggvar, schema) +";\n";
        }
      }

//      The size of the tuple is the size of the tuple struct plus the
//      size of the buffers to be copied in.

      ret+="\ttup.tuple_size = sizeof(" + generate_tuple_name( this->get_node_name()) +") + sizeof(gs_uint8_t)";
      for(s=0;s<select_list.size();s++){
//		if(s>0) ret += "+";
		scalarexp_t *se = select_list[s]->se;
        data_type *sdt = select_list[s]->se->get_data_type();
        if(sdt->is_buffer_type()){
		  if(!( (se->get_operator_type() == SE_COLREF) ||
				(se->get_operator_type() == SE_AGGR_STAR) ||
				(se->get_operator_type() == SE_AGGR_SE) ||
			   (se->get_operator_type() == SE_FUNC && se->is_partial()) ||
			   (se->get_operator_type() == SE_FUNC && se->get_aggr_ref() >= 0))
		  ){
            sprintf(tmpstr," + %s(&selvar_%d)", sdt->get_hfta_buffer_size().c_str(),s);
            ret.append(tmpstr);
		  }else{
            sprintf(tmpstr," + %s(&%s)", sdt->get_hfta_buffer_size().c_str(),generate_se_code_fm_aggr(se,gbvar, aggvar, schema).c_str());
            ret.append(tmpstr);
		  }
        }
      }
      ret.append(";\n");

//		Allocate tuple data block.
	ret += "//\t\tCreate the tuple block.\n";
	  ret += "\ttup.data = malloc(tup.tuple_size);\n";
	  ret += "\ttup.heap_resident = true;\n";

//		Mark tuple as regular
	  ret += "\t*((gs_sp_t )tup.data + sizeof(" + generate_tuple_name( this->get_node_name()) +")) = REGULAR_TUPLE;\n";

//	  ret += "\ttup.channel = 0;\n";
	  ret += "\t"+generate_tuple_name( this->get_node_name())+" *tuple = ("+
				generate_tuple_name( this->get_node_name())+" *)(tup.data);\n";

//		Start packing.
//			(Here, offsets are hard-wired.  is this a problem?)

	ret += "//\t\tPack the fields into the tuple.\n";
	  ret += "\tint tuple_pos = sizeof("+generate_tuple_name( this->get_node_name()) +") + sizeof(gs_uint8_t);\n";
      for(s=0;s<select_list.size();s++){
		scalarexp_t *se = select_list[s]->se;
        data_type *sdt = se->get_data_type();
        if(sdt->is_buffer_type()){
		  if(!( (se->get_operator_type() == SE_COLREF) ||
				(se->get_operator_type() == SE_AGGR_STAR) ||
				(se->get_operator_type() == SE_AGGR_SE) ||
			   (se->get_operator_type() == SE_FUNC && se->is_partial()) ||
			   (se->get_operator_type() == SE_FUNC && se->get_aggr_ref() >= 0))
		  ){
            sprintf(tmpstr,"\t%s(&(tuple->tuple_var%d), &selvar_%d,  ((gs_sp_t )tuple)+tuple_pos, tuple_pos);\n", sdt->get_hfta_buffer_tuple_copy().c_str(),s, s);
            ret.append(tmpstr);
            sprintf(tmpstr,"\ttuple_pos += %s(&selvar_%d);\n", sdt->get_hfta_buffer_size().c_str(), s);
            ret.append(tmpstr);
		  }else{
            sprintf(tmpstr,"\t%s(&(tuple->tuple_var%d), &%s,  ((gs_sp_t )tuple)+tuple_pos, tuple_pos);\n", sdt->get_hfta_buffer_tuple_copy().c_str(),s, generate_se_code_fm_aggr(se,gbvar, aggvar, schema).c_str());
            ret.append(tmpstr);
            sprintf(tmpstr,"\ttuple_pos += %s(&%s);\n", sdt->get_hfta_buffer_size().c_str(), generate_se_code_fm_aggr(se,gbvar, aggvar, schema).c_str());
            ret.append(tmpstr);
		  }
        }else{
            sprintf(tmpstr,"\ttuple->tuple_var%d = ",s);
            ret.append(tmpstr);
            ret.append(generate_se_code_fm_aggr(se,gbvar, aggvar, schema) );
            ret.append(";\n");
        }
      }

//			Destroy string temporaries
	  ret += gen_buffer_selvars_dtr(select_list);
//			Destroy string return vals of UDAFs
   	for(a=0;a<aggr_tbl.size();a++){
		if(! aggr_tbl.is_builtin(a)){
			int afcn_id = aggr_tbl.get_fcn_id(a);
			data_type *adt = Ext_fcns->get_fcn_dt(afcn_id);
			if(adt->is_buffer_type()){
				sprintf(tmpstr,"\t%s(&udaf_ret_%d);\n",
			  	adt->get_hfta_buffer_destroy().c_str(), a );
				ret += tmpstr;
			}
		}
	}


	  ret += "\treturn tup;\n";
	  ret += "};\n";


//-------------------------------------------------------------------
//		Temporal update functions

	ret+="bool temp_status_received(){return temp_tuple_received;};\n\n";

//		create a temp status tuple
	ret += "int create_temp_status_tuple(host_tuple& result, bool flush_finished) {\n\n";

	ret += gen_init_temp_status_tuple(this->get_node_name());

//		Start packing.
//			(Here, offsets are hard-wired.  is this a problem?)

	ret += "//\t\tPack the fields into the tuple.\n";
	for(s=0;s<select_list.size();s++){
		data_type *sdt = select_list[s]->se->get_data_type();
		if(sdt->is_temporal()){
			sprintf(tmpstr,"\ttuple->tuple_var%d = ",s);
	  		ret += tmpstr;
			sprintf(tmpstr,"(flush_finished) ? %s : %s ", generate_se_code(select_list[s]->se,schema).c_str(), generate_se_code_fm_aggr(select_list[s]->se,"last_flushed_gb", "", schema).c_str());
			ret += tmpstr;
	  		ret += ";\n";
		}
	}

	ret += "\treturn 0;\n";
	ret += "};};\n\n\n";


//----------------------------------------------------------
//			The hash function

	ret += "struct "+generate_functor_name()+"_hash_func{\n";
	ret += "\tgs_uint32_t operator()(const "+generate_functor_name()+
				"_groupdef *grp) const{\n";
	ret += "\t\treturn(";
	for(g=0;g<gb_tbl.size();g++){
		if(g>0) ret += "^";
		data_type *gdt = gb_tbl.get_data_type(g);
		if(gdt->use_hashfunc()){
			if(gdt->is_buffer_type())
				sprintf(tmpstr,"(%s*%s(&)grp->gb_var%d)))",hash_nums[g%NRANDS].c_str(),gdt->get_hfta_hashfunc().c_str(),g);
			else
				sprintf(tmpstr,"(%s*%s(grp->gb_var%d))",hash_nums[g%NRANDS].c_str(),gdt->get_hfta_hashfunc().c_str(),g);
		}else{
			sprintf(tmpstr,"(%s*grp->gb_var%d)",hash_nums[g%NRANDS].c_str(),g);
		}
		ret += tmpstr;
	}
	ret += ") >> 32);\n";
	ret += "\t}\n";
	ret += "};\n\n";

//----------------------------------------------------------
//			The superhash function

	ret += "struct "+generate_functor_name()+"_superhash_func{\n";
	ret += "\tgs_uint32_t operator()(const "+generate_functor_name()+
				"_groupdef *grp) const{\n";
	ret += "\t\treturn(0";

	for(g=0;g<gb_tbl.size();g++){
		if(sg_tbl.count(g)>0){
			ret += "^";
			data_type *gdt = gb_tbl.get_data_type(g);
			if(gdt->use_hashfunc()){
				sprintf(tmpstr,"(%s*%s(&(grp->gb_var%d)))",hash_nums[g%NRANDS].c_str(),gdt->get_hfta_hashfunc().c_str(),g);
			}else{
				sprintf(tmpstr,"(%s*grp->gb_var%d)",hash_nums[g%NRANDS].c_str(),g);
			}
			ret += tmpstr;
		}
	}
	ret += ") >> 32);\n";

	ret += "\t}\n";
	ret += "};\n\n";

//----------------------------------------------------------
//			The comparison function

	ret += "struct "+generate_functor_name()+"_equal_func{\n";
	ret += "\tbool operator()(const "+generate_functor_name()+"_groupdef *grp1, "+
			generate_functor_name()+"_groupdef *grp2) const{\n";
	ret += "\t\treturn( (";
	for(g=0;g<gb_tbl.size();g++){
		if(g>0) ret += ") && (";
		data_type *gdt = gb_tbl.get_data_type(g);
		if(gdt->complex_comparison(gdt)){
		  if(gdt->is_buffer_type())
			sprintf(tmpstr,"(%s(&(grp1->gb_var%d), &(grp2->gb_var%d))==0)",
				gdt->get_hfta_comparison_fcn(gdt).c_str(),g,g);
		  else
			sprintf(tmpstr,"(%s((grp1->gb_var%d), (grp2->gb_var%d))==0)",
				gdt->get_hfta_comparison_fcn(gdt).c_str(),g,g);
		}else{
			sprintf(tmpstr,"grp1->gb_var%d == grp2->gb_var%d",g,g);
		}
		ret += tmpstr;
	}
	ret += ") );\n";
	ret += "\t}\n";
	ret += "};\n\n";


//----------------------------------------------------------
//			The superhashcomparison function

	ret += "struct "+generate_functor_name()+"_superequal_func{\n";
	ret += "\tbool operator()(const "+generate_functor_name()+"_groupdef *grp1, "+
			generate_functor_name()+"_groupdef *grp2) const{\n";
	ret += "\t\treturn( (";
    if(sg_tbl.size()){
		bool first_elem = true;
		for(g=0;g<gb_tbl.size();g++){
			if(sg_tbl.count(g)){
				if(first_elem) first_elem=false; else ret += ") && (";
				data_type *gdt = gb_tbl.get_data_type(g);
				if(gdt->complex_comparison(gdt)){
				  if(gdt->is_buffer_type())
					sprintf(tmpstr,"(%s(&(grp1->gb_var%d), &(grp2->gb_var%d))==0)",
						gdt->get_hfta_comparison_fcn(gdt).c_str(),g,g);
				  else
					sprintf(tmpstr,"(%s((grp1->gb_var%d), (grp2->gb_var%d))==0)",
						gdt->get_hfta_comparison_fcn(gdt).c_str(),g,g);
				}else{
					sprintf(tmpstr,"grp1->gb_var%d == grp2->gb_var%d",g,g);
				}
			ret += tmpstr;
			}
		}
	}else{
		ret += "true";
	}

	ret += ") );\n";
	ret += "\t}\n";


	ret += "};\n\n";
	return(ret);
}

string sgahcwcb_qpn::generate_operator(int i, string params){

		return(
			"	clean_operator<" +
			generate_functor_name()+",\n\t"+
			generate_functor_name() + "_groupdef, \n\t" +
			generate_functor_name() + "_aggrdef, \n\t" +
			generate_functor_name() + "_statedef, \n\t" +
			generate_functor_name()+"_hash_func, \n\t"+
			generate_functor_name()+"_equal_func ,\n\t"+
			generate_functor_name()+"_superhash_func,\n\t "+
			generate_functor_name()+"_superequal_func \n\t"+
			"> *op"+int_to_string(i)+" = new clean_operator<"+
			generate_functor_name()+",\n\t"+
			generate_functor_name() + "_groupdef,\n\t " +
			generate_functor_name() + "_aggrdef, \n\t" +
			generate_functor_name() + "_statedef, \n\t" +
			generate_functor_name()+"_hash_func, \n\t"+
			generate_functor_name()+"_equal_func, \n\t"+
			generate_functor_name()+"_superhash_func, \n\t"+
			generate_functor_name()+"_superequal_func\n\t "
			">("+params+", \"" + get_node_name() + "\");\n"
		);
}

////////////////////////////////////////////////////////////////
////	RSGAH functor



string rsgah_qpn::generate_functor_name(){
	return("rsgah_functor_" + normalize_name(this->get_node_name()));
}


string rsgah_qpn::generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, vector<bool> &needs_xform){
	int a,g,w,s;


//			Initialize generate utility globals
	segen_gb_tbl = &(gb_tbl);


//--------------------------------
//			group definition class
	string ret = "class " + generate_functor_name() + "_groupdef{\n";
	ret += "public:\n";
	for(g=0;g<this->gb_tbl.size();g++){
		sprintf(tmpstr,"gb_var%d",g);
		ret+="\t"+this->gb_tbl.get_data_type(g)->make_host_cvar(tmpstr)+";\n";
	}
//		Constructors
	ret += "\t"+generate_functor_name() + "_groupdef(){};\n";
	ret += "\t"+generate_functor_name() + "_groupdef("+
		this->generate_functor_name() + "_groupdef *gd){\n";
	for(g=0;g<gb_tbl.size();g++){
		data_type *gdt = gb_tbl.get_data_type(g);
		if(gdt->is_buffer_type()){
			sprintf(tmpstr,"\t\t%s(&gb_var%d, &(gd->gb_var%d));\n",
			  gdt->get_hfta_buffer_assign_copy().c_str(),g,g );
			ret += tmpstr;
		}else{
			sprintf(tmpstr,"\t\tgb_var%d = gd->gb_var%d;\n",g,g);
			ret += tmpstr;
		}
	}
	ret += "\t};\n";
//		destructor
	ret += "\t~"+ generate_functor_name() + "_groupdef(){\n";
	for(g=0;g<gb_tbl.size();g++){
		data_type *gdt = gb_tbl.get_data_type(g);
		if(gdt->is_buffer_type()){
			sprintf(tmpstr,"\t\t%s(&gb_var%d);\n",
			  gdt->get_hfta_buffer_destroy().c_str(), g );
			ret += tmpstr;
		}
	}
	ret += "\t};\n";
	ret +="};\n\n";

//--------------------------------
//			aggr definition class
	ret += "class " + this->generate_functor_name() + "_aggrdef{\n";
	ret += "public:\n";
	for(a=0;a<aggr_tbl.size();a++){
aggr_table_entry *ate = aggr_tbl.agr_tbl[a];
		sprintf(tmpstr,"aggr_var%d",a);
		if(aggr_tbl.is_builtin(a))
		  ret+="\t"+ aggr_tbl.get_data_type(a)->make_host_cvar(tmpstr)+";\n";
		else
		  ret+="\t"+ aggr_tbl.get_storage_type(a)->make_host_cvar(tmpstr)+";\n";
	}
//		Constructors
	ret += "\t"+this->generate_functor_name() + "_aggrdef(){};\n";
//		destructor
	ret += "\t~"+this->generate_functor_name() + "_aggrdef(){\n";
	for(a=0;a<aggr_tbl.size();a++){
		if(aggr_tbl.is_builtin(a)){
			data_type *adt = aggr_tbl.get_data_type(a);
			if(adt->is_buffer_type()){
				sprintf(tmpstr,"\t\t%s(&aggr_var%d);\n",
			  	adt->get_hfta_buffer_destroy().c_str(), a );
				ret += tmpstr;
			}
		}else{
			ret+="\t\t"+aggr_tbl.get_op(a)+"_HFTA_AGGR_DESTROY_(";
			if(aggr_tbl.get_storage_type(a)->get_type() != fstring_t) ret+="&";
			ret+="(aggr_var"+int_to_string(a)+"));\n";
		}
	}
	ret += "\t};\n";
	ret +="};\n\n";

//--------------------------------
//			gb functor class
	ret += "class " + this->generate_functor_name() + "{\n";

//			Find variables referenced in this query node.

  col_id_set cid_set;
  col_id_set::iterator csi;

    for(w=0;w<where.size();++w)
    	gather_pr_col_ids(where[w]->pr,cid_set,segen_gb_tbl);
    for(w=0;w<having.size();++w)
    	gather_pr_col_ids(having[w]->pr,cid_set,segen_gb_tbl);
    for(w=0;w<closing_when.size();++w)
    	gather_pr_col_ids(closing_when[w]->pr,cid_set,segen_gb_tbl);
	for(g=0;g<gb_tbl.size();g++)
		gather_se_col_ids(gb_tbl.get_def(g),cid_set,segen_gb_tbl);

    for(s=0;s<select_list.size();s++){
    	gather_se_col_ids(select_list[s]->se,cid_set,segen_gb_tbl);	// descends into aggregates
    }


//			Private variables : store the state of the functor.
//			1) variables for unpacked attributes
//			2) offsets of the upacked attributes
//			3) storage of partial functions
//			4) storage of complex literals (i.e., require a constructor)

	ret += "private:\n";

	// var to save the schema handle
	ret += "\tint schema_handle0;\n";

	// generate the declaration of all the variables related to
	// temp tuples generation
	ret += gen_decl_temp_vars();

//			unpacked attribute storage, offsets
	ret += "//\t\tstorage and offsets of accessed fields.\n";
	ret += generate_access_vars(cid_set, schema);
//			tuple metadata offset
	ret += "\tint tuple_metadata_offset0;\n";

//			Variables to store results of partial functions.
//			WARNING find_partial_functions modifies the SE
//			(it marks the partial function id).
	ret += "//\t\tParital function result storage\n";
	vector<scalarexp_t *> partial_fcns;
	vector<int> fcn_ref_cnt;
	vector<bool> is_partial_fcn;
	for(s=0;s<select_list.size();s++){
		find_partial_fcns(select_list[s]->se, &partial_fcns, NULL,NULL, Ext_fcns);
	}
	for(w=0;w<where.size();w++){
		find_partial_fcns_pr(where[w]->pr, &partial_fcns, NULL,NULL, Ext_fcns);
	}
	for(w=0;w<having.size();w++){
		find_partial_fcns_pr(having[w]->pr, &partial_fcns, NULL,NULL, Ext_fcns);
	}
	for(w=0;w<closing_when.size();w++){
		find_partial_fcns_pr(closing_when[w]->pr, &partial_fcns, NULL,NULL, Ext_fcns);
	}
	for(g=0;g<gb_tbl.size();g++){
		find_partial_fcns(gb_tbl.get_def(g), &partial_fcns, NULL,NULL, Ext_fcns);
	}
	for(a=0;a<aggr_tbl.size();a++){
		find_partial_fcns(aggr_tbl.get_aggr_se(a), &partial_fcns, NULL,NULL, Ext_fcns);
	}
	if(partial_fcns.size()>0){
	  ret += "/*\t\tVariables for storing results of partial functions. \t*/\n";
	  ret += generate_partial_fcn_vars(partial_fcns,fcn_ref_cnt,is_partial_fcn,false);
	}

//			Create cached temporaries for UDAF return values.
   	for(a=0;a<aggr_tbl.size();a++){
		if(! aggr_tbl.is_builtin(a)){
			int afcn_id = aggr_tbl.get_fcn_id(a);
			data_type *adt = Ext_fcns->get_fcn_dt(afcn_id);
			sprintf(tmpstr,"udaf_ret_%d", a);
			ret+="\t"+adt->make_host_cvar(tmpstr)+";\n";
		}
	}


//			Complex literals (i.e., they need constructors)
	ret += "//\t\tComplex literal storage.\n";
	cplx_lit_table *complex_literals = this->get_cplx_lit_tbl(Ext_fcns);
	ret += generate_complex_lit_vars(complex_literals);

//			Pass-by-handle parameters
	ret += "//\t\tPass-by-handle storage.\n";
	vector<handle_param_tbl_entry *> param_handle_table = this->get_handle_param_tbl(Ext_fcns);
	ret += generate_pass_by_handle_vars(param_handle_table);


//			variables to hold parameters.
	ret += "//\tfor query parameters\n";
	ret += generate_param_vars(param_tbl);

//		Is there a temporal flush?  If so create flush temporaries,
//		create flush indicator.
	bool uses_temporal_flush = false;
	for(g=0;g<gb_tbl.size();g++){
		data_type *gdt = gb_tbl.get_data_type(g);
		if(gdt->is_temporal())
			uses_temporal_flush = true;
	}

	if(uses_temporal_flush){
		ret += "//\t\tFor temporal flush\n";
		for(g=0;g<gb_tbl.size();g++){
			data_type *gdt = gb_tbl.get_data_type(g);
			if(gdt->is_temporal()){
			  sprintf(tmpstr,"last_gb%d",g);
			  ret+="\t"+gb_tbl.get_data_type(g)->make_host_cvar(tmpstr)+";\n";
			  sprintf(tmpstr,"last_flushed_gb%d",g);
			  ret+="\t"+gb_tbl.get_data_type(g)->make_host_cvar(tmpstr)+";\n";
			}
		}
		ret += "\tbool needs_temporal_flush;\n";
	}

//			The publicly exposed functions

	ret += "\npublic:\n";


//-------------------
//			The functor constructor
//			pass in the schema handle.
//			1) make assignments to the unpack offset variables
//			2) initialize the complex literals

	ret += "//\t\tFunctor constructor.\n";
	ret +=  this->generate_functor_name()+"(int schema_handle0){\n";

	// save the schema handle
	ret += "\t\tthis->schema_handle0 = schema_handle0;\n";
//		metadata offset
	ret += "\ttuple_metadata_offset0 = ftaschema_get_tuple_metadata_offset(schema_handle0);\n";

//		unpack vars
	ret += "//\t\tGet offsets for unpacking fields from input tuple.\n";
	ret += gen_access_var_init(cid_set);

//		complex literals
	ret += "//\t\tInitialize complex literals.\n";
	ret += gen_complex_lit_init(complex_literals);

//		Initialize partial function results so they can be safely GC'd
	ret += gen_partial_fcn_init(partial_fcns);

//		Initialize non-query-parameter parameter handles
	ret += gen_pass_by_handle_init(param_handle_table);

//		temporal flush variables
//		ASSUME that structured values won't be temporal.
	if(uses_temporal_flush){
		ret += "//\t\tInitialize temporal flush variables.\n";
		for(g=0;g<gb_tbl.size();g++){
			data_type *gdt = gb_tbl.get_data_type(g);
			if(gdt->is_temporal()){
				literal_t gl(gdt->type_indicator());
				sprintf(tmpstr,"\tlast_gb%d = %s;\n",g, gl.to_hfta_C_code("").c_str());
				ret.append(tmpstr);
			}
		}
		ret += "\tneeds_temporal_flush = false;\n";
	}

	//		Init temporal attributes referenced in select list
	ret += gen_init_temp_vars(schema, select_list, segen_gb_tbl);

	ret += "};\n";


//-------------------
//			Functor destructor
	ret += "//\t\tFunctor destructor.\n";
	ret +=  "~"+this->generate_functor_name()+"(){\n";

//			clean up buffer type complex literals
	ret += gen_complex_lit_dtr(complex_literals);

//			Deregister the pass-by-handle parameters
	ret += "/* register and de-register the pass-by-handle parameters */\n";
	ret += gen_pass_by_handle_dtr(param_handle_table);

//			clean up partial function results.
	ret += "/* clean up partial function storage	*/\n";
	ret += gen_partial_fcn_dtr(partial_fcns);

//			Destroy the parameters, if any need to be destroyed
	ret += "\tthis->destroy_params_"+this->generate_functor_name()+"();\n";

	ret += "};\n\n";


//-------------------
//			Parameter manipulation routines
	ret += generate_load_param_block(this->generate_functor_name(),
									this->param_tbl,param_handle_table);
	ret += generate_delete_param_block(this->generate_functor_name(),
									this->param_tbl,param_handle_table);

//-------------------
//			Register new parameter block

	ret += "int set_param_block(gs_int32_t sz, void* value){\n";
	  ret += "\tthis->destroy_params_"+this->generate_functor_name()+"();\n";
	  ret += "\treturn this->load_params_"+this->generate_functor_name()+
				"(sz, value);\n";
	ret += "};\n\n";


//-------------------
//		the create_group method.
//		This method creates a group in a buffer passed in
//		(to allow for creation on the stack).
//		There are also a couple of side effects:
//		1) evaluate the WHERE clause (and therefore, unpack all partial fcns)
//		2) determine if a temporal flush is required.

	ret += this->generate_functor_name()+"_groupdef *create_group(host_tuple &tup0, gs_sp_t buffer){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

	if(partial_fcns.size()>0){		// partial fcn access failure
	  ret += "\tgs_retval_t retval = 0;\n";
	  ret += "\n";
	}
//		return value
	ret += "\t"+generate_functor_name()+"_groupdef *gbval = ("+generate_functor_name()+
			"_groupdef *) buffer;\n";

//		Start by cleaning up partial function results
	ret += "//\t\tcall destructors for partial fcn storage vars of buffer type\n";
	set<int> w_pfcns;	// partial fcns in where clause
	for(w=0;w<where.size();++w)
		collect_partial_fcns_pr(where[w]->pr, w_pfcns);

	set<int> ag_gb_pfcns;	// partial fcns in gbdefs, aggr se's
	for(g=0;g<gb_tbl.size();g++){
		collect_partial_fcns(gb_tbl.get_def(g), ag_gb_pfcns);
	}
	for(a=0;a<aggr_tbl.size();a++){
		collect_partial_fcns(aggr_tbl.get_aggr_se(a), ag_gb_pfcns);
	}
	ret += gen_partial_fcn_dtr(partial_fcns,w_pfcns);
	ret += gen_partial_fcn_dtr(partial_fcns,ag_gb_pfcns);
//	ret += gen_partial_fcn_dtr(partial_fcns);


	ret += gen_temp_tuple_check(this->node_name, 0);
	col_id_set found_cids;	// colrefs unpacked thus far.
	ret += gen_unpack_temp_vars(schema, found_cids, select_list, segen_gb_tbl, needs_xform);


//			Save temporal group-by variables


	ret.append("\n\t//\t\tCompute temporal groupby attributes\n\n");

	  for(g=0;g<gb_tbl.size();g++){

			data_type *gdt = gb_tbl.get_data_type(g);

			if(gdt->is_temporal()){
				sprintf(tmpstr,"\tgbval->gb_var%d = %s;\n",
					g, generate_se_code(gb_tbl.get_def(g),schema).c_str() );
				ret.append(tmpstr);
			}
		}
		ret.append("\n");



//			Compare the temporal GB vars with the stored ones,
//			set flush indicator and update stored GB vars if there is any change.

	if(uses_temporal_flush){
		ret+= "\tif( !( (";
		bool first_one = true;
		for(g=0;g<gb_tbl.size();g++){
			data_type *gdt = gb_tbl.get_data_type(g);

			if(gdt->is_temporal()){
			  sprintf(tmpstr,"last_gb%d",g);   string lhs_op = tmpstr;
			  sprintf(tmpstr,"gbval->gb_var%d",g);   string rhs_op = tmpstr;
			  if(first_one){first_one = false;} else {ret += ") && (";}
			  ret += generate_equality_test(lhs_op, rhs_op, gdt);
			}
		}
		ret += ") ) ){\n";
		for(g=0;g<gb_tbl.size();g++){
		  data_type *gdt = gb_tbl.get_data_type(g);
		  if(gdt->is_temporal()){
			  if(gdt->is_buffer_type()){
				sprintf(tmpstr,"\t\t%s(&(gbval->gb_var%d),&last_gb%d);\n",gdt->get_hfta_buffer_replace().c_str(),g,g);
			  }else{
				sprintf(tmpstr,"\t\tlast_flushed_gb%d = last_gb%d;\n",g,g);
				ret += tmpstr;
				sprintf(tmpstr,"\t\tlast_gb%d = gbval->gb_var%d;\n",g,g);
			  }
			  ret += tmpstr;
			}
		}
		ret += "\t\tneeds_temporal_flush=true;\n";
		ret += "\t\t}else{\n"
			"\t\t\tneeds_temporal_flush=false;\n"
			"\t\t}\n";
	}


//		For temporal status tuple we don't need to do anything else
	ret += "\tif (temp_tuple_received) return NULL;\n\n";

	for(w=0;w<where.size();++w){
		sprintf(tmpstr,"//\t\tPredicate clause %d.\n",w);
		ret += tmpstr;
//			Find the set of	variables accessed in this CNF elem,
//			but in no previous element.
		col_id_set new_cids;
		get_new_pred_cids(where[w]->pr, found_cids, new_cids, segen_gb_tbl);

//			Unpack these values.
		ret += gen_unpack_cids(schema, new_cids, "NULL", needs_xform);
//			Find partial fcns ref'd in this cnf element
		set<int> pfcn_refs;
		collect_partial_fcns_pr(where[w]->pr, pfcn_refs);
		ret += gen_unpack_partial_fcn(schema, partial_fcns, pfcn_refs,"NULL");

		ret += "\tif( !("+generate_predicate_code(where[w]->pr,schema)+
				+") ) return(NULL);\n";
	}

//		The partial functions ref'd in the group-by var and aggregate
//		definitions must also be evaluated.  If one returns false,
//		then implicitly the predicate is false.
	set<int>::iterator pfsi;

	if(ag_gb_pfcns.size() > 0)
		ret += "//\t\tUnpack remaining partial fcns.\n";
	ret += gen_full_unpack_partial_fcn(schema, partial_fcns, ag_gb_pfcns,
										found_cids, segen_gb_tbl, "NULL", needs_xform);

//			Unpack the group-by variables

	  for(g=0;g<gb_tbl.size();g++){
		data_type *gdt = gb_tbl.get_data_type(g);
		if(!gdt->is_temporal()){	// temproal gbs already computed
//			Find the new fields ref'd by this GBvar def.
			col_id_set new_cids;
			get_new_se_cids(gb_tbl.get_def(g), found_cids, new_cids, segen_gb_tbl);
//			Unpack these values.
			ret += gen_unpack_cids(schema, new_cids, "NULL", needs_xform);

			sprintf(tmpstr,"\tgbval->gb_var%d = %s;\n",
				g, generate_se_code(gb_tbl.get_def(g),schema).c_str() );
/*
//				There seems to be no difference between the two
//				branches of the IF statement.
		data_type *gdt = gb_tbl.get_data_type(g);
		  if(gdt->is_buffer_type()){
//				Create temporary copy.
			sprintf(tmpstr,"\tgbval->gb_var%d = %s;\n",
				g, generate_se_code(gb_tbl.get_def(g),schema).c_str() );
		  }else{
			scalarexp_t *gse = gb_tbl.get_def(g);
			sprintf(tmpstr,"\tgbval->gb_var%d = %s;\n",
					g,generate_se_code(gse,schema).c_str());
		  }
*/
		  	ret.append(tmpstr);
		}
	  }
	  ret.append("\n");


	ret+= "\treturn gbval;\n";
	ret += "};\n\n\n";

//--------------------------------------------------------
//			Create and initialize an aggregate object

	ret += this->generate_functor_name()+"_aggrdef *create_aggregate(host_tuple &tup0, gs_sp_t buffer){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

//		return value
	ret += "\t"+generate_functor_name()+"_aggrdef *aggval = ("+generate_functor_name()+
			"_aggrdef *)buffer;\n";

	for(a=0;a<aggr_tbl.size();a++){
		if(aggr_tbl.is_builtin(a)){
//			Create temporaries for buffer return values
		  data_type *adt = aggr_tbl.get_data_type(a);
		  if(adt->is_buffer_type()){
			sprintf(tmpstr,"aggr_tmp_%d", a);
			ret+=adt->make_host_cvar(tmpstr)+";\n";
		  }
		}
	}

//		Unpack all remaining attributes
	ret += gen_remaining_colrefs(schema, cid_set, found_cids, "NULL", needs_xform);
	for(a=0;a<aggr_tbl.size();a++){
	  sprintf(tmpstr,"aggval->aggr_var%d",a);
	  string assignto_var = tmpstr;
	  ret += "\t"+generate_aggr_init(assignto_var,&aggr_tbl,a, schema);
	}

	ret += "\treturn aggval;\n";
	ret += "};\n\n";

//--------------------------------------------------------
//			update an aggregate object

	ret += "void update_aggregate(host_tuple &tup0, "
		+generate_functor_name()+"_groupdef *gbval, "+
		generate_functor_name()+"_aggrdef *aggval){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

//			use of temporaries depends on the aggregate,
//			generate them in generate_aggr_update


//		Unpack all remaining attributes
	ret += gen_remaining_colrefs(schema, cid_set, found_cids, "", needs_xform);
	for(a=0;a<aggr_tbl.size();a++){
	  sprintf(tmpstr,"aggval->aggr_var%d",a);
	  string varname = tmpstr;
	  ret.append(generate_aggr_update(varname,&aggr_tbl,a, schema));
	}

	ret += "\treturn;\n";
	ret += "};\n";

//--------------------------------------------------------
//			reinitialize an aggregate object

	ret += "void reinit_aggregates( "+
		generate_functor_name()+"_groupdef *gbval, "+
		generate_functor_name()+"_aggrdef *aggval){\n";
	//		Variables for execution of the function.
	ret += "\tgs_int32_t problem = 0;\n";	// return unpack failure

//			use of temporaries depends on the aggregate,
//			generate them in generate_aggr_update

	for(g=0;g<gb_tbl.size();g++){
	  data_type *gdt = gb_tbl.get_data_type(g);
	  if(gdt->is_temporal()){
		  if(gdt->is_buffer_type()){
			sprintf(tmpstr,"\t\t%s(&(gbval->gb_var%d),&last_gb%d);\n",gdt->get_hfta_buffer_replace().c_str(),g,g);
		  }else{
			sprintf(tmpstr,"\t\t gbval->gb_var%d =last_gb%d;\n",g,g);
		  }
		  ret += tmpstr;
		}
	}

//		Unpack all remaining attributes
	for(a=0;a<aggr_tbl.size();a++){
	  sprintf(tmpstr,"aggval->aggr_var%d",a);
	  string varname = tmpstr;
	  ret.append(generate_aggr_reinitialize(varname,&aggr_tbl,a, schema));
	}

	ret += "\treturn;\n";
	ret += "};\n";





//---------------------------------------------------
//			Flush test

	ret += "\tbool flush_needed(){\n";
	if(uses_temporal_flush){
		ret += "\t\treturn needs_temporal_flush;\n";
	}else{
		ret += "\t\treturn false;\n";
	}
	ret += "\t};\n";

//---------------------------------------------------
//			create output tuple
//			Unpack the partial functions ref'd in the where clause,
//			select clause.  Evaluate the where clause.
//			Finally, pack the tuple.

//			I need to use special code generation here,
//			so I'll leave it in longhand.

	ret += "host_tuple create_output_tuple("
		+generate_functor_name()+"_groupdef *gbval, "+
		generate_functor_name()+"_aggrdef *aggval, bool &failed){\n";

	ret += "\thost_tuple tup;\n";
	ret += "\tfailed = false;\n";
	ret += "\tgs_retval_t retval = 0;\n";

	string gbvar = "gbval->gb_var";
	string aggvar = "aggval->";


//			First, get the return values from the UDAFS
   	for(a=0;a<aggr_tbl.size();a++){
		if(! aggr_tbl.is_builtin(a)){
			ret += "\t"+aggr_tbl.get_op(a)+"_HFTA_AGGR_OUTPUT_(&(udaf_ret_"+int_to_string(a)+"),";
			if(aggr_tbl.get_storage_type(a)->get_type() != fstring_t) ret+="&";
			ret+="("+aggvar+"aggr_var"+int_to_string(a)+"));\n";
		}
	}

	set<int> hv_sl_pfcns;
	for(w=0;w<having.size();w++){
		collect_partial_fcns_pr(having[w]->pr, hv_sl_pfcns);
	}
	for(s=0;s<select_list.size();s++){
		collect_partial_fcns(select_list[s]->se, hv_sl_pfcns);
	}

//		clean up the partial fcn results from any previous execution
	ret += gen_partial_fcn_dtr(partial_fcns,hv_sl_pfcns);

//		Unpack them now
	for(pfsi=hv_sl_pfcns.begin();pfsi!=hv_sl_pfcns.end();++pfsi){
		ret += unpack_partial_fcn_fm_aggr(partial_fcns[(*pfsi)], (*pfsi), gbvar, aggvar, schema);
		ret += "\tif(retval){ failed = true; return(tup);}\n";
	}

//		Evalaute the HAVING clause
//		TODO: this seems to have a ++ operator rather than a + operator.
	for(w=0;w<having.size();++w){
		ret += "\tif( !("+generate_predicate_code_fm_aggr(having[w]->pr,gbvar, aggvar, schema) +") ) { failed = true; return(tup);}\n";
	}

//          Now, compute the size of the tuple.

//          Unpack any BUFFER type selections into temporaries
//          so that I can compute their size and not have
//          to recompute their value during tuple packing.
//          I can use regular assignment here because
//          these temporaries are non-persistent.
//			TODO: should I be using the selvar generation routine?

	ret += "//\t\tCompute the size of the tuple.\n";
	ret += "//\t\t\tNote: buffer storage packed at the end of the tuple.\n";
      for(s=0;s<select_list.size();s++){
		scalarexp_t *se = select_list[s]->se;
        data_type *sdt = se->get_data_type();
        if(sdt->is_buffer_type() &&
			 !( (se->get_operator_type() == SE_COLREF) ||
				(se->get_operator_type() == SE_AGGR_STAR) ||
				(se->get_operator_type() == SE_AGGR_SE) ||
			   (se->get_operator_type() == SE_FUNC && se->is_partial()) ||
			   (se->get_operator_type() == SE_FUNC && se->get_aggr_ref() >= 0))
		){
            sprintf(tmpstr,"selvar_%d",s);
			ret+="\t"+sdt->make_host_cvar(tmpstr)+" = ";
			ret += generate_se_code_fm_aggr(se,gbvar, aggvar, schema) +";\n";
        }
      }

//      The size of the tuple is the size of the tuple struct plus the
//      size of the buffers to be copied in.

      ret+="\ttup.tuple_size = sizeof(" + generate_tuple_name( this->get_node_name()) +") + sizeof(gs_uint8_t)";
      for(s=0;s<select_list.size();s++){
//		if(s>0) ret += "+";
		scalarexp_t *se = select_list[s]->se;
        data_type *sdt = select_list[s]->se->get_data_type();
        if(sdt->is_buffer_type()){
		  if(!( (se->get_operator_type() == SE_COLREF) ||
				(se->get_operator_type() == SE_AGGR_STAR) ||
				(se->get_operator_type() == SE_AGGR_SE) ||
			   (se->get_operator_type() == SE_FUNC && se->is_partial()) ||
			   (se->get_operator_type() == SE_FUNC && se->get_aggr_ref() >= 0))
		  ){
            sprintf(tmpstr," + %s(&selvar_%d)", sdt->get_hfta_buffer_size().c_str(),s);
            ret.append(tmpstr);
		  }else{
            sprintf(tmpstr," + %s(&%s)", sdt->get_hfta_buffer_size().c_str(),generate_se_code_fm_aggr(se,gbvar, aggvar, schema).c_str());
            ret.append(tmpstr);
		  }
        }
      }
      ret.append(";\n");

//		Allocate tuple data block.
	ret += "//\t\tCreate the tuple block.\n";
	  ret += "\ttup.data = malloc(tup.tuple_size);\n";
	  ret += "\ttup.heap_resident = true;\n";

//		Mark tuple as regular
	  ret += "\t*((gs_sp_t )tup.data + sizeof(" + generate_tuple_name( this->get_node_name()) +")) = REGULAR_TUPLE;\n";

//	  ret += "\ttup.channel = 0;\n";
	  ret += "\t"+generate_tuple_name( this->get_node_name())+" *tuple = ("+
				generate_tuple_name( this->get_node_name())+" *)(tup.data);\n";

//		Start packing.
//			(Here, offsets are hard-wired.  is this a problem?)

	ret += "//\t\tPack the fields into the tuple.\n";
	  ret += "\tint tuple_pos = sizeof("+generate_tuple_name( this->get_node_name()) +") + sizeof(gs_uint8_t);\n";
      for(s=0;s<select_list.size();s++){
		scalarexp_t *se = select_list[s]->se;
        data_type *sdt = se->get_data_type();
        if(sdt->is_buffer_type()){
		  if(!( (se->get_operator_type() == SE_COLREF) ||
				(se->get_operator_type() == SE_AGGR_STAR) ||
				(se->get_operator_type() == SE_AGGR_SE) ||
			   (se->get_operator_type() == SE_FUNC && se->is_partial()) ||
			   (se->get_operator_type() == SE_FUNC && se->get_aggr_ref() >= 0))
		  ){
            sprintf(tmpstr,"\t%s(&(tuple->tuple_var%d), &selvar_%d,  ((gs_sp_t )tuple)+tuple_pos, tuple_pos);\n", sdt->get_hfta_buffer_tuple_copy().c_str(),s, s);
            ret.append(tmpstr);
            sprintf(tmpstr,"\ttuple_pos += %s(&selvar_%d);\n", sdt->get_hfta_buffer_size().c_str(), s);
            ret.append(tmpstr);
		  }else{
            sprintf(tmpstr,"\t%s(&(tuple->tuple_var%d), &%s,  ((gs_sp_t )tuple)+tuple_pos, tuple_pos);\n", sdt->get_hfta_buffer_tuple_copy().c_str(),s, generate_se_code_fm_aggr(se,gbvar, aggvar, schema).c_str());
            ret.append(tmpstr);
            sprintf(tmpstr,"\ttuple_pos += %s(&%s);\n", sdt->get_hfta_buffer_size().c_str(), generate_se_code_fm_aggr(se,gbvar, aggvar, schema).c_str());
            ret.append(tmpstr);
		  }
        }else{
            sprintf(tmpstr,"\ttuple->tuple_var%d = ",s);
            ret.append(tmpstr);
            ret.append(generate_se_code_fm_aggr(se,gbvar, aggvar, schema) );
            ret.append(";\n");
        }
      }

//			Destroy string temporaries
	  ret += gen_buffer_selvars_dtr(select_list);

	  ret += "\treturn tup;\n";
	  ret += "};\n";

//------------------------------------------------------------------
//		Cleaning_when : evaluate the cleaning_when clause.
//		ASSUME that the udaf return values have already
//		been unpacked.  delete the string udaf return values at the end.

	ret += "bool cleaning_when("
		+generate_functor_name()+"_groupdef *gbval, "+
		generate_functor_name()+"_aggrdef *aggval){\n";

	ret += "\tbool retval = true;\n";


	gbvar = "gbval->gb_var";
	aggvar = "aggval->";


	set<int> clw_pfcns;
	for(w=0;w<closing_when.size();w++){
		collect_partial_fcns_pr(closing_when[w]->pr, clw_pfcns);
	}

//		clean up the partial fcn results from any previous execution
	ret += gen_partial_fcn_dtr(partial_fcns,clw_pfcns);

//		Unpack them now
	for(pfsi=clw_pfcns.begin();pfsi!=clw_pfcns.end();++pfsi){
		ret += unpack_partial_fcn_fm_aggr(partial_fcns[(*pfsi)], (*pfsi), gbvar, aggvar, schema);
		ret += "\tif(retval){ return false;}\n";
	}

//		Evalaute the Closing When clause
//		TODO: this seems to have a ++ operator rather than a + operator.
	for(w=0;w<closing_when.size();++w){
		ret += "\tif( !("+generate_predicate_code_fm_aggr(closing_when[w]->pr,gbvar, aggvar, schema) +") ) { return false;}\n";
	}


//			Destroy string return vals of UDAFs
   	for(a=0;a<aggr_tbl.size();a++){
		if(! aggr_tbl.is_builtin(a)){
			int afcn_id = aggr_tbl.get_fcn_id(a);
			data_type *adt = Ext_fcns->get_fcn_dt(afcn_id);
			if(adt->is_buffer_type()){
				sprintf(tmpstr,"\t%s(&udaf_ret_%d);\n",
			  	adt->get_hfta_buffer_destroy().c_str(), a );
				ret += tmpstr;
			}
		}
	}

	ret += "\treturn retval;\n";
	ret += "};\n";




//-------------------------------------------------------------------
//		Temporal update functions

	ret+="bool temp_status_received(){return temp_tuple_received;};\n\n";

//		create a temp status tuple
	ret += "int create_temp_status_tuple(host_tuple& result, bool flush_finished) {\n\n";

	ret += gen_init_temp_status_tuple(this->get_node_name());

//		Start packing.
//			(Here, offsets are hard-wired.  is this a problem?)

	ret += "//\t\tPack the fields into the tuple.\n";
	for(s=0;s<select_list.size();s++){
		data_type *sdt = select_list[s]->se->get_data_type();
		if(sdt->is_temporal()){
			sprintf(tmpstr,"\ttuple->tuple_var%d = ",s);
	  		ret += tmpstr;
			sprintf(tmpstr,"(flush_finished) ? %s : %s ", generate_se_code(select_list[s]->se,schema).c_str(), generate_se_code_fm_aggr(select_list[s]->se,"last_flushed_gb", "", schema).c_str());
			ret += tmpstr;
	  		ret += ";\n";
		}
	}

	ret += "\treturn 0;\n";
	ret += "};};\n\n\n";


//----------------------------------------------------------
//			The hash function

	ret += "struct "+generate_functor_name()+"_hash_func{\n";
	ret += "\tgs_uint32_t operator()(const "+generate_functor_name()+
				"_groupdef *grp) const{\n";
	ret += "\t\treturn(0";
	for(g=0;g<gb_tbl.size();g++){
		data_type *gdt = gb_tbl.get_data_type(g);
		if(! gdt->is_temporal()){
			ret += "^";
			if(gdt->use_hashfunc()){
				if(gdt->is_buffer_type())
					sprintf(tmpstr,"(%s*%s(&(grp->gb_var%d)))",hash_nums[g%NRANDS].c_str(),gdt->get_hfta_hashfunc().c_str(),g);
					else
				sprintf(tmpstr,"(%s*%s(grp->gb_var%d))",hash_nums[g%NRANDS].c_str(),gdt->get_hfta_hashfunc().c_str(),g);
			}else{
				sprintf(tmpstr,"(%s*grp->gb_var%d)",hash_nums[g%NRANDS].c_str(),g);
			}
			ret += tmpstr;
		}
	}
	ret += " >> 32);\n";
	ret += "\t}\n";
	ret += "};\n\n";

//----------------------------------------------------------
//			The comparison function

	ret += "struct "+generate_functor_name()+"_equal_func{\n";
	ret += "\tbool operator()(const "+generate_functor_name()+"_groupdef *grp1, "+
			generate_functor_name()+"_groupdef *grp2) const{\n";
	ret += "\t\treturn( (";

	string hcmpr = "";
	bool first_exec = true;
	for(g=0;g<gb_tbl.size();g++){
		data_type *gdt = gb_tbl.get_data_type(g);
		if(! gdt->is_temporal()){
			if(first_exec){first_exec=false;}else{ hcmpr += ") && (";}
			if(gdt->complex_comparison(gdt)){
			  if(gdt->is_buffer_type())
				sprintf(tmpstr,"(%s(&(grp1->gb_var%d), &(grp2->gb_var%d))==0)",
				gdt->get_hfta_comparison_fcn(gdt).c_str(),g,g);
			  else
				sprintf(tmpstr,"(%s((grp1->gb_var%d), (grp2->gb_var%d))==0)",
				gdt->get_hfta_comparison_fcn(gdt).c_str(),g,g);
			}else{
				sprintf(tmpstr,"grp1->gb_var%d == grp2->gb_var%d",g,g);
			}
			hcmpr += tmpstr;
		}
	}
	if(hcmpr == "")
		hcmpr = "true";
	ret += hcmpr;

	ret += ") );\n";
	ret += "\t}\n";
	ret += "};\n\n";


	return(ret);
}

string rsgah_qpn::generate_operator(int i, string params){

		return(
			"	running_agg_operator<" +
			generate_functor_name()+","+
			generate_functor_name() + "_groupdef, " +
			generate_functor_name() + "_aggrdef, " +
			generate_functor_name()+"_hash_func, "+
			generate_functor_name()+"_equal_func "
			"> *op"+int_to_string(i)+" = new running_agg_operator<"+
			generate_functor_name()+","+
			generate_functor_name() + "_groupdef, " +
			generate_functor_name() + "_aggrdef, " +
			generate_functor_name()+"_hash_func, "+
			generate_functor_name()+"_equal_func "
			">("+params+", \"" + get_node_name() + "\");\n"
		);
}



//		Split aggregation into two HFTA components - sub and superaggregation
//		If unable to split the aggreagates, empty vector will be returned
vector<qp_node *> sgah_qpn::split_node_for_hfta(ext_fcn_list *Ext_fcns, table_list *Schema){

	vector<qp_node *> ret_vec;
	int s, p, g, a, o, i;
	int si;

	vector<string> fta_flds, stream_flds;
	int t = table_name->get_schema_ref();

//			Get the set of interfaces it accesses.
	int ierr;
	vector<string> sel_names;

//			Verify that all of the ref'd UDAFs can be split.

	for(a=0;a<aggr_tbl.size();++a){
		if(! aggr_tbl.is_builtin(a)){
			int afcn = aggr_tbl.get_fcn_id(a);
			int hfta_super_id = Ext_fcns->get_hfta_superaggr_id(afcn);
			int hfta_sub_id = Ext_fcns->get_hfta_subaggr_id(afcn);
			if(hfta_super_id < 0 || hfta_sub_id < 0){
				return(ret_vec);
			}
		}
    }

/////////////////////////////////////////////////////
//			Split into  aggr/aggr.


	sgah_qpn *low_hfta_node = new sgah_qpn();
	low_hfta_node->table_name = table_name;
	low_hfta_node->set_node_name( "_"+node_name );
	low_hfta_node->table_name->set_range_var(table_name->get_var_name());


	sgah_qpn *hi_hfta_node = new sgah_qpn();
	hi_hfta_node->table_name = new tablevar_t(  ("_"+node_name).c_str());
	hi_hfta_node->set_node_name( node_name );
	hi_hfta_node->table_name->set_range_var(table_name->get_var_name());

//			First, process the group-by variables.
//			both low and hi level queries duplicate group-by variables of original query


	for(g=0;g<gb_tbl.size();g++){
//			Insert the gbvar into both low- and hi level hfta.
		scalarexp_t *gbvar_def = dup_se(gb_tbl.get_def(g), &aggr_tbl);
		low_hfta_node->gb_tbl.add_gb_var(
			gb_tbl.get_name(g), gb_tbl.get_tblvar_ref(g), gbvar_def, gb_tbl.get_reftype(g)
		);

//			Insert a ref to the value of the gbvar into the low-level hfta select list.
		colref_t *new_cr = new colref_t(gb_tbl.get_name(g).c_str() );
		scalarexp_t *gbvar_fta = new scalarexp_t(new_cr);
 		gbvar_fta->set_gb_ref(g);
		gbvar_fta->set_data_type( gb_tbl.get_def(g)->get_data_type() );
		scalarexp_t *gbvar_stream = make_fta_se_ref(low_hfta_node->select_list, gbvar_fta,0);

//			Insert the corresponding gbvar ref (gbvar_stream) into the stream.
 		gbvar_stream->set_gb_ref(-1);	// used as GBvar def
		hi_hfta_node->gb_tbl.add_gb_var(
			gbvar_stream->get_colref()->get_field(), -1, gbvar_stream,  gb_tbl.get_reftype(g)
		);

	}
//	hi_hfta_node->gb_tbl.gb_patterns = gb_tbl.gb_patterns; // pattern processing at higtest level
	hi_hfta_node->gb_tbl.set_pattern_info( &gb_tbl); // pattern processing at higtest level

//			SEs in the aggregate definitions.
//			They are all safe, so split them up for later processing.
	map<int, scalarexp_t *> hfta_aggr_se;
	for(a=0;a<aggr_tbl.size();++a){
		split_hfta_aggr( &(aggr_tbl), a,
						&(hi_hfta_node->aggr_tbl), &(low_hfta_node->aggr_tbl)  ,
						low_hfta_node->select_list,
						hfta_aggr_se,
						Ext_fcns
					);
	}


//			Next, the select list.

	for(s=0;s<select_list.size();s++){
		bool fta_forbidden = false;
		scalarexp_t *root_se = rehome_fta_se(select_list[s]->se, &hfta_aggr_se);
		hi_hfta_node->select_list.push_back(
			new select_element(root_se, select_list[s]->name));
	}



//			All the predicates in the where clause must execute
//			in the low-level hfta.

	for(p=0;p<where.size();p++){
		predicate_t *new_pr = dup_pr(where[p]->pr, &aggr_tbl);
		cnf_elem *new_cnf = new cnf_elem(new_pr);
		analyze_cnf(new_cnf);

		low_hfta_node->where.push_back(new_cnf);
	}

//			All of the predicates in the having clause must
//			execute in the high-level hfta node.

	for(p=0;p<having.size();p++){
		predicate_t *pr_root = rehome_fta_pr( having[p]->pr,  &hfta_aggr_se);
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);

		hi_hfta_node->having.push_back(cnf_root);
	}


//			Copy parameters to both nodes
	vector<string> param_names = param_tbl->get_param_names();
	int pi;
	for(pi=0;pi<param_names.size();pi++){
		data_type *dt = param_tbl->get_data_type(param_names[pi]);
		low_hfta_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
		hi_hfta_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
	}
	low_hfta_node->definitions = definitions;
	hi_hfta_node->definitions = definitions;


	low_hfta_node->table_name->set_machine(table_name->get_machine());
	low_hfta_node->table_name->set_interface(table_name->get_interface());
	low_hfta_node->table_name->set_ifq(false);

	hi_hfta_node->table_name->set_machine(table_name->get_machine());
	hi_hfta_node->table_name->set_interface(table_name->get_interface());
	hi_hfta_node->table_name->set_ifq(false);

	ret_vec.push_back(low_hfta_node);
	ret_vec.push_back(hi_hfta_node);


	return(ret_vec);


	// TODO: add splitting into selection/aggregation
}


//		Split aggregation into two HFTA components - sub and superaggregation
//		If unable to split the aggreagates, empty vector will be returned
//			Similar to sgah, but super aggregate is rsgah, subaggr is sgah
vector<qp_node *> rsgah_qpn::split_node_for_hfta(ext_fcn_list *Ext_fcns, table_list *Schema){

	vector<qp_node *> ret_vec;
	int s, p, g, a, o, i;
	int si;

	vector<string> fta_flds, stream_flds;
	int t = table_name->get_schema_ref();

//			Get the set of interfaces it accesses.
	int ierr;
	vector<string> sel_names;

//			Verify that all of the ref'd UDAFs can be split.

	for(a=0;a<aggr_tbl.size();++a){
		if(! aggr_tbl.is_builtin(a)){
			int afcn = aggr_tbl.get_fcn_id(a);
			int hfta_super_id = Ext_fcns->get_hfta_superaggr_id(afcn);
			int hfta_sub_id = Ext_fcns->get_hfta_subaggr_id(afcn);
			if(hfta_super_id < 0 || hfta_sub_id < 0){
				return(ret_vec);
			}
		}
    }

/////////////////////////////////////////////////////
//			Split into  aggr/aggr.


	sgah_qpn *low_hfta_node = new sgah_qpn();
	low_hfta_node->table_name = table_name;
	low_hfta_node->set_node_name( "_"+node_name );
	low_hfta_node->table_name->set_range_var(table_name->get_var_name());


	rsgah_qpn *hi_hfta_node = new rsgah_qpn();
	hi_hfta_node->table_name = new tablevar_t(  ("_"+node_name).c_str());
	hi_hfta_node->set_node_name( node_name );
	hi_hfta_node->table_name->set_range_var(table_name->get_var_name());

//			First, process the group-by variables.
//			both low and hi level queries duplicate group-by variables of original query


	for(g=0;g<gb_tbl.size();g++){
//			Insert the gbvar into both low- and hi level hfta.
		scalarexp_t *gbvar_def = dup_se(gb_tbl.get_def(g), &aggr_tbl);
		low_hfta_node->gb_tbl.add_gb_var(
			gb_tbl.get_name(g), gb_tbl.get_tblvar_ref(g), gbvar_def, gb_tbl.get_reftype(g)
		);

//			Insert a ref to the value of the gbvar into the low-level hfta select list.
		colref_t *new_cr = new colref_t(gb_tbl.get_name(g).c_str() );
		scalarexp_t *gbvar_fta = new scalarexp_t(new_cr);
 		gbvar_fta->set_gb_ref(g);
		gbvar_fta->set_data_type( gb_tbl.get_def(g)->get_data_type() );
		scalarexp_t *gbvar_stream = make_fta_se_ref(low_hfta_node->select_list, gbvar_fta,0);

//			Insert the corresponding gbvar ref (gbvar_stream) into the stream.
 		gbvar_stream->set_gb_ref(-1);	// used as GBvar def
		hi_hfta_node->gb_tbl.add_gb_var(
			gbvar_stream->get_colref()->get_field(), -1, gbvar_stream,  gb_tbl.get_reftype(g)
		);

	}

//			SEs in the aggregate definitions.
//			They are all safe, so split them up for later processing.
	map<int, scalarexp_t *> hfta_aggr_se;
	for(a=0;a<aggr_tbl.size();++a){
		split_hfta_aggr( &(aggr_tbl), a,
						&(hi_hfta_node->aggr_tbl), &(low_hfta_node->aggr_tbl)  ,
						low_hfta_node->select_list,
						hfta_aggr_se,
						Ext_fcns
					);
	}


//			Next, the select list.

	for(s=0;s<select_list.size();s++){
		bool fta_forbidden = false;
		scalarexp_t *root_se = rehome_fta_se(select_list[s]->se, &hfta_aggr_se);
		hi_hfta_node->select_list.push_back(
			new select_element(root_se, select_list[s]->name));
	}



//			All the predicates in the where clause must execute
//			in the low-level hfta.

	for(p=0;p<where.size();p++){
		predicate_t *new_pr = dup_pr(where[p]->pr, &aggr_tbl);
		cnf_elem *new_cnf = new cnf_elem(new_pr);
		analyze_cnf(new_cnf);

		low_hfta_node->where.push_back(new_cnf);
	}

//			All of the predicates in the having clause must
//			execute in the high-level hfta node.

	for(p=0;p<having.size();p++){
		predicate_t *pr_root = rehome_fta_pr( having[p]->pr,  &hfta_aggr_se);
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);

		hi_hfta_node->having.push_back(cnf_root);
	}

//		Similar for closing when
	for(p=0;p<closing_when.size();p++){
		predicate_t *pr_root = rehome_fta_pr( closing_when[p]->pr,  &hfta_aggr_se);
		cnf_elem *cnf_root = new cnf_elem(pr_root);
		analyze_cnf(cnf_root);

		hi_hfta_node->closing_when.push_back(cnf_root);
	}


//			Copy parameters to both nodes
	vector<string> param_names = param_tbl->get_param_names();
	int pi;
	for(pi=0;pi<param_names.size();pi++){
		data_type *dt = param_tbl->get_data_type(param_names[pi]);
		low_hfta_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
		hi_hfta_node->param_tbl->add_param(param_names[pi],dt->duplicate(),
									param_tbl->handle_access(param_names[pi]));
	}
	low_hfta_node->definitions = definitions;
	hi_hfta_node->definitions = definitions;


	low_hfta_node->table_name->set_machine(table_name->get_machine());
	low_hfta_node->table_name->set_interface(table_name->get_interface());
	low_hfta_node->table_name->set_ifq(false);

	hi_hfta_node->table_name->set_machine(table_name->get_machine());
	hi_hfta_node->table_name->set_interface(table_name->get_interface());
	hi_hfta_node->table_name->set_ifq(false);

	ret_vec.push_back(low_hfta_node);
	ret_vec.push_back(hi_hfta_node);


	return(ret_vec);


	// TODO: add splitting into selection/aggregation
}

//---------------------------------------------------------------
//		Code for propagating Protocol field source information


scalarexp_t *resolve_protocol_se(scalarexp_t *se, vector<map<string, scalarexp_t *> *> &src_vec, gb_table *gb_tbl, table_list *Schema){
	scalarexp_t *rse, *lse,*p_se, *gb_se;
	int tno, schema_type;
	map<string, scalarexp_t *> *pse_map;

  switch(se->get_operator_type()){
    case SE_LITERAL:
		return new scalarexp_t(se->get_literal());
    case SE_PARAM:
		return scalarexp_t::make_param_reference(se->get_op().c_str());
    case SE_COLREF:
    	if(se->is_gb()){
			if(gb_tbl == NULL)
					fprintf(stderr,"INTERNAL ERROR, in resolve_protocol_se, se->gb_ref=%d, but gb_tbl is NULL\n",se->get_gb_ref());
			gb_se = gb_tbl->get_def(se->get_gb_ref());
			return resolve_protocol_se(gb_se,src_vec,gb_tbl,Schema);
		}

		schema_type = Schema->get_schema_type(se->get_colref()->get_schema_ref());
		if(schema_type == PROTOCOL_SCHEMA)
			return dup_se(se,NULL);

    	tno = se->get_colref()->get_tablevar_ref();
    	if(tno >= src_vec.size()){
			fprintf(stderr,"INTERNAL ERROR, in resolve_protocol_se, tno=%d, src_vec.size()=%lu\n",tno,src_vec.size());
		}
		if(src_vec[tno] == NULL)
			return NULL;

		pse_map =src_vec[tno];
		p_se = (*pse_map)[se->get_colref()->get_field()];
		if(p_se == NULL)
			return NULL;
		return dup_se(p_se,NULL);
    case SE_UNARY_OP:
    	lse = resolve_protocol_se(se->get_left_se(),src_vec,gb_tbl,Schema);
    	if(lse == NULL)
    		return NULL;
    	else
    		return new scalarexp_t(se->get_op().c_str(),lse);
    case SE_BINARY_OP:
    	lse = resolve_protocol_se(se->get_left_se(),src_vec,gb_tbl,Schema);
    	if(lse == NULL)
    		return NULL;
    	rse = resolve_protocol_se(se->get_right_se(),src_vec,gb_tbl,Schema);
    	if(rse == NULL)
    		return NULL;
   		return new scalarexp_t(se->get_op().c_str(),lse,rse);
    case SE_AGGR_STAR:
		return( NULL );
    case SE_AGGR_SE:
		return( NULL );
	case SE_FUNC:
		return(NULL);
	default:
		return(NULL);
	break;
  }

}

void spx_qpn::create_protocol_se(vector<qp_node *> q_sources, table_list *Schema){
	int i;
	vector<map<string, scalarexp_t *> *> src_vec;

	for(i=0;i<q_sources.size();i++){
		if(q_sources[i] != NULL)
			src_vec.push_back(q_sources[i]->get_protocol_se());
		else
			src_vec.push_back(NULL);
	}

	for(i=0;i<select_list.size();i++){
		protocol_map[select_list[i]->name] = resolve_protocol_se(select_list[i]->se,src_vec,NULL,Schema);
	}
}

void join_eq_hash_qpn::create_protocol_se(vector<qp_node *> q_sources, table_list *Schema){
	int i;
	vector<map<string, scalarexp_t *> *> src_vec;

	for(i=0;i<q_sources.size();i++){
		if(q_sources[i] != NULL)
			src_vec.push_back(q_sources[i]->get_protocol_se());
		else
			src_vec.push_back(NULL);
	}

	for(i=0;i<select_list.size();i++){
		protocol_map[select_list[i]->name] = resolve_protocol_se(select_list[i]->se,src_vec,NULL,Schema);
	}

	for(i=0;i<hash_eq.size();i++){
		hash_src_l.push_back(resolve_protocol_se(hash_eq[i]->pr->get_left_se(),src_vec,NULL,Schema));
		hash_src_r.push_back(resolve_protocol_se(hash_eq[i]->pr->get_right_se(),src_vec,NULL,Schema));
  	}
}

void filter_join_qpn::create_protocol_se(vector<qp_node *> q_sources, table_list *Schema){
	int i;
	vector<map<string, scalarexp_t *> *> src_vec;

	for(i=0;i<q_sources.size();i++){
		if(q_sources[i] != NULL)
			src_vec.push_back(q_sources[i]->get_protocol_se());
		else
			src_vec.push_back(NULL);
	}

	for(i=0;i<select_list.size();i++){
		protocol_map[select_list[i]->name] = resolve_protocol_se(select_list[i]->se,src_vec,NULL,Schema);
	}

	for(i=0;i<hash_eq.size();i++){
		hash_src_l.push_back(resolve_protocol_se(hash_eq[i]->pr->get_left_se(),src_vec,NULL,Schema));
		hash_src_r.push_back(resolve_protocol_se(hash_eq[i]->pr->get_right_se(),src_vec,NULL,Schema));
  	}
}

void sgah_qpn::create_protocol_se(vector<qp_node *> q_sources, table_list *Schema){
	int i;
	vector<map<string, scalarexp_t *> *> src_vec;

	for(i=0;i<q_sources.size();i++){
		if(q_sources[i] != NULL)
			src_vec.push_back(q_sources[i]->get_protocol_se());
		else
			src_vec.push_back(NULL);
	}

	for(i=0;i<select_list.size();i++){
		protocol_map[select_list[i]->name] = resolve_protocol_se(select_list[i]->se,src_vec,&gb_tbl,Schema);
	}

	for(i=0;i<gb_tbl.size();i++)
		gb_sources.push_back(resolve_protocol_se(gb_tbl.get_def(i),src_vec,&gb_tbl,Schema));

}

void rsgah_qpn::create_protocol_se(vector<qp_node *> q_sources, table_list *Schema){
	int i;
	vector<map<string, scalarexp_t *> *> src_vec;

	for(i=0;i<q_sources.size();i++){
		if(q_sources[i] != NULL)
			src_vec.push_back(q_sources[i]->get_protocol_se());
		else
			src_vec.push_back(NULL);
	}

	for(i=0;i<select_list.size();i++){
		protocol_map[select_list[i]->name] = resolve_protocol_se(select_list[i]->se,src_vec,&gb_tbl,Schema);
	}

	for(i=0;i<gb_tbl.size();i++)
		gb_sources.push_back(resolve_protocol_se(gb_tbl.get_def(i),src_vec,&gb_tbl,Schema));
}

void sgahcwcb_qpn::create_protocol_se(vector<qp_node *> q_sources, table_list *Schema){
	int i;
	vector<map<string, scalarexp_t *> *> src_vec;

	for(i=0;i<q_sources.size();i++){
		if(q_sources[i] != NULL)
			src_vec.push_back(q_sources[i]->get_protocol_se());
		else
			src_vec.push_back(NULL);
	}

	for(i=0;i<select_list.size();i++){
		protocol_map[select_list[i]->name] = resolve_protocol_se(select_list[i]->se,src_vec,&gb_tbl,Schema);
	}

	for(i=0;i<gb_tbl.size();i++)
		gb_sources.push_back(resolve_protocol_se(gb_tbl.get_def(i),src_vec,&gb_tbl,Schema));
}

void mrg_qpn::create_protocol_se(vector<qp_node *> q_sources, table_list *Schema){
	int f,s,i;
	scalarexp_t *first_se;

	vector<map<string, scalarexp_t *> *> src_vec;
	map<string, scalarexp_t *> *pse_map;

	for(i=0;i<q_sources.size();i++){
		if(q_sources[i] != NULL)
			src_vec.push_back(q_sources[i]->get_protocol_se());
		else
			src_vec.push_back(NULL);
	}

	if(q_sources.size() == 0){
		fprintf(stderr,"INTERNAL ERROR in mrg_qpn::create_protocol_se, q_sources.size() == 0\n");
		exit(1);
	}

	vector<field_entry *> tbl_flds = table_layout->get_fields();
	for(f=0;f<tbl_flds.size();f++){
		bool match = true;
		string fld_nm = tbl_flds[f]->get_name();
		pse_map = src_vec[0];
		first_se = (*pse_map)[fld_nm];
		if(first_se == NULL)
			match = false;
		for(s=1;s<src_vec.size() && match;s++){
			pse_map = src_vec[s];
			scalarexp_t *match_se = (*pse_map)[fld_nm];
			if(match_se == false)
				match = false;
			else
				match = is_equivalent_se_base(first_se, match_se, Schema);
		}
		if(match)
			protocol_map[fld_nm] = first_se;
		else
			protocol_map[fld_nm] = NULL;
	}
}
