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

#include<unistd.h>

#include "parse_fta.h"
#include "parse_schema.h"
#include "parse_ext_fcns.h"


#include"analyze_fta.h"

#include"type_objects.h"

#include <string>
#include<list>

using namespace std;

extern string hostname;			// name of the current host

//			Utility function

string int_to_string(int i){
    string ret;
    char tmpstr[100];
    sprintf(tmpstr,"%d",i);
    ret=tmpstr;
    return(ret);
}


//				Globals

//			These represent derived information from the
//			query analysis stage.  I extract them from a class,
//			perhaps this is dangerous.

static gb_table *gb_tbl=NULL;			// Table of all group-by attributes.
static aggregate_table *aggr_tbl=NULL;	// Table of all referenced aggregates.

// static cplx_lit_table *complex_literals=NULL;	// Table of literals with constructors.
static param_table *param_tbl=NULL;		// Table of all referenced parameters.

vector<scalarexp_t *> partial_fcns_list;
int wh_partial_start, wh_partial_end;
int gb_partial_start, gb_partial_end;
int aggr_partial_start, aggr_partial_end;
int sl_partial_start, sl_partial_end;


//			Infer the table of a column refrence and return the table ref.
//			First, extract the
//			field name and table name.  If no table name is used,
//			search all tables to try to find a unique match.
//			Of course, plenty of error checking.

//		Return the set of tablevar indices in the FROM clause
//		which contain a field with the same name.
vector<int> find_source_tables(string field, tablevar_list_t *fm, table_list *Schema){
	int i;
	vector<int> tv;
//	vector<string> tn = fm->get_schema_names();
	vector<int> tn = fm->get_schema_refs();
// printf("Calling find_source_tables on field %s\n",field.c_str());
	for(i=0;i<tn.size();i++){
//		if(Schema->contains_field(Schema->find_tbl(tn[i]), field) ){
		if(Schema->contains_field(tn[i], field) ){
			tv.push_back(i);
// printf("\tfound in table %s\n",tn[i].c_str());
		}
	}
	return(tv);
}

int infer_tablevar_from_ifpref(ifpref_t *ir, tablevar_list_t *fm){
	int i;
	string tname = ir->get_tablevar();
	if(tname ==""){
		if(fm->size()==1) return 0;
		fprintf(stderr,"ERROR, interface parameter %s has no tablevar specified and there is more than one table variable in the FROM clause.\n",ir->to_string().c_str());
		return -1;
	}
	for(i=0;i<fm->size();++i){
		if(tname == fm->get_tablevar_name(i))
			return i;
	}
	fprintf(stderr,"ERROR, interface parameter %s has no matching table variable in the FROM clause.\n",ir->to_string().c_str());
	return -1;
}


//		compute the index of the tablevar in the from clause that the
//		colref is in.
//		return -1 if no tablevar can be imputed.
int infer_tablevar_from_colref(colref_t *cr, tablevar_list_t *fm, table_list *schema){
	int i;
	string table_name;
	int table_ref;
	vector<int> tv;
	vector<tablevar_t *> fm_tbls = fm->get_table_list();

	string field = cr->get_field();

// printf("Calling infer_tablevar_from_colref on field %s.\n",field.c_str());
	if(cr->uses_default_table() ){
		tv = find_source_tables(field, fm, schema);
		if(tv.size() > 1){
			fprintf(stderr,"ERROR, line %d, character %d : field %s exists in multiple table variables: ",
				cr->get_lineno(), cr->get_charno(),field.c_str() );
			for(i=0;i<tv.size();i++){
				fprintf(stderr,"%s ",fm_tbls[ tv[i] ]->to_string().c_str() );
			}
			fprintf(stderr,"\n\tYou must specify one of these.\n");
			return(-1);
		}
		if(tv.size() == 0){
			fprintf(stderr,"ERROR, line %d, character %d: field %s does not exist in any table.\n",
				cr->get_lineno(), cr->get_charno(),field.c_str() );
			return(-1);
		}

		return(tv[0]);
	}

//			The table source is named -- but is it a schema name
//			or a var name?

	string interface = cr->get_interface();
	table_name = cr->get_table_name();

//		if interface is not specified, prefer to look at the tablevar names
//		Check for duplicates.
	if(interface==""){
		for(i=0;i<fm_tbls.size();++i){
			if(table_name == fm_tbls[i]->get_var_name())
				tv.push_back(i);
		}
		if(tv.size() > 1){
			fprintf(stderr,"ERROR, there are two or more table variables for column ref %s.%s (line %d, char %d).\n",table_name.c_str(), field.c_str(), cr->get_lineno(), cr->get_charno() );
			return(-1);
		}
		if(tv.size() == 1) return(tv[0]);
	}

//		Tableref not found by looking at tableref vars, or an interface
//		was specified.  Try to match on schema and interface.
//		Check for duplicates.
	for(i=0;i<fm_tbls.size();++i){
		if(table_name == fm_tbls[i]->get_var_name() && interface == fm_tbls[i]->get_interface())
			tv.push_back(i);
	}
	if(tv.size() > 1){
		fprintf(stderr,"ERROR, (line %d, char %d) there are two or more table variables whose schemas match for column ref \n",
			cr->get_lineno(), cr->get_charno() );
		if(interface != "") fprintf(stderr,"%s.",interface.c_str());
		fprintf(stderr,"%s.%s\n",table_name.c_str(), field.c_str());
		return(-1);
	}

	if(tv.size() == 0 ){
		fprintf(stderr,"ERROR, line %d, character %d : no table reference found for column ref ", cr->get_lineno(), cr->get_charno());
		if(interface != "") fprintf(stderr,"%s.",interface.c_str());
		fprintf(stderr,"%s.%s\n",table_name.c_str(), field.c_str());
		return(-1)	;
	}

	return(tv[0]);
}


//			Reset temporal properties of a scalar expression
void reset_temporal(scalarexp_t *se){
	col_id ci;
	vector<scalarexp_t *> operands;
	int o;

	se->get_data_type()->reset_temporal();

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
	case SE_IFACE_PARAM:
	case SE_COLREF:
		return;
	case SE_UNARY_OP:
		reset_temporal(se->get_left_se());
		return;
	case SE_BINARY_OP:
		reset_temporal(se->get_left_se());
		reset_temporal(se->get_right_se());
		return;
	case SE_AGGR_STAR:
		return;
	case SE_AGGR_SE:
		reset_temporal(se->get_left_se());
		return;
	case SE_FUNC:
		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			reset_temporal(operands[o]);
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in reset_temporal, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
}

//		Verify that column references exist in their
//		declared tables.  As a side effect, assign
//		their data types.  Other side effects :
//
//		return -1 on error

int verify_colref(scalarexp_t *se, tablevar_list_t *fm,
					table_list *schema, gb_table *gtbl){
	int l_ret, r_ret;
	int gb_ref;
	colref_t *cr;
	ifpref_t *ir;
	string field, table_source, type_name;
	data_type *dt;
	vector<string> tn;
	vector<int> tv;
	int table_var;
	int o;
	vector<scalarexp_t *> operands;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
		return(1);
	case SE_IFACE_PARAM:
		ir = se->get_ifpref();
		table_var = infer_tablevar_from_ifpref(ir, fm);
		if(table_var < 0) return(table_var);
		ir->set_tablevar_ref(table_var);
		return(1);
	case SE_UNARY_OP:
		return( verify_colref(se->get_left_se(), fm, schema, gtbl) );
	case SE_BINARY_OP:
		l_ret = verify_colref(se->get_left_se(), fm, schema, gtbl);
		r_ret = verify_colref(se->get_right_se(), fm, schema, gtbl);
		if( (l_ret < 0 ) || (r_ret < 0) ) return(-1);
		return(1);
	case SE_COLREF:
		cr = se->get_colref();
		field = cr->get_field();

//				Determine if this is really a GB ref.
//				(the parser can only see that its a colref).
		if(gtbl != NULL){
			gb_ref = gtbl->find_gb(cr, fm, schema);
		}else{
			gb_ref = -1;
		}

		se->set_gb_ref(gb_ref);

		if(gb_ref < 0){
//				Its a colref, verify its existance and
//				record the data type.
			table_var = infer_tablevar_from_colref(cr,fm,schema);
			if(table_var < 0) return(table_var);

	//			Store the table ref in the colref.
			cr->set_tablevar_ref(table_var);
			cr->set_schema_ref(fm->get_schema_ref(table_var));
			cr->set_interface("");
			cr->set_table_name(fm->get_tablevar_name(table_var));


			type_name = schema->get_type_name(cr->get_schema_ref(), field);
			param_list *modifiers = schema->get_modifier_list(cr->get_schema_ref(), field);
			dt = new data_type(type_name, modifiers);
			se->set_data_type(dt);
		}else{
//				Else, its a gbref, use the GB var's data type.
			se->set_data_type(gtbl->get_data_type(gb_ref));
		}

		return(1);
	case SE_AGGR_STAR:
		return(1);
	case SE_AGGR_SE:
		return( verify_colref(se->get_left_se(), fm, schema, gtbl) );
	case SE_FUNC:
		operands = se->get_operands();
		r_ret = 1;
		for(o=0;o<operands.size();o++){
			l_ret = verify_colref(operands[o], fm, schema, gtbl);
			if(l_ret < 0) r_ret = -1;
		}
		return(r_ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in verify_colref, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		return(-1);
	}
	return(-1);
}


int verify_predicate_colref(predicate_t *pr, tablevar_list_t *fm, table_list *schema, gb_table *gtbl){
	int l_ret, r_ret;
	std::vector<scalarexp_t *> op_list;
	int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		return(verify_colref(pr->get_left_se(),fm,schema, gtbl) );
	case PRED_COMPARE:
		l_ret = verify_colref(pr->get_left_se(),fm,schema, gtbl) ;
		r_ret = verify_colref(pr->get_right_se(),fm,schema, gtbl) ;
		if( (l_ret < 0) || (r_ret < 0) ) return(-1);
		return(1);
	case PRED_UNARY_OP:
		return(verify_predicate_colref(pr->get_left_pr(),fm,schema, gtbl));
	case PRED_BINARY_OP:
		l_ret = verify_predicate_colref(pr->get_left_pr(),fm,schema, gtbl) ;
		r_ret = verify_predicate_colref(pr->get_right_pr(),fm,schema, gtbl) ;
		if( (l_ret < 0) || (r_ret < 0) ) return(-1);
		return(1);
	case PRED_FUNC:
		op_list = pr->get_op_list();
		l_ret = 0;
		for(o=0;o<op_list.size();++o){
			if(verify_colref(op_list[o],fm,schema,gtbl) < 0) l_ret = -1;
		}
		return(l_ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in verify_predicate_colref, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
	}

	return(-1);
}


bool literal_only_se(scalarexp_t *se){		// really only literals.
	int o;
	vector<scalarexp_t *> operands;

	if(se == NULL) return(1);
	switch(se->get_operator_type()){
	case SE_LITERAL:
		return(true);
	case SE_PARAM:
		return(false);
	case SE_IFACE_PARAM:
		return(false);
	case SE_UNARY_OP:
		return( literal_only_se(se->get_left_se()) );
	case SE_BINARY_OP:
		return( literal_only_se(se->get_left_se()) &&
				literal_only_se(se->get_right_se()) );
	case SE_COLREF:
		return false;
	case SE_AGGR_STAR:
		return false;
	case SE_AGGR_SE:
		return false;
		return(1);
	case SE_FUNC:
		return false;
	default:
		return false;
	}
	return false;
}




//		Verify that column references exist in their
//		declared tables.  As a side effect, assign
//		their data types.  Other side effects :
//

int bind_to_schema_se(scalarexp_t *se, tablevar_list_t *fm, table_list *schema){
	int l_ret, r_ret;
	int gb_ref;
	colref_t *cr;
	string field, table_source, type_name;
	data_type *dt;
	vector<string> tn;
	vector<int> tv;
	int tablevar_ref;
	int o;
	vector<scalarexp_t *> operands;

	if(se == NULL) return(1);

	switch(se->get_operator_type()){
	case SE_LITERAL:
		return(1);
	case SE_PARAM:
		return(1);
	case SE_IFACE_PARAM:
		return(1);
	case SE_UNARY_OP:
		return( bind_to_schema_se(se->get_left_se(), fm, schema) );
	case SE_BINARY_OP:
		l_ret = bind_to_schema_se(se->get_left_se(), fm, schema);
		r_ret = bind_to_schema_se(se->get_right_se(), fm, schema);
		if( (l_ret < 0 ) || (r_ret < 0) ) return(-1);
		return(1);
	case SE_COLREF:
		if(se->is_gb()) return(1);	// gb ref not a colref.

		cr = se->get_colref();
		field = cr->get_field();

		tablevar_ref = infer_tablevar_from_colref(cr,fm,schema);
		if(tablevar_ref < 0){
			return(tablevar_ref);
		}else{
	//			Store the table ref in the colref.
			cr->set_tablevar_ref(tablevar_ref);
			cr->set_schema_ref(fm->get_schema_ref(tablevar_ref));
			cr->set_interface("");
			cr->set_table_name(fm->get_tablevar_name(tablevar_ref));

//				Check the data type
			type_name = schema->get_type_name(cr->get_schema_ref(), field);
			param_list *modifiers = schema->get_modifier_list(cr->get_schema_ref(), field);
			data_type dt(type_name, modifiers);
//			if(! dt.equals(se->get_data_type()) ){
//			if(! dt.subsumes_type(se->get_data_type()) ){
			if(! se->get_data_type()->subsumes_type(&dt) ){
				fprintf(stderr,"INTERNAL ERROR in bind_to_schema_se: se's type is %d, table's is %d, colref is %s.\n",
					dt.type_indicator(), se->get_data_type()->type_indicator(), cr->to_string().c_str());
				return(-1);
			}
		}
		return(1);
	case SE_AGGR_STAR:
		return(1);
	case SE_AGGR_SE:	// Probably I should just return,
						// aggregate se's are explicitly bound to the schema.
//			return( bind_to_schema_se(se->get_left_se(), fm, schema, gtbl) );
		return(1);
	case SE_FUNC:
		if(se->get_aggr_ref() >= 0) return 1;

		operands = se->get_operands();
		r_ret = 1;
		for(o=0;o<operands.size();o++){
			l_ret = bind_to_schema_se(operands[o], fm, schema);
			if(l_ret < 0) r_ret = -1;
		}
		return(r_ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in bind_to_schema_se, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		return(-1);
	}
	return(-1);
}


int bind_to_schema_pr(predicate_t *pr, tablevar_list_t *fm, table_list *schema){
	int l_ret, r_ret;
	vector<scalarexp_t *> op_list;
	int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		return(bind_to_schema_se(pr->get_left_se(),fm,schema) );
	case PRED_COMPARE:
		l_ret = bind_to_schema_se(pr->get_left_se(),fm,schema) ;
		r_ret = bind_to_schema_se(pr->get_right_se(),fm,schema) ;
		if( (l_ret < 0) || (r_ret < 0) ) return(-1);
		return(1);
	case PRED_UNARY_OP:
		return(bind_to_schema_pr(pr->get_left_pr(),fm,schema));
	case PRED_BINARY_OP:
		l_ret = bind_to_schema_pr(pr->get_left_pr(),fm,schema) ;
		r_ret = bind_to_schema_pr(pr->get_right_pr(),fm,schema) ;
		if( (l_ret < 0) || (r_ret < 0) ) return(-1);
		return(1);
	case PRED_FUNC:
		op_list = pr->get_op_list();
		l_ret = 0;
		for(o=0;o<op_list.size();++o){
			if(bind_to_schema_se(op_list[o],fm,schema) < 0) l_ret = -1;
		}
		return(l_ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in bind_to_schema_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
	}

	return(-1);
}






//			verify_colref assigned data types to the column refs.
//			Now assign data types to all other nodes in the
//			scalar expression.
//
//			return -1 on error

temporal_type compute_se_temporal(scalarexp_t *se, map<col_id, temporal_type> &tcol){
	int l_ret, r_ret;
	data_type *dt;
	bool bret;
	vector<scalarexp_t *> operands;
	vector<data_type *> odt;
	int o, fcn_id;
	vector<bool> handle_ind;

	switch(se->get_operator_type()){
	case SE_LITERAL:
		return(constant_t);
	case SE_PARAM:
		return(varying_t);
	case SE_IFACE_PARAM:
		return(varying_t);		// actually, this should not be called.
	case SE_UNARY_OP:
		return data_type::compute_temporal_type(
			compute_se_temporal(se->get_left_se(), tcol), se->get_op()
		);
	case SE_BINARY_OP:
		return data_type::compute_temporal_type(
			compute_se_temporal(se->get_left_se(), tcol),
			compute_se_temporal(se->get_right_se(), tcol),
			se->get_left_se()->get_data_type()->get_type(),
			se->get_right_se()->get_data_type()->get_type(),
			se->get_op()
		);
	case SE_COLREF:
		{
			col_id cid(se->get_colref() );
			if(tcol.count(cid) > 0){ return tcol[cid];
			}else{ return varying_t;}
		}
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
	case SE_FUNC:
	default:
		return varying_t;
	}
	return(varying_t);
}



//			verify_colref assigned data types to the column refs.
//			Now assign data types to all other nodes in the
//			scalar expression.
//
//			return -1 on error

int assign_data_types(scalarexp_t *se, table_list *schema,
						table_exp_t *fta_tree, ext_fcn_list *Ext_fcns){
	int l_ret, r_ret;
	data_type *dt;
	bool bret;
	vector<scalarexp_t *> operands;
	vector<data_type *> odt;
	int o, fcn_id;
	vector<bool> handle_ind;
	vector<bool> constant_ind;

	switch(se->get_operator_type()){
	case SE_LITERAL:
		dt = new data_type( se->get_literal()->get_type() );
		se->set_data_type(dt);
		if( ! dt->is_defined() ){
			fprintf(stderr,"ERROR, Literal type is undefined, line =%d, char = %d, literal=%s\n",
				se->get_literal()->get_lineno(),se->get_literal()->get_charno(), se->get_literal()->to_string().c_str() );
			return(-1);
		}else{
			return(1);
		}
	case SE_PARAM:
		{
			string pname = se->get_param_name();
			dt = param_tbl->get_data_type(pname);
// dt->set_temporal(constant_t);
			se->set_data_type(dt);
			if( ! dt->is_defined() ){
				fprintf(stderr,"ERROR, parameter %s has undefined type, line =%d, char = %d\n",
					pname.c_str(), se->get_lineno(),se->get_charno() );
				return(-1);
			}
			return(1);
		}
	case SE_IFACE_PARAM:
		dt = new data_type( "STRING" );
		se->set_data_type(dt);
		return(1);
	case SE_UNARY_OP:
		l_ret = assign_data_types(se->get_left_se(), schema, fta_tree, Ext_fcns);
		if(l_ret < 0) return -1;

		dt = new data_type(se->get_left_se()->get_data_type(),se->get_op() );
		se->set_data_type(dt);
		if( ! dt->is_defined() ){
			fprintf(stderr,"ERROR, unary operator %s not defined for type %s, line=%d, char = %d\n",
				se->get_op().c_str(), se->get_left_se()->get_data_type()->to_string().c_str(),
				se->get_lineno(), se->get_charno() );
			return(-1);
		}else{
			return(1);
		}
	case SE_BINARY_OP:
		l_ret = assign_data_types(se->get_left_se(),  schema, fta_tree, Ext_fcns);
		r_ret = assign_data_types(se->get_right_se(),  schema, fta_tree, Ext_fcns);
		if( (l_ret < 0 ) || (r_ret < 0) ) return(-1);

		dt = new data_type(se->get_left_se()->get_data_type(),se->get_right_se()->get_data_type(),se->get_op() );
		se->set_data_type(dt);
		if( ! dt->is_defined() ){
			fprintf(stderr,"ERROR, Binary operator %s not defined for type %s, %s line=%d, char = %d\n",
				se->get_op().c_str(), se->get_left_se()->get_data_type()->to_string().c_str(),
				se->get_right_se()->get_data_type()->to_string().c_str(),
				se->get_lineno(), se->get_charno() );
			return(-1);
		}else{
			return(1);
		}
	case SE_COLREF:
		dt = se->get_data_type();
		bret = dt->is_defined();
		if( bret ){
			return(1);
		}else{
			fprintf(stderr,"ERROR, column reference type  is undefined, line =%d, char = %d, colref=%s\n",
				se->get_colref()->get_lineno(),se->get_colref()->get_charno(), se->get_colref()->to_string().c_str() );
			return(-1);
		}
	case SE_AGGR_STAR:
		dt = new data_type("Int");	// changed Uint to Int
		se->set_data_type(dt);
		return(1);
	case SE_AGGR_SE:
		l_ret = assign_data_types(se->get_left_se(), schema, fta_tree, Ext_fcns);
		if(l_ret < 0) return -1;

		dt = new data_type();
		dt->set_aggr_data_type(se->get_op(), se->get_left_se()->get_data_type());
		se->set_data_type(dt);

		if( ! dt->is_defined() ){
			fprintf(stderr,"ERROR, aggregate %s not defined for type %s, line=%d, char = %d\n",
				se->get_op().c_str(), se->get_left_se()->get_data_type()->to_string().c_str(),
				se->get_lineno(), se->get_charno() );
			return(-1);
		}else{
			return(1);
		}
	case SE_FUNC:

		operands = se->get_operands();
		r_ret = 1;
		for(o=0;o<operands.size();o++){
			l_ret = assign_data_types(operands[o], schema, fta_tree, Ext_fcns);
			odt.push_back(operands[o]->get_data_type());
			if(l_ret < 0) r_ret = -1;
		}
		if(r_ret < 0) return(r_ret);

//			Is it an aggregate extraction function?
		fcn_id = Ext_fcns->lookup_extr(se->get_op(), odt);
		if(fcn_id >= 0){
			int actual_fcn_id = Ext_fcns->get_actual_fcn_id(fcn_id);
			int subaggr_id = Ext_fcns->get_subaggr_id(fcn_id);
			int n_fcn_params = Ext_fcns->get_nparams(actual_fcn_id);
//				Construct a se for the subaggregate.
			vector<scalarexp_t *> op_a;
			int n_aggr_oprs = operands.size()-n_fcn_params+1;
			for(o=0;o<n_aggr_oprs;++o){
					op_a.push_back(operands[o]);
			}
//				check handle params
			vector<bool> handle_a = Ext_fcns->get_handle_indicators(subaggr_id);
			for(o=0;o<op_a.size();o++){
			if(handle_a[o]){
				if(op_a[o]->get_operator_type() != SE_LITERAL &&
						op_a[o]->get_operator_type() != SE_IFACE_PARAM &&
						op_a[o]->get_operator_type() != SE_PARAM){
						fprintf(stderr,"ERROR, the %d-th parameter of UDAF %s (extractor %s) must be a literal or query parameter (because it is a pass-by-HANDLE parameter).\n  Line=%d, char=%d.\n",
				o+1, Ext_fcns->get_fcn_name(subaggr_id).c_str(), se->get_op().c_str(),se->get_lineno(), se->get_charno());
						return(-1);
					}
				}
			}
			vector<bool> is_const_a=Ext_fcns->get_const_indicators(subaggr_id);
			for(o=0;o<op_a.size();o++){
			if(is_const_a[o]){
				if(op_a[o]->get_data_type()->get_temporal() != constant_t){
						fprintf(stderr,"ERROR, the %d-th parameter of UDAF %s (extractor %s) must be constant.\n  Line=%d, char=%d.\n",
				o+1, Ext_fcns->get_fcn_name(subaggr_id).c_str(), se->get_op().c_str(),se->get_lineno(), se->get_charno());
						return(-1);
					}
				}
			}

			scalarexp_t *se_a  = new scalarexp_t(Ext_fcns->get_fcn_name(subaggr_id).c_str(), op_a);
			se_a->set_fcn_id(subaggr_id);
			se_a->set_data_type(Ext_fcns->get_fcn_dt(subaggr_id));
			se_a->set_aggr_id(0);		// label this as a UDAF.


//				Change this se to be the actual function
			vector<scalarexp_t *> op_f;
			op_f.push_back(se_a);
			for(o=n_aggr_oprs;o<operands.size();++o)
				op_f.push_back(operands[o]);
//				check handle params
			vector<bool> handle_f = Ext_fcns->get_handle_indicators(actual_fcn_id);
			for(o=0;o<op_f.size();o++){
			if(handle_f[o]){
				if(op_f[o]->get_operator_type() != SE_LITERAL &&
						op_f[o]->get_operator_type() != SE_IFACE_PARAM &&
						op_f[o]->get_operator_type() != SE_PARAM){
						fprintf(stderr,"ERROR, the %d-th parameter of fcn %s (extractor %s) must be a literal or query parameter (because it is a pass-by-HANDLE parameter).\n  Line=%d, char=%d.\n",
				o+1, Ext_fcns->get_fcn_name(actual_fcn_id).c_str(), se->get_op().c_str(),se->get_lineno(), se->get_charno());
						return(-1);
					}
				}
			}
			vector<bool> is_const_f=Ext_fcns->get_const_indicators(actual_fcn_id);
			for(o=0;o<op_f.size();o++){
			if(is_const_f[o]){
				if(op_f[o]->get_data_type()->get_temporal() != constant_t){
						fprintf(stderr,"ERROR, the %d-th parameter of fcn %s (extractor %s) must be constant.\n  Line=%d, char=%d.\n",
				o+1, Ext_fcns->get_fcn_name(actual_fcn_id).c_str(), se->get_op().c_str(),se->get_lineno(), se->get_charno());
						return(-1);
					}
				}
			}

			se->param_list = op_f;
			se->op = Ext_fcns->get_fcn_name(actual_fcn_id);
			se->set_fcn_id(actual_fcn_id);
			se->set_data_type(Ext_fcns->get_fcn_dt(actual_fcn_id));
			return(1);
		}
		if(fcn_id == -2){
			fprintf(stderr,"Warning: multiple subsuming aggregate extractors found for %s\n",se->get_op().c_str());
		}

//			Is it a UDAF?
		fcn_id = Ext_fcns->lookup_udaf(se->get_op(), odt);
		if(fcn_id >= 0){
			se->set_fcn_id(fcn_id);
			se->set_data_type(Ext_fcns->get_fcn_dt(fcn_id));
			se->set_aggr_id(0);		// label this as a UDAF.
//			Finally, verify that all HANDLE parameters are literals or params.
			handle_ind = Ext_fcns->get_handle_indicators(se->get_fcn_id() );
			for(o=0;o<operands.size();o++){
				if(handle_ind[o]){
					if(operands[o]->get_operator_type() != SE_LITERAL &&
						operands[o]->get_operator_type() != SE_IFACE_PARAM &&
						operands[o]->get_operator_type() != SE_PARAM){
						fprintf(stderr,"ERROR, the %d-th parameter of UDAF %s must be a literal or query parameter (because it is a pass-by-HANDLE parameter).\n  Line=%d, char=%d.\n",
					o+1, se->get_op().c_str(),se->get_lineno(), se->get_charno());
						return(-1);
					}
				}
			}
			constant_ind = Ext_fcns->get_const_indicators(se->get_fcn_id());
			for(o=0;o<operands.size();o++){
			if(constant_ind[o]){
				if(operands[o]->get_data_type()->get_temporal() != constant_t){
						fprintf(stderr,"ERROR, the %d-th parameter of UDAF %s  must be constant.\n  Line=%d, char=%d.\n",
				o+1, se->get_op().c_str(),se->get_lineno(), se->get_charno());
						return(-1);
					}
				}
			}

//	UDAFS as superaggregates not yet supported.
if(se->is_superaggr()){
fprintf(stderr,"WARNING: UDAF superagggregates (%s) are not yet supported, ignored.\n  Line=%d, char=%d.\n", se->get_op().c_str(),se->get_lineno(), se->get_charno());
se->set_superaggr(false);
}
			return(1);
		}
		if(fcn_id == -2){
			fprintf(stderr,"Warning: multiple subsuming UDAFs found for %s\n",se->get_op().c_str());
		}

//			Is it a stateful fcn?
		fcn_id = Ext_fcns->lookup_sfun(se->get_op(), odt);
		if(fcn_id >= 0){
			se->set_fcn_id(fcn_id);
			se->set_data_type(Ext_fcns->get_fcn_dt(fcn_id));
			se->set_storage_state(Ext_fcns->get_storage_state(fcn_id)); // label as sfun
//			Finally, verify that all HANDLE parameters are literals or params.
			handle_ind = Ext_fcns->get_handle_indicators(se->get_fcn_id() );
			for(o=0;o<operands.size();o++){
				if(handle_ind[o]){
					if(operands[o]->get_operator_type() != SE_LITERAL &&
						operands[o]->get_operator_type() != SE_IFACE_PARAM &&
						operands[o]->get_operator_type() != SE_PARAM){
						fprintf(stderr,"ERROR, the %d-th parameter of UDAF %s must be a literal or query parameter (because it is a pass-by-HANDLE parameter).\n  Line=%d, char=%d.\n",
					o+1, se->get_op().c_str(),se->get_lineno(), se->get_charno());
						return(-1);
					}
				}
			}
			constant_ind = Ext_fcns->get_const_indicators(se->get_fcn_id());
			for(o=0;o<operands.size();o++){
			if(constant_ind[o]){
				if(operands[o]->get_data_type()->get_temporal() != constant_t){
						fprintf(stderr,"ERROR, the %d-th parameter of UDAF %s  must be constant.\n  Line=%d, char=%d.\n",
				o+1, se->get_op().c_str(),se->get_lineno(), se->get_charno());
						return(-1);
					}
				}
			}

			if(se->is_superaggr()){
				fprintf(stderr,"WARNING: stateful function %s cannot be marked as a superaggregate, ignored.\n  Line=%d, char=%d.\n", se->get_op().c_str(),se->get_lineno(), se->get_charno());
			}
			return(1);
		}
		if(fcn_id == -2){
			fprintf(stderr,"Warning: multiple stateful fcns found for %s\n",se->get_op().c_str());
		}


//			Is it a regular function?
		fcn_id = Ext_fcns->lookup_fcn(se->get_op(), odt);
		if( fcn_id < 0 ){
			fprintf(stderr,"ERROR, no external function %s(",se->get_op().c_str());
			for(o=0;o<operands.size();o++){
				if(o>0) fprintf(stderr,", ");
				fprintf(stderr,"%s",operands[o]->get_data_type()->to_string().c_str());
			}
			fprintf(stderr,") is defined, line %d, char %d\n", se->get_lineno(), se->get_charno() );
			if(fcn_id == -2) fprintf(stderr,"(multiple subsuming functions found)\n");

			return(-1);
		}

		se->set_fcn_id(fcn_id);
		dt = Ext_fcns->get_fcn_dt(fcn_id);

		if(! dt->is_defined() ){
			fprintf(stderr,"ERROR, external function %s(",se->get_op().c_str());
			for(o=0;o<operands.size();o++){
				if(o>0) fprintf(stderr,", ");
				fprintf(stderr,"%s",operands[o]->get_data_type()->to_string().c_str());
			}
			fprintf(stderr,") has undefined type, line %d, char %d\n", se->get_lineno(), se->get_charno() );
			return(-1);
		}

//			Finally, verify that all HANDLE parameters are literals or params.
		handle_ind = Ext_fcns->get_handle_indicators(se->get_fcn_id() );
		for(o=0;o<operands.size();o++){
			if(handle_ind[o]){
				if(operands[o]->get_operator_type() != SE_LITERAL &&
						operands[o]->get_operator_type() != SE_IFACE_PARAM &&
						operands[o]->get_operator_type() != SE_PARAM){
					fprintf(stderr,"ERROR, the %d-th parameter of function %s must be a literal or query parameter (because it is a pass-by-HANDLE parameter).\n  Line=%d, char=%d.\n",
				o+1, se->get_op().c_str(),se->get_lineno(), se->get_charno());
					return(-1);
				}
			}
		}
		constant_ind = Ext_fcns->get_const_indicators(se->get_fcn_id());
		for(o=0;o<operands.size();o++){
		if(constant_ind[o]){
			if(operands[o]->get_data_type()->get_temporal() != constant_t){
					fprintf(stderr,"ERROR, the %d-th parameter of function %s  must be constant.\n  Line=%d, char=%d.\n",
			o+1, se->get_op().c_str(),se->get_lineno(), se->get_charno());
					return(-1);
				}
			}
		}


		if(se->is_superaggr()){
			fprintf(stderr,"WARNING: function %s cannot be marked as a superaggregate, ignored.\n  Line=%d, char=%d.\n", se->get_op().c_str(),se->get_lineno(), se->get_charno());
		}

		se->set_data_type(dt);
		return(1);
	default:
		fprintf(stderr,"INTERNAL ERROR in assign_data_types, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		return(-1);
	}
	return(-1);
}


int assign_predicate_data_types(predicate_t *pr, table_list *schema,
							table_exp_t *fta_tree, ext_fcn_list *Ext_fcns){
	int l_ret, r_ret;
	int i;
	data_type *dt, *dtl;
	vector<data_type *> odt;
	vector<literal_t *> litl;
	vector<scalarexp_t *> operands;
	vector<bool> handle_ind;
	vector<bool> constant_ind;
	int o, fcn_id;

	switch(pr->get_operator_type()){
	case PRED_IN:
		l_ret = assign_data_types(pr->get_left_se(),schema, fta_tree, Ext_fcns); // , ext_fcn_set);
		litl = pr->get_lit_vec();
		dt = pr->get_left_se()->get_data_type();

		for(i=0;i<litl.size();i++){
			dtl = new data_type( litl[i]->get_type() );
			if( ! dt->is_comparable(dtl,pr->get_op()) ){
				fprintf(stderr,"ERROR line %d, char %d: IS_IN types must be comparable (lhs type is %s, rhs type is %s).\n",
					litl[i]->get_lineno(), litl[i]->get_charno(), dt->to_string().c_str(),dtl->to_string().c_str() );
				delete dtl;
				return(-1);
			}
			delete dtl;
		}
		return(1);
	case PRED_COMPARE:
		l_ret = assign_data_types(pr->get_left_se(),schema, fta_tree, Ext_fcns); // , ext_fcn_set) ;
		r_ret = assign_data_types(pr->get_right_se(),schema, fta_tree, Ext_fcns); // , ext_fcn_set) ;
		if( (l_ret < 0) || (r_ret < 0) ) return(-1);

		if( !(pr->get_left_se()->get_data_type()->is_comparable(pr->get_right_se()->get_data_type(), pr->get_op() ) )){
			fprintf(stderr,"ERROR line %d, char %d, operands of comparison must have comparable types (%s %s %s).\n",
				pr->get_lineno(), pr->get_charno(), pr->get_left_se()->get_data_type()->to_string().c_str(),
				 pr->get_right_se()->get_data_type()->to_string().c_str(), pr->get_op().c_str() );
			return(-1);
		}else{
			return(1);
		}
	case PRED_UNARY_OP:
		return(assign_predicate_data_types(pr->get_left_pr(),schema,fta_tree, Ext_fcns)); // , ext_fcn_set));
	case PRED_BINARY_OP:
		l_ret = assign_predicate_data_types(pr->get_left_pr(),schema,fta_tree, Ext_fcns); // , ext_fcn_set);
		r_ret = assign_predicate_data_types(pr->get_right_pr(),schema,fta_tree, Ext_fcns); // , ext_fcn_set);
		if( (l_ret < 0) || (r_ret < 0) ) return(-1);
		return(1);
	case PRED_FUNC:
		operands = pr->get_op_list();
		r_ret = 1;
		for(o=0;o<operands.size();o++){
			l_ret = assign_data_types(operands[o], schema, fta_tree, Ext_fcns); // , ext_fcn_set);
			odt.push_back(operands[o]->get_data_type());
			if(l_ret < 0) r_ret = -1;
		}
		if(r_ret < 0) return(r_ret);

		fcn_id = Ext_fcns->lookup_pred(pr->get_op(), odt);
		if( fcn_id < 0 ){
			fprintf(stderr,"ERROR, no external predicate %s(",pr->get_op().c_str());
			for(o=0;o<operands.size();o++){
				if(o>0) fprintf(stderr,", ");
				fprintf(stderr,"%s",operands[o]->get_data_type()->to_string().c_str());
			}
			fprintf(stderr,") is defined, line %d, char %d\n", pr->get_lineno(), pr->get_charno() );
			if(fcn_id == -2) fprintf(stderr,"(multiple subsuming predicates found)\n");
			return(-1);
		}

//		ext_fcn_set.insert(fcn_id);
		pr->set_fcn_id(fcn_id);

//			Finally, verify that all HANDLE parameters are literals or params.
		handle_ind = Ext_fcns->get_handle_indicators(pr->get_fcn_id() );
		for(o=0;o<operands.size();o++){
			if(handle_ind[o]){
				if(operands[o]->get_operator_type() != SE_LITERAL &&
						operands[o]->get_operator_type() != SE_IFACE_PARAM &&
						operands[o]->get_operator_type() != SE_PARAM){
					fprintf(stderr,"ERROR, the %d-th parameter of predicate %s must be a literal or query parameter (because it is a pass-by-HANDLE parameter).\n  Line=%d, char=%d.\n",
				o+1, pr->get_op().c_str(),pr->get_lineno(), pr->get_charno());
					exit(1);
				}
			}
		}
		constant_ind = Ext_fcns->get_const_indicators(pr->get_fcn_id());
		for(o=0;o<operands.size();o++){
		if(constant_ind[o]){
			if(operands[o]->get_data_type()->get_temporal() != constant_t){
					fprintf(stderr,"ERROR, the %d-th parameter of predicate %s  must be constant.\n  Line=%d, char=%d.\n",
			o+1, pr->get_op().c_str(),pr->get_lineno(), pr->get_charno());
					exit(1);
				}
			}
		}


//			Check if this predicate function is special sampling function
		pr->is_sampling_fcn = Ext_fcns->is_sampling_fcn(pr->get_fcn_id());


		return(l_ret);
	default:
		fprintf(stderr,"INTERNAL ERROR in assign_predicate_data_types, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
	}

	return(-1);
}



/////////////////////////////////////////////////////////////////////
////////////////		Make a deep copy of a se / pred tree
/////////////////////////////////////////////////////////////////////


//		duplicate a select element
select_element *dup_select(select_element *sl, aggregate_table *aggr_tbl){
	return new select_element(dup_se(sl->se,aggr_tbl),sl->name.c_str());
}

//		duplicate a scalar expression.
scalarexp_t *dup_se(scalarexp_t *se,
				  aggregate_table *aggr_tbl
				 ){
  int p;
  vector<scalarexp_t *> operand_list;
  vector<data_type *> dt_signature;
  scalarexp_t *ret_se, *l_se, *r_se;

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
		ret_se = new scalarexp_t(se->get_colref()->duplicate());
		ret_se->rhs.scalarp = se->rhs.scalarp;	// carry along notation
		ret_se->use_decorations_of(se);
		return(ret_se);

    case SE_UNARY_OP:
		l_se = dup_se(se->get_left_se(),  aggr_tbl);
		ret_se = new scalarexp_t(se->get_op().c_str(), l_se);
 		ret_se->use_decorations_of(se);
		return(ret_se);

    case SE_BINARY_OP:
		l_se = dup_se(se->get_left_se(), aggr_tbl);
		r_se = dup_se(se->get_right_se(), aggr_tbl);

		ret_se = new scalarexp_t(se->get_op().c_str(), l_se, r_se);
 		ret_se->use_decorations_of(se);

		return(ret_se);

    case SE_AGGR_STAR:
		ret_se = scalarexp_t::make_star_aggr(se->get_op().c_str());
 		ret_se->use_decorations_of(se);
		return(ret_se);

    case SE_AGGR_SE:
		l_se = dup_se(se->get_left_se(),  aggr_tbl);
		ret_se = scalarexp_t::make_se_aggr(se->get_op().c_str(), l_se);
 		ret_se->use_decorations_of(se);
		return(ret_se);

	case SE_FUNC:
		{
			operand_list = se->get_operands();
			vector<scalarexp_t *> new_operands;
			for(p=0;p<operand_list.size();p++){
				l_se = dup_se(operand_list[p], aggr_tbl);
				new_operands.push_back(l_se);
			}

			ret_se = new scalarexp_t(se->get_op().c_str(), new_operands);
 			ret_se->use_decorations_of(se);
			return(ret_se);
		}

	default:
		printf("INTERNAL ERROR in dup_se: operator type %d\n",se->get_operator_type());
		exit(1);
	break;
  }
  return(false);

}



predicate_t *dup_pr(predicate_t *pr,
						 aggregate_table *aggr_tbl
						 ){

  vector<literal_t *> llist;
  scalarexp_t *se_l, *se_r;
  predicate_t *pr_l, *pr_r, *ret_pr;
  vector<scalarexp_t *> op_list, new_op_list;
  int o;


	switch(pr->get_operator_type()){
	case PRED_IN:
		se_l = dup_se(pr->get_left_se(), aggr_tbl);
		ret_pr = new predicate_t(se_l, pr->get_lit_vec());
		return(ret_pr);

	case PRED_COMPARE:
		se_l = dup_se(pr->get_left_se(), aggr_tbl);
		se_r = dup_se(pr->get_right_se(),  aggr_tbl);
		ret_pr = new predicate_t(se_l, pr->get_op().c_str(), se_r);
		return(ret_pr);

	case PRED_UNARY_OP:
		pr_l = dup_pr(pr->get_left_pr(), aggr_tbl);
		ret_pr = new predicate_t(pr->get_op().c_str(), pr_l);
		return(ret_pr);

	case PRED_BINARY_OP:
		pr_l = dup_pr(pr->get_left_pr(), aggr_tbl);
		pr_r = dup_pr(pr->get_right_pr(), aggr_tbl);
		ret_pr = new predicate_t(pr->get_op().c_str(), pr_l, pr_r);
		return(ret_pr);
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			se_l = dup_se(op_list[o], aggr_tbl);
			new_op_list.push_back(se_l);
		}
		ret_pr=  new predicate_t(pr->get_op().c_str(), new_op_list);
		ret_pr->set_fcn_id(pr->get_fcn_id());
		ret_pr->is_sampling_fcn = pr->is_sampling_fcn;
		return(ret_pr);

	default:
		fprintf(stderr,"INTERNAL ERROR in dup_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return(0);

}

table_exp_t *dup_table_exp(table_exp_t *te){
	int i;
	table_exp_t *ret = new table_exp_t();

	ret->query_type = te->query_type;

	ss_map::iterator ss_i;
	for(ss_i=te->nmap.begin();ss_i!=te->nmap.end();++ss_i){
		ret->nmap[(*ss_i).first] = (*ss_i).second;
	}

	for(i=0;i<te->plist.size();++i){
		ret->plist.push_back(new
		 var_pair_t(te->plist[i]->name,te->plist[i]->val) );
	}

	if(te->sl){
		ret->sl = new select_list_t();
		ret->sl->lineno = te->sl->lineno; ret->sl->charno = te->sl->charno;
		vector<select_element *> select_list = te->sl->get_select_list();
		for(i=0;i<select_list.size();++i){
			scalarexp_t *se = dup_se(select_list[i]->se,NULL);
			ret->sl->append(se,select_list[i]->name);
		}
	}

	ret->fm = te->fm->duplicate();

	if(te->wh) ret->wh = dup_pr(te->wh,NULL);
	if(te->hv) ret->hv = dup_pr(te->hv,NULL);
	if(te->cleaning_when) ret->cleaning_when = dup_pr(te->cleaning_when,NULL);
	if(te->cleaning_by) ret->cleaning_by = dup_pr(te->cleaning_by,NULL);
	if(te->closing_when) ret->closing_when = dup_pr(te->closing_when,NULL);

	for(i=0;i<te->gb.size();++i){
		extended_gb_t *tmp_g =  te->gb[i]->duplicate();
		ret->gb.push_back(tmp_g);
	}

	ret->mergevars = te->mergevars;
	if(te->slack)
		ret->slack = dup_se(te->slack,NULL);
	ret->lineno = te->lineno;
	ret->charno = te->charno;

	return(ret);
}







/////////////////////////////////////////////////////////////////////////
//			Bind colrefs to a member of their FROM list

void bind_colref_se(scalarexp_t *se,
				  vector<tablevar_t *> &fm,
				  int prev_ref, int new_ref
				 ){
  int p;
  vector<scalarexp_t *> operand_list;
  colref_t *cr;
  ifpref_t *ir;

  switch(se->get_operator_type()){
    case SE_LITERAL:
    case SE_PARAM:
		return;
    case SE_IFACE_PARAM:
		ir = se->get_ifpref();
		if(ir->get_tablevar_ref() == prev_ref){
			ir->set_tablevar_ref(new_ref);
			ir->set_tablevar(fm[new_ref]->get_var_name());
		}
		return;

    case SE_COLREF:
		cr=se->get_colref();
		if(cr->get_tablevar_ref() == prev_ref){
			cr->set_tablevar_ref(new_ref);
//			cr->set_interface(fm[new_ref]->get_interface());
			cr->set_table_name(fm[new_ref]->get_var_name());
		}
		return;

    case SE_UNARY_OP:
		bind_colref_se(se->get_left_se(),  fm, prev_ref, new_ref);
		return;

    case SE_BINARY_OP:
		bind_colref_se(se->get_left_se(), fm, prev_ref, new_ref);
		bind_colref_se(se->get_right_se(),  fm, prev_ref, new_ref);
		return;

    case SE_AGGR_STAR:
    case SE_AGGR_SE:
		return;

	case SE_FUNC:
		if(se->get_aggr_ref() >= 0) return;

		operand_list = se->get_operands();
		for(p=0;p<operand_list.size();p++){
			bind_colref_se(operand_list[p], fm, prev_ref, new_ref);
		}
		return;

	default:
		printf("INTERNAL ERROR in bind_colref_se: operator type %d\n",se->get_operator_type());
		exit(1);
	break;
  }
  return;

}




void bind_colref_pr(predicate_t *pr,
				  vector<tablevar_t *> &fm,
				  int prev_ref, int new_ref
				 ){
  vector<scalarexp_t *> op_list;
  int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		bind_colref_se(pr->get_left_se(), fm, prev_ref, new_ref);
		return;

	case PRED_COMPARE:
		bind_colref_se(pr->get_left_se(), fm, prev_ref, new_ref);
		bind_colref_se(pr->get_right_se(),  fm, prev_ref, new_ref);
		return;

	case PRED_UNARY_OP:
		bind_colref_pr(pr->get_left_pr(), fm, prev_ref, new_ref);
		return;

	case PRED_BINARY_OP:
		bind_colref_pr(pr->get_left_pr(), fm, prev_ref, new_ref);
		bind_colref_pr(pr->get_right_pr(), fm, prev_ref, new_ref);
		return;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			bind_colref_se(op_list[o], fm, prev_ref, new_ref);
		}
		return;

	default:
		fprintf(stderr,"INTERNAL ERROR in bind_colref_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return;

}


/////////////////////////////////////////////////////////////////////
//		verify that the se refs only literals and params.
//	    (use to verify that the expression should stay in the hfta
//		 during a split)
/////////////////////////////////////////////////////////////////////

bool is_literal_or_param_only(scalarexp_t *se){
	int o;
	vector<scalarexp_t *> operands;
	bool sum = true;

	if(se == NULL) return(true);

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
		return(true);
	case SE_IFACE_PARAM:
		return(false);		// need to treat as colref
	case SE_UNARY_OP:
		return(is_literal_or_param_only(se->get_left_se()) );
	case SE_BINARY_OP:
		return(
			is_literal_or_param_only(se->get_left_se()) &&
			is_literal_or_param_only(se->get_right_se())
			);
	case SE_COLREF:
		return(false);
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
		return(false);
	case SE_FUNC:
//			The fcn might have special meaning at the lfta ...
		return(false);

	default:
		fprintf(stderr,"INTERNAL ERROR in is_literal_or_param_only, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return(0);
}



/////////////////////////////////////////////////////////////////////
//		Search for gb refs.
//	    (use to verify that no gbrefs in a gb def.)
/////////////////////////////////////////////////////////////////////


int count_gb_se(scalarexp_t *se){
	int o;
	vector<scalarexp_t *> operands;
	int sum = 0;

	if(se == NULL) return(0);

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return(0);
	case SE_UNARY_OP:
		return(count_gb_se(se->get_left_se()) );
	case SE_BINARY_OP:
		return(
			count_gb_se(se->get_left_se()) +
			count_gb_se(se->get_right_se())
			);
	case SE_COLREF:
		if(se->get_gb_ref() < 0) return(0);
		return(1);
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
		return(0);
	case SE_FUNC:
		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			sum +=  count_gb_se(operands[o]);
		}
		return(sum);

	default:
		fprintf(stderr,"INTERNAL ERROR in count_gb_se, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return(0);
}


/////////////////////////////////////////////////////////////////////
////////////////		Search for stateful fcns.
/////////////////////////////////////////////////////////////////////


int se_refs_sfun(scalarexp_t *se){
	int o;
	vector<scalarexp_t *> operands;
	int sum = 0;

	if(se == NULL) return(0);

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return(0);
	case SE_UNARY_OP:
		return(se_refs_sfun(se->get_left_se()) );
	case SE_BINARY_OP:
		return(
			se_refs_sfun(se->get_left_se()) +
			se_refs_sfun(se->get_right_se())
			);
	case SE_COLREF:
		return(0);
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
		return(0);
	case SE_FUNC:
		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			sum +=  se_refs_sfun(operands[o]);
		}
		if(se->get_aggr_ref()>=0) sum++; // is it tagged as a UDAF?

//			for now, stateful functions count as aggregates.
		if(se->get_storage_state() != "")
			sum++;

		return(sum);

	default:
		fprintf(stderr,"INTERNAL ERROR in se_refs_sfun, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return(0);
}


//		Return a count of the number of stateful fcns in this predicate.
int pred_refs_sfun(predicate_t *pr){
	vector<scalarexp_t *> op_list;
	int o, aggr_sum;

	switch(pr->get_operator_type()){
	case PRED_IN:
		return(se_refs_sfun(pr->get_left_se()) );
	case PRED_COMPARE:
		return(
			se_refs_sfun(pr->get_left_se()) +
			se_refs_sfun(pr->get_right_se())
		);
	case PRED_UNARY_OP:
		return(pred_refs_sfun(pr->get_left_pr()) );
	case PRED_BINARY_OP:
		return(
			pred_refs_sfun(pr->get_left_pr()) +
			pred_refs_sfun(pr->get_right_pr())
		);
	case PRED_FUNC:
		op_list = pr->get_op_list();
		aggr_sum = 0;
		for(o=0;o<op_list.size();++o){
			aggr_sum += se_refs_sfun(op_list[o]);
		}
		return(aggr_sum);

	default:
		fprintf(stderr,"INTERNAL ERROR in pred_refs_sfun, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return(0);
}

//////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////
////////////////		Search for aggregates.
/////////////////////////////////////////////////////////////////////


int count_aggr_se(scalarexp_t *se, bool strict){
	int o;
	vector<scalarexp_t *> operands;
	int sum = 0;

	if(se == NULL) return(0);

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return(0);
	case SE_UNARY_OP:
		return(count_aggr_se(se->get_left_se(), strict) );
	case SE_BINARY_OP:
		return(
			count_aggr_se(se->get_left_se(), strict) +
			count_aggr_se(se->get_right_se(), strict)
			);
	case SE_COLREF:
		return(0);
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
		return(1);
	case SE_FUNC:
		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			sum +=  count_aggr_se(operands[o], strict);
		}
		if(se->get_aggr_ref()>=0) sum++; // is it tagged as a UDAF?

//			now, stateful functions can count as aggregates.
//			if we are being strict.
		if(! strict && se->get_storage_state() != "")
			sum++;

		return(sum);

	default:
		fprintf(stderr,"INTERNAL ERROR in count_aggr_se, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return(0);
}


//		Return a count of the number of aggregate fcns in this predicate.
int count_aggr_pred(predicate_t *pr, bool strict){
	vector<scalarexp_t *> op_list;
	int o, aggr_sum;

	switch(pr->get_operator_type()){
	case PRED_IN:
		return(count_aggr_se(pr->get_left_se(), strict) );
	case PRED_COMPARE:
		return(
			count_aggr_se(pr->get_left_se(), strict) +
			count_aggr_se(pr->get_right_se(), strict)
		);
	case PRED_UNARY_OP:
		return(count_aggr_pred(pr->get_left_pr(), strict) );
	case PRED_BINARY_OP:
		return(
			count_aggr_pred(pr->get_left_pr(), strict) +
			count_aggr_pred(pr->get_right_pr(), strict)
		);
	case PRED_FUNC:
		op_list = pr->get_op_list();
		aggr_sum = 0;
		for(o=0;o<op_list.size();++o){
			aggr_sum += count_aggr_se(op_list[o], strict);
		}
		return(aggr_sum);

	default:
		fprintf(stderr,"INTERNAL ERROR in count_aggr_pred, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return(0);
}

//////////////////////////////////////////////////
///		Analyze tablevar refs

void get_tablevar_ref_se(scalarexp_t *se, vector<int> &reflist){
	int o;
	vector<scalarexp_t *> operands;
	int vref;
	colref_t *cr;
	ifpref_t *ir;

	if(se == NULL) return;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
		return;
	case SE_IFACE_PARAM:
		ir = se->get_ifpref();
		vref = ir->get_tablevar_ref();
		for(o=0;o<reflist.size();++o){
			if(vref == reflist[o]) return;
		}
		reflist.push_back(vref);
		return;
	case SE_UNARY_OP:
		get_tablevar_ref_se(se->get_left_se(), reflist);
		return;
	case SE_BINARY_OP:
		get_tablevar_ref_se(se->get_left_se(), reflist);
		get_tablevar_ref_se(se->get_right_se(), reflist);
		return;
	case SE_COLREF:
		if(se->is_gb()) return;
		cr = se->get_colref();
		vref = cr->get_tablevar_ref();
		for(o=0;o<reflist.size();++o){
			if(vref == reflist[o]) return;
		}
		reflist.push_back(vref);
		return;
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
		return;
	case SE_FUNC:
		if(se->get_aggr_ref() >= 0) return;

		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			get_tablevar_ref_se(operands[o], reflist);
		}
		return;

	default:
		fprintf(stderr,"INTERNAL ERROR in get_tablevar_ref_se, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return;
}


void get_tablevar_ref_pr(predicate_t *pr, vector<int> &reflist){
	vector<scalarexp_t *> op_list;
	int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		get_tablevar_ref_se(pr->get_left_se(),reflist);
		return;
	case PRED_COMPARE:
		get_tablevar_ref_se(pr->get_left_se(),reflist);
		get_tablevar_ref_se(pr->get_right_se(),reflist);
		return;
	case PRED_UNARY_OP:
		get_tablevar_ref_pr(pr->get_left_pr(),reflist);
		return;
	case PRED_BINARY_OP:
		get_tablevar_ref_pr(pr->get_left_pr(),reflist);
		get_tablevar_ref_pr(pr->get_right_pr(),reflist);
		return;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			get_tablevar_ref_se(op_list[o],reflist);
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in get_tablevar_ref_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
	}

	return;
}


//			Walk SE tree and gather STATES ref'd by STATEFUL fcns.

void gather_fcn_states_se(scalarexp_t *se, set<string> &states_refd, ext_fcn_list *Ext_fcns){
	int agg_id;
	int o;
	vector<scalarexp_t *> operands;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return;
	case SE_UNARY_OP:
		gather_fcn_states_se(se->get_left_se(), states_refd, Ext_fcns) ;
		return;
	case SE_BINARY_OP:
		gather_fcn_states_se(se->get_left_se(), states_refd, Ext_fcns);
		gather_fcn_states_se(se->get_right_se(), states_refd,Ext_fcns);
		return;
	case SE_COLREF:
		return;
	case SE_AGGR_STAR:
		return;
	case SE_AGGR_SE:
		gather_fcn_states_se(se->get_left_se(), states_refd, Ext_fcns);
		return;
	case SE_FUNC:
		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			gather_fcn_states_se(operands[o], states_refd, Ext_fcns);
		}
		if(se->get_storage_state() != ""){
			states_refd.insert(se->get_storage_state());
		}
		return;

	default:
		fprintf(stderr,"INTERNAL ERROR in gather_fcn_states_se, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return;
}


//			Walk SE tree and gather STATES ref'd by STATEFUL fcns.

void gather_fcn_states_pr(predicate_t *pr, set<string> &states_refd, ext_fcn_list *Ext_fcns){
	vector<scalarexp_t *> op_list;
	int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		gather_fcn_states_se(pr->get_left_se(),states_refd, Ext_fcns) ;
		return;
	case PRED_COMPARE:
		gather_fcn_states_se(pr->get_left_se(),states_refd, Ext_fcns) ;
		gather_fcn_states_se(pr->get_right_se(),states_refd, Ext_fcns) ;
		return;
	case PRED_UNARY_OP:
		gather_fcn_states_pr(pr->get_left_pr(),states_refd, Ext_fcns);
		return;
	case PRED_BINARY_OP:
		gather_fcn_states_pr(pr->get_left_pr(),states_refd, Ext_fcns) ;
		gather_fcn_states_pr(pr->get_right_pr(),states_refd, Ext_fcns) ;
		return;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			gather_fcn_states_se(op_list[o],states_refd, Ext_fcns);
		}
		return;

	default:
		fprintf(stderr,"INTERNAL ERROR in gather_fcn_states_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return;
}




//			walk se tree and collect aggregates into aggregate table.
//			duplicate aggregates receive the same idx to the table.

void build_aggr_tbl_fm_se(scalarexp_t *se, aggregate_table *aggregate_table, ext_fcn_list *Ext_fcns){
	int agg_id;
	int o;
	vector<scalarexp_t *> operands;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return;
	case SE_UNARY_OP:
		build_aggr_tbl_fm_se(se->get_left_se(), aggregate_table, Ext_fcns) ;
		return;
	case SE_BINARY_OP:
		build_aggr_tbl_fm_se(se->get_left_se(), aggregate_table, Ext_fcns);
		build_aggr_tbl_fm_se(se->get_right_se(), aggregate_table,Ext_fcns);
		return;
	case SE_COLREF:
		return;
	case SE_AGGR_STAR:
		agg_id = aggregate_table->add_aggr(se->get_op(),NULL,se->is_superaggr());
		se->set_aggr_id(agg_id);
		return;
	case SE_AGGR_SE:
		agg_id = aggregate_table->add_aggr(se->get_op(),se->get_left_se(),se->is_superaggr());
		se->set_aggr_id(agg_id);
		return;
	case SE_FUNC:
		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			build_aggr_tbl_fm_se(operands[o], aggregate_table, Ext_fcns);
		}
		if(se->get_aggr_ref() >= 0){ //	it's been tagged as a UDAF
			agg_id = aggregate_table->add_aggr(se->get_op(), se->get_fcn_id(), operands, Ext_fcns->get_storage_dt(se->get_fcn_id()), se->is_superaggr(), Ext_fcns->is_running_aggr(se->get_fcn_id()),Ext_fcns->has_lfta_bailout(se->get_fcn_id()));
			se->set_aggr_id(agg_id);
		}
		return;

	default:
		fprintf(stderr,"INTERNAL ERROR in build_aggr_tbl_fm_se, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return;
}


//			walk se tree and collect aggregates into aggregate table.
//			duplicate aggregates receive the same idx to the table.

void build_aggr_tbl_fm_pred(predicate_t *pr, aggregate_table *aggregate_table,ext_fcn_list *Ext_fcns){
	vector<scalarexp_t *> op_list;
	int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		build_aggr_tbl_fm_se(pr->get_left_se(),aggregate_table, Ext_fcns) ;
		return;
	case PRED_COMPARE:
		build_aggr_tbl_fm_se(pr->get_left_se(),aggregate_table, Ext_fcns) ;
		build_aggr_tbl_fm_se(pr->get_right_se(),aggregate_table, Ext_fcns) ;
		return;
	case PRED_UNARY_OP:
		build_aggr_tbl_fm_pred(pr->get_left_pr(),aggregate_table, Ext_fcns);
		return;
	case PRED_BINARY_OP:
		build_aggr_tbl_fm_pred(pr->get_left_pr(),aggregate_table, Ext_fcns) ;
		build_aggr_tbl_fm_pred(pr->get_right_pr(),aggregate_table, Ext_fcns) ;
		return;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			build_aggr_tbl_fm_se(op_list[o],aggregate_table, Ext_fcns);
		}
		return;

	default:
		fprintf(stderr,"INTERNAL ERROR in build_aggr_tbl_fm_pred, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return;
}


//			Return true if the two scalar expressions
//			represent the same value (e.g., use to eliminate
//			duplicate aggregates).
bool is_equivalent_se(scalarexp_t *se1, scalarexp_t *se2){
	vector<scalarexp_t *> operands1;
	vector<scalarexp_t *> operands2;
	int o;

//		First handle the case of nulls (e.g. COUNT aggrs)
	if(se1 == NULL && se2 == NULL) return(true);
	if(se1 == NULL || se2 == NULL) return(false);

//		In all cases, must be the same oeprator type and same operator.
	if(se1->get_operator_type() != se2->get_operator_type())
		return(false);
	if(se1->get_op() != se2->get_op() )
		return(false);

	switch(se1->get_operator_type()){
	case SE_LITERAL:
		return(se1->get_literal()->is_equivalent(se2->get_literal()) );
	case SE_PARAM:
		return(se1->get_param_name() == se2->get_param_name() );
	case SE_IFACE_PARAM:
		return(se1->get_ifpref()->is_equivalent(se2->get_ifpref()) );
	case SE_UNARY_OP:
		return(is_equivalent_se(se1->get_left_se(), se2->get_left_se()) );
	case SE_BINARY_OP:
		if(is_equivalent_se(se1->get_left_se(), se2->get_left_se()) )
			return(is_equivalent_se(se1->get_right_se(), se2->get_right_se()) );
		return(false);
	case SE_COLREF:
		if(se1->is_gb() && se2->is_gb())
			return( se1->get_gb_ref() == se2->get_gb_ref() );
		if(se1->is_gb() || se2->is_gb())
			return(false);
		return(se1->get_colref()->is_equivalent(se2->get_colref()) );
	case SE_AGGR_STAR:
		return(true);
	case SE_AGGR_SE:
		return(is_equivalent_se(se1->get_left_se(), se2->get_left_se()) );
	case SE_FUNC:
		if(se1->get_op() != se2->get_op()) return(false);

		operands1 = se1->get_operands();
		operands2 = se2->get_operands();
		if(operands1.size() != operands2.size()) return(false);

		for(o=0;o<operands1.size();o++){
			if(! is_equivalent_se(operands1[o], operands2[o]) )
				return(false);
		}
		return(true);
	default:
		fprintf(stderr,"INTERNAL ERROR in is_equivalent_se, line %d, character %d: unknown operator type %d\n",
				se1->get_lineno(), se1->get_charno(),se1->get_operator_type());
		exit(1);
	}
	return(false);
}


//		Similar to is_equivalent_se, but with a looser definition
//		of equivalence of colrefs.  Here, say they are equivalent
//		if their base table is the same.  Use to find equivalent
//		predicates on base tables.
bool is_equivalent_se_base(scalarexp_t *se1, scalarexp_t *se2, table_list *Schema){
	vector<scalarexp_t *> operands1;
	vector<scalarexp_t *> operands2;
	int o;

	if(se1->get_operator_type() == SE_COLREF && se1->is_gb()){
		se1 = se1->get_right_se();
	}
	if(se2->get_operator_type() == SE_COLREF && se2->is_gb()){
		se2 = se2->get_right_se();
	}

//		First handle the case of nulls (e.g. COUNT aggrs)
	if(se1 == NULL && se2 == NULL) return(true);
	if(se1 == NULL || se2 == NULL) return(false);

//		In all cases, must be the same oeprator type and same operator.
	if(se1->get_operator_type() != se2->get_operator_type())
		return(false);
	if(se1->get_op() != se2->get_op() )
		return(false);

	switch(se1->get_operator_type()){
	case SE_LITERAL:
		return(se1->get_literal()->is_equivalent(se2->get_literal()) );
	case SE_PARAM:
		return(se1->get_param_name() == se2->get_param_name() );
	case SE_IFACE_PARAM:
		return(se1->get_ifpref()->is_equivalent(se2->get_ifpref()) );
	case SE_UNARY_OP:
		return(is_equivalent_se_base(se1->get_left_se(), se2->get_left_se(), Schema) );
	case SE_BINARY_OP:
		if(is_equivalent_se_base(se1->get_left_se(), se2->get_left_se(), Schema) )
			return(is_equivalent_se_base(se1->get_right_se(), se2->get_right_se(), Schema) );
		return(false);
	case SE_COLREF:
/*
		if(se1->is_gb() && se2->is_gb())
			return( se1->get_gb_ref() == se2->get_gb_ref() );
		if(se1->is_gb() || se2->is_gb())
			return(false);
*/
		return(se1->get_colref()->is_equivalent_base(se2->get_colref(), Schema) );
	case SE_AGGR_STAR:
		return(true);
	case SE_AGGR_SE:
		return(is_equivalent_se_base(se1->get_left_se(), se2->get_left_se(), Schema) );
	case SE_FUNC:
		if(se1->get_op() != se2->get_op()) return(false);

		operands1 = se1->get_operands();
		operands2 = se2->get_operands();
		if(operands1.size() != operands2.size()) return(false);

		for(o=0;o<operands1.size();o++){
			if(! is_equivalent_se_base(operands1[o], operands2[o], Schema) )
				return(false);
		}
		return(true);
	default:
		fprintf(stderr,"INTERNAL ERROR in is_equivalent_se, line %d, character %d: unknown operator type %d\n",
				se1->get_lineno(), se1->get_charno(),se1->get_operator_type());
		exit(1);
	}
	return(false);
}


//		Find predicates which are equivalent when
//		looking at the base tables.  Use to find
//		common prefilter.
bool is_equivalent_pred_base(predicate_t *p1, predicate_t *p2, table_list *Schema){
int i, o;

//		First handle the case of nulls
	if(p1 == NULL && p2 == NULL) return(true);
	if(p1 == NULL || p2 == NULL) return(false);


  if(p1->get_operator_type() != p2->get_operator_type())
         return(false);
  if(p1->get_op() != p2->get_op())
         return(false);

    vector<literal_t *> ll1;
    vector<literal_t *> ll2;
	vector<scalarexp_t *> op_list1, op_list2;


  switch(p2->get_operator_type()){
     case PRED_COMPARE:
        if( ! is_equivalent_se_base(p1->get_left_se(),p2->get_left_se(), Schema) )
            return(false);
        return( is_equivalent_se_base(p1->get_right_se(),p2->get_right_se(), Schema) );
    break;
    case PRED_IN:
        if( ! is_equivalent_se_base(p1->get_left_se(),p2->get_left_se(), Schema) )
            return(false);
        ll1 = p1->get_lit_vec();
        ll2 = p2->get_lit_vec();
        if(ll1.size() != ll2.size())
            return(false);
        for(i=0;i<ll1.size();i++){
          if(! ll1[i]->is_equivalent( ll2[i] ) )
            return(false);
        }
        return(true);
    break;
     case PRED_UNARY_OP:
        return(is_equivalent_pred_base(p1->get_left_pr(), p2->get_left_pr(), Schema) );
    break;
     case PRED_BINARY_OP:
        if(! is_equivalent_pred_base(p1->get_left_pr(), p2->get_left_pr(), Schema))
            return(false);
        return(is_equivalent_pred_base(p1->get_right_pr(), p2->get_right_pr(), Schema) );
    break;
	 case PRED_FUNC:
		op_list1 = p1->get_op_list();
		op_list2 = p2->get_op_list();
		if(op_list1.size() != op_list2.size()) return(false);
		for(o=0;o<op_list1.size();++o){
			if(! is_equivalent_se_base(op_list1[o],op_list2[o], Schema) ) return(false);
		}
		return(true);

   }

    return(false);
}



bool is_equivalent_class_pred_base(predicate_t *p1, predicate_t *p2, table_list *Schema,ext_fcn_list *Ext_fcns){
  if((p1->get_operator_type()!=PRED_FUNC)||(p2->get_operator_type()!=PRED_FUNC))
         return(false);
  if(p1->get_fcn_id() != p2->get_fcn_id())
		return false;
  vector<bool> cl_op = Ext_fcns->get_class_indicators(p1->get_fcn_id());
  int o;
  vector<scalarexp_t *> op_list1 = p1->get_op_list();
  vector<scalarexp_t *> op_list2 = p2->get_op_list();
  if(op_list1.size() != op_list2.size()) return(false);
  for(o=0;o<op_list1.size();++o){
	  if(cl_op[o]){
		if(! is_equivalent_se_base(op_list1[o],op_list2[o], Schema) )
			return(false);
	}
  }
  return true;

}




//			Verify that the scalar expression (in a such that clause)
//			is acceptable in an aggregation query.  No column
//			references allowed outside aggergates, except for
//			references to group-by attributes.
//			return true if OK, false if bad.
bool verify_aggr_query_se(scalarexp_t *se){
	vector <scalarexp_t *> operands;
	int o;

    switch(se->get_operator_type()){
    case SE_LITERAL:
    case SE_PARAM:
    case SE_IFACE_PARAM:
        return(true );
    case SE_UNARY_OP:
        return(verify_aggr_query_se(se->get_left_se() ) );
    case SE_BINARY_OP:
        return(verify_aggr_query_se(se->get_left_se() ) &&
            verify_aggr_query_se(se->get_right_se() ) );
    case SE_COLREF:
        if(se->is_gb() ) return(true);
        fprintf(stderr,"ERROR: the select clause in an aggregate query can "
                        "only reference constants, group-by attributes, and "
                        "aggregates,  (%s) line %d, character %d.\n",
                        se->get_colref()->to_string().c_str(),
						se->get_lineno(), se->get_charno() );
        return(false);
    case SE_AGGR_STAR:
    case SE_AGGR_SE:
//			colrefs and gbrefs allowed.
//			check for nested aggregation elsewhere, so just return TRUE
        return(true);
	case SE_FUNC:
//			If its a UDAF, just return true
		if(se->get_aggr_ref() >= 0) return true;

		operands = se->get_operands();

		for(o=0;o<operands.size();o++){
			if(! verify_aggr_query_se(operands[o]) )
				return(false);
		}
		return(true);
    default:
        fprintf(stderr,"INTERNAL ERROR in verify_aggr_query_se, line %d, character %d: unknown operator type %d\n",
                se->get_lineno(), se->get_charno(),se->get_operator_type());
        exit(1);
    }
    return(false);
}




//			Find complex literals.
//			NOTE : This analysis should be deferred to
//				   code generation time.
//			This analysis drills into aggr se specs.
//			Shouldn't this be done at the aggregate table?
//			But, its not a major loss of efficiency.
//				UPDATE : drilling into aggr se's is causnig a problem
//					so I've eliminated it.

bool find_complex_literal_se(scalarexp_t *se, ext_fcn_list *Ext_fcns,
								cplx_lit_table *complex_literals){
	literal_t *l;
	vector<scalarexp_t *> operands;
	int o;
	scalarexp_t *param_se;
	data_type *dt;

	switch(se->get_operator_type()){
	case SE_LITERAL:
		l = se->get_literal();
		if(l->constructor_name() != ""){
			int cl_idx = complex_literals->add_cpx_lit(l, false);
			l->set_cpx_lit_ref(cl_idx);
		}
		return(true);
	case SE_PARAM:
		return(true );
//			SE_IFACE_PARAM should not exist when this is called.
	case SE_UNARY_OP:
		return(find_complex_literal_se(se->get_left_se(), Ext_fcns, complex_literals ) );
	case SE_BINARY_OP:
		return(find_complex_literal_se(se->get_left_se(), Ext_fcns, complex_literals ) &&
			find_complex_literal_se(se->get_right_se(), Ext_fcns, complex_literals ) );
	case SE_COLREF:
		return(true);
	case SE_AGGR_STAR:
		return(true);
	case SE_AGGR_SE:
		return true;
//		return(find_complex_literal_se(se->get_left_se(), Ext_fcns, complex_literals ) );
	case SE_FUNC:
		if(se->get_aggr_ref() >= 0) return true;

		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			find_complex_literal_se(operands[o], Ext_fcns, complex_literals);
		}
		return(true);
	default:
		fprintf(stderr,"INTERNAL ERROR in find_complex_literal_se, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return(false);
}




void find_complex_literal_pr(predicate_t *pr, ext_fcn_list *Ext_fcns,
								cplx_lit_table *complex_literals){
	int i,o;
	vector<literal_t *> litl;
	vector<scalarexp_t *> op_list;


	switch(pr->get_operator_type()){
	case PRED_IN:
		find_complex_literal_se(pr->get_left_se(), Ext_fcns, complex_literals) ;
		litl = pr->get_lit_vec();
		for(i=0;i<litl.size();i++){
			if(litl[i]->constructor_name() != ""){
				int cl_idx = complex_literals->add_cpx_lit(litl[i],false);
				litl[i]->set_cpx_lit_ref(cl_idx);
			}
		}
		return;
	case PRED_COMPARE:
		find_complex_literal_se(pr->get_left_se(), Ext_fcns, complex_literals) ;
		find_complex_literal_se(pr->get_right_se(), Ext_fcns, complex_literals) ;
		return;
	case PRED_UNARY_OP:
		find_complex_literal_pr(pr->get_left_pr(), Ext_fcns, complex_literals);
		return;
	case PRED_BINARY_OP:
		find_complex_literal_pr(pr->get_left_pr(), Ext_fcns, complex_literals) ;
		find_complex_literal_pr(pr->get_right_pr(), Ext_fcns, complex_literals) ;
		return;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			find_complex_literal_se(op_list[o],Ext_fcns, complex_literals);
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in find_complex_literal_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return;
}


//		Find all things which are passed as handle parameters to functions
//		(query parameters, (simple) literals, complex literals)
//		These expressions MUST be processed with find_complex_literal_??
//		first.
//			TODO: this analysis drills into the aggregate SEs.
//			Shouldn't this be done on the aggr table SEs instead?
//			to avoid duplication.  THe handle registration
//			might be expensive ...
//			REVISED : drilling into aggr se's is causing problems, eliminated.

void find_param_handles_se(scalarexp_t *se, ext_fcn_list *Ext_fcns,
						vector<handle_param_tbl_entry *> &handle_tbl){
	vector<scalarexp_t *> operands;
	vector<bool> handle_ind;
	int o;
	scalarexp_t *param_se;
	data_type *dt;
	literal_t *l;

	switch(se->get_operator_type()){
	case SE_LITERAL:
		return;
	case SE_PARAM:
		return;
//		case SE_IFACE_PARAM:		SHOULD NOT EXIST when this is called
	case SE_UNARY_OP:
		find_param_handles_se(se->get_left_se(), Ext_fcns, handle_tbl ) ;
		return;
	case SE_BINARY_OP:
		find_param_handles_se(se->get_left_se(), Ext_fcns , handle_tbl) ;
		find_param_handles_se(se->get_right_se(), Ext_fcns, handle_tbl ) ;
		return;
	case SE_COLREF:
		return;
	case SE_AGGR_STAR:
		return;
	case SE_AGGR_SE:
//		find_param_handles_se(se->get_left_se(), Ext_fcns, handle_tbl ) ;
		return;
	case SE_FUNC:
		if(se->get_aggr_ref() >= 0) return ;

		operands = se->get_operands();
		handle_ind = Ext_fcns->get_handle_indicators(se->get_fcn_id() );
		for(o=0;o<operands.size();o++){
			if(handle_ind[o]){
				handle_param_tbl_entry *he;
				param_se = operands[o];
				if(param_se->get_operator_type() != SE_LITERAL &&
						param_se->get_operator_type() != SE_PARAM){
					fprintf(stderr,"ERROR, the %d-th parameter of function %s must be a literal or query parameter (because it is a pass-by-HANDLE parameter).\n  Line=%d, char=%d.\n",
				o+1, se->get_op().c_str(),se->get_lineno(), se->get_charno());
					exit(1);
				}

				if(param_se->get_operator_type() == SE_PARAM){
					he = new handle_param_tbl_entry(
						se->get_op(), o, param_se->get_param_name(),
						param_se->get_data_type()->get_type_str());
				}else{
					l = param_se->get_literal();
					if(l->is_cpx_lit()){
						he = new handle_param_tbl_entry(
							se->get_op(), o, l->get_cpx_lit_ref(),
						param_se->get_data_type()->get_type_str());
					}else{
						he = new handle_param_tbl_entry(
							se->get_op(), o, l,
						param_se->get_data_type()->get_type_str());
					}
				}
				param_se->set_handle_ref(handle_tbl.size());
				handle_tbl.push_back(he);
			}else{
				find_param_handles_se(operands[o], Ext_fcns, handle_tbl ) ;
			}
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in find_param_handles, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return;
}


void find_param_handles_pr(predicate_t *pr, ext_fcn_list *Ext_fcns,
						vector<handle_param_tbl_entry *> &handle_tbl){
	vector<literal_t *> litl;
	vector<scalarexp_t *> op_list;
	scalarexp_t *param_se;
	vector<bool> handle_ind;
	int o;
	literal_t *l;

	switch(pr->get_operator_type()){
	case PRED_IN:
		find_param_handles_se(pr->get_left_se(), Ext_fcns, handle_tbl) ;
		return;
	case PRED_COMPARE:
		find_param_handles_se(pr->get_left_se(), Ext_fcns, handle_tbl) ;
		find_param_handles_se(pr->get_right_se(), Ext_fcns, handle_tbl) ;
		return;
	case PRED_UNARY_OP:
		find_param_handles_pr(pr->get_left_pr(), Ext_fcns, handle_tbl);
		return;
	case PRED_BINARY_OP:
		find_param_handles_pr(pr->get_left_pr(), Ext_fcns, handle_tbl) ;
		find_param_handles_pr(pr->get_right_pr(), Ext_fcns, handle_tbl) ;
		return;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		handle_ind = Ext_fcns->get_handle_indicators(pr->get_fcn_id() );
		for(o=0;o<op_list.size();++o){
			if(handle_ind[o]){
				handle_param_tbl_entry *he;
				param_se = op_list[o];
				if(param_se->get_operator_type() != SE_LITERAL &&
						param_se->get_operator_type() != SE_PARAM){
					fprintf(stderr,"ERROR, the %d-th parameter of predicate %s must be a literal or query parameter (because it is a pass-by-HANDLE parameter).\n  Line=%d, char=%d.\n",
				o+1, pr->get_op().c_str(),pr->get_lineno(), pr->get_charno());
					exit(1);
				}

				if(param_se->get_operator_type() == SE_PARAM){
					he = new handle_param_tbl_entry(
						pr->get_op(), o, param_se->get_param_name(),
						param_se->get_data_type()->get_type_str());
				}else{
					l = param_se->get_literal();
					if(l->is_cpx_lit()){
						he = new handle_param_tbl_entry(
							pr->get_op(), o, l->get_cpx_lit_ref(),
						param_se->get_data_type()->get_type_str());
					}else{
						he = new handle_param_tbl_entry(
							pr->get_op(), o, l,
						param_se->get_data_type()->get_type_str());
					}
				}
				param_se->set_handle_ref(handle_tbl.size());
				handle_tbl.push_back(he);
			}else{
				find_param_handles_se(op_list[o], Ext_fcns, handle_tbl ) ;
			}
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in find_param_handles_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return;
}


//			Verify the HAVING predicate : it
//			can access gb vars, aggregates, and constants,
//			but not colrefs.
//			return 1 if OK, -1 if bad.
//			Perhaps replace by a pair of fcns which counts non-gb colrefs?

//			Extended to deal with cleaning_by, cleaning_when :
//			verify that any aggregate function
//			has the multiple output property.

int verify_having_se(scalarexp_t *se, const char *clause, ext_fcn_list *Ext_fcns){
	int l_ret, r_ret;
	vector<scalarexp_t *> operands;
	vector<data_type *> odt;
	int o;

	switch(se->get_operator_type()){
	case SE_LITERAL:
		return(1);
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return(1);
	case SE_UNARY_OP:
		return(verify_having_se(se->get_left_se(), clause, Ext_fcns) );
	case SE_BINARY_OP:
		l_ret = verify_having_se(se->get_left_se(), clause, Ext_fcns);
		r_ret = verify_having_se(se->get_right_se(), clause, Ext_fcns);
		if( (l_ret < 0 ) || (r_ret < 0) ) return(-1);
		return(1);
	case SE_COLREF:
		if(se->is_gb()) return 1;
		fprintf(stderr,"ERROR, %s clause references a non-group by attribute line =%d, char = %d, colref=%s\n", clause,
			se->get_colref()->get_lineno(),se->get_colref()->get_charno(), se->get_colref()->to_string().c_str() );
		return(-1);
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
//			colrefs and gbrefs allowed.
//			check for nested aggregation elsewhere, so just return TRUE
		if(!se->is_superaggr() && !strcmp(clause,"CLEANING_WHEN")){
			fprintf(stderr,"ERROR, %s clause references a superaggregate, line =%d, char = %d, op=%s\n", clause,
				se->get_lineno(),se->get_charno(), se->get_op().c_str() );
			return(-1);
		}

//				Ensure that aggregate refs allow multiple outputs
//				in CLEANING_WHEN, CLEANING_BY
		if(!strcmp(clause,"CLEANING_WHEN") || !strcmp(clause,"CLEANING_BY")){
			if(! aggr_table_entry::multiple_return_allowed(true,Ext_fcns,se->get_fcn_id())){
				fprintf(stderr,"ERROR, the %s clause references aggregate %s, which does not allow multiple outputs, line=%d, char=%d\n",clause,
				  se->get_op().c_str(),se->get_lineno(),se->get_charno() );
				return(-1);
			}
		}


		return(1);
	case SE_FUNC:
		if(se->get_aggr_ref() >= 0 && !se->is_superaggr() && !strcmp(clause,"CLEANING_WHEN")){
			fprintf(stderr,"ERROR, %s clause references a superaggregate, line =%d, char = %d, op=%s\n", clause,
			se->get_colref()->get_lineno(),se->get_colref()->get_charno(), se->get_op().c_str() );
		return(-1);
		}

		if(!strcmp(clause,"CLEANING_WHEN") || !strcmp(clause,"CLEANING_BY")){
			if(se->get_aggr_ref() >= 0  && ! aggr_table_entry::multiple_return_allowed(true,Ext_fcns,se->get_fcn_id())){
				fprintf(stderr,"ERROR, the %s clause references aggregate %s, which does not allow multiple outputs, line=%d, char=%d\n",clause,
				  se->get_op().c_str(),se->get_lineno(),se->get_charno() );
				return(-1);
			}
		}

		if(se->get_aggr_ref() >= 0)	// don't descent into aggregates.
			return 1;

		operands = se->get_operands();
		r_ret = 1;
		for(o=0;o<operands.size();o++){
			l_ret = verify_having_se(operands[o], clause, Ext_fcns);
			if(l_ret < 0) r_ret = -1;
		}
		if(r_ret < 0) return(-1); else return(1);
		return(1);
	default:
		fprintf(stderr,"INTERNAL ERROR in verify_having_se, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		return(-1);
	}
	return(-1);
}


//			Verify the HAVING predicate : it
//			can access gb vars, aggregates, and constants,
//			but not colrefs.
//			return 1 if OK, -1 if bad.
//			Perhaps replace by a pair of fcns which counts non-gb colrefs?


int verify_having_pred(predicate_t *pr, const char *clause, ext_fcn_list *Ext_fcns){
	int l_ret, r_ret;
	vector<literal_t *> litl;
	vector<scalarexp_t *> op_list;
	int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		return(verify_having_se(pr->get_left_se(), clause, Ext_fcns));
	case PRED_COMPARE:
		l_ret = verify_having_se(pr->get_left_se(), clause, Ext_fcns) ;
		r_ret = verify_having_se(pr->get_right_se(), clause, Ext_fcns) ;
		if( (l_ret < 0) || (r_ret < 0) ) return(-1); else return(1);
	case PRED_UNARY_OP:
		return(verify_having_pred(pr->get_left_pr(), clause, Ext_fcns));
	case PRED_BINARY_OP:
		l_ret = verify_having_pred(pr->get_left_pr(), clause, Ext_fcns);
		r_ret = verify_having_pred(pr->get_right_pr(), clause, Ext_fcns);
		if( (l_ret < 0) || (r_ret < 0) ) return(-1);
		return(1);
	case PRED_FUNC:
		op_list = pr->get_op_list();
		l_ret = 1;
		for(o=0;o<op_list.size();++o){
			if( verify_having_se(op_list[o], clause, Ext_fcns) < 0) l_ret = -1;
		}
		return(l_ret);

	default:
		fprintf(stderr,"INTERNAL ERROR in verify_having_pred, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
	}

	return(-1);
}


//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
///////			cnf and pred analysis and manipulation

// ----------------------------------------------------------------------
//  Convert the predicates to a list of conjuncts
//  (not actually cnf).  Do some analysis
//  on their properties.
// ----------------------------------------------------------------------


//  Put into list clist the predicates that
//  are AND'ed together.

void make_cnf_from_pr(predicate_t *pr, vector<cnf_elem *> &clist){

  if(pr == NULL) return;

  switch(pr->get_operator_type()){
     case PRED_COMPARE:
        clist.push_back(new cnf_elem(pr));
        return;
        break;
     case PRED_IN:
        clist.push_back(new cnf_elem(pr));
        return;
        break;
     case PRED_UNARY_OP:
        clist.push_back(new cnf_elem(pr));
        return;
        break;
     case PRED_BINARY_OP:
        if(pr->get_op() == "OR"){
			clist.push_back(new cnf_elem(pr));
			return;
		}
		if(pr->get_op() =="AND"){
		   make_cnf_from_pr(pr->get_left_pr(),clist);
		   make_cnf_from_pr(pr->get_right_pr(),clist);
		   return;
		}
	case PRED_FUNC:
        clist.push_back(new cnf_elem(pr));
        return;
        break;
	default:
		fprintf(stderr,"INTERNAL ERROR in make_cnf_from_pr: I don't recognize predicate operator %s\n",pr->get_op().c_str());
		exit(1);
			break;
	   }
}



//  Find out what things are referenced in a se,
//  to use for analyzing a predicate.
//  Currently, is it simple (no operators), does it
//  reference a group-by column, does it reference an
//  attribute of a table.
//
//	analyze_cnf_se and analyze_cnf_pr are called by analyze_cnf


void analyze_cnf_se(scalarexp_t *se, int &s, int &g, int &a, int &agr){
 int p;
 vector<scalarexp_t *> operand_list;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return;
	case SE_COLREF:
		if(se->is_gb() ) g=1;
		else			a=1;
		return;
	case SE_UNARY_OP:
		s=0;
		analyze_cnf_se(se->get_left_se(),s,g,a,agr);
		return;
	case SE_BINARY_OP:
		s=0;
		analyze_cnf_se(se->get_left_se(),s,g,a,agr);
		analyze_cnf_se(se->get_right_se(),s,g,a,agr);
		return;
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
		agr = 1;
		return;
	case SE_FUNC:
		if(se->get_aggr_ref() >= 0){
			agr = 1;
			return;
		}
		s = 0;
		operand_list = se->get_operands();
		for(p=0;p<operand_list.size();p++){
			analyze_cnf_se(operand_list[p],s,g,a,agr);
		}
	break;
	}

	return;
}



void analyze_cnf_pr(predicate_t *pr, int &g, int &a,  int &agr){
int dum_simple, o;
vector<scalarexp_t *> op_list;


	switch(pr->get_operator_type()){
	case PRED_COMPARE:
		analyze_cnf_se(pr->get_left_se(),dum_simple,g,a,agr);
		analyze_cnf_se(pr->get_right_se(),dum_simple,g,a,agr);
		return;
	case PRED_IN:
		analyze_cnf_se(pr->get_left_se(),dum_simple,g,a,agr);
		return;
	case PRED_UNARY_OP:
		analyze_cnf_pr(pr->get_left_pr(),g,a,agr);
		return;
	case PRED_BINARY_OP:
		analyze_cnf_pr(pr->get_left_pr(),g,a,agr);
		analyze_cnf_pr(pr->get_right_pr(),g,a,agr);
		return;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			analyze_cnf_se(op_list[o],dum_simple,g,a,agr);
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in analyze_cnf_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}
}



//  analyze a conjunct of a predicate.
//  Is it atomic (e.g., a single predicate),
//  and if so do a further analysis.

void analyze_cnf(cnf_elem *c){

//  analyze the predicate.
   analyze_cnf_pr(c->pr, c->pr_gb, c->pr_attr, c->pr_aggr);

   if((c->pr->get_operator_type()!= PRED_COMPARE) && (c->pr->get_operator_type()!= PRED_IN)){
		return;
   }


//  its an atomic predicate -- get more info
   c->is_atom = 1;

	if(c->pr->get_op() == "=")
		c->eq_pred = 1;
	else
		c->eq_pred = 0;

	if(c->pr->get_operator_type() == PRED_IN)
		c->in_pred = 1;
	else
		c->in_pred = 0;

	c->l_simple = 1; c->l_gb = c->l_attr = c->l_aggr = 0;
	analyze_cnf_se(c->pr->get_left_se(),c->l_simple,c->l_gb,c->l_attr, c->l_aggr);

	if(c->pr->get_operator_type() == PRED_COMPARE){
		c->r_simple = 1; c->r_gb = c->r_attr = c->r_aggr = 0;
		analyze_cnf_se(c->pr->get_left_se(),c->r_simple,c->r_gb,c->r_attr, c->r_aggr);
	}
}

void analyze_constraint_se(scalarexp_t *se,
			int &n_agr, int &n_gb, int &n_par, int &n_func, int &n_op, ext_fcn_list *Ext_fcns, bool enter_gb){
 int l_agr, l_gb, l_par, l_func, l_op;
 int r_agr, r_gb, r_par, r_func, r_op;
 int p;
 vector<scalarexp_t *> operand_list;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_IFACE_PARAM:
		n_agr=0; n_gb = 0; n_par = 0; n_func = 0; n_op = 0;
		return;
	case SE_PARAM:
		n_agr=0; n_gb = 0; n_par = 1; n_func = 0; n_op = 0;
		return;
	case SE_COLREF:
		n_agr=0; n_gb = 0; n_par = 0; n_func = 0; n_op = 0;
		if(se->is_gb() ){
			if(enter_gb){
				analyze_constraint_se(se->get_right_se(),n_agr,n_gb,n_par,n_func,n_op,Ext_fcns,enter_gb);
			}else{
				n_gb=1;
			}
		}
		return;
	case SE_UNARY_OP:
		analyze_constraint_se(se->get_left_se(),n_agr,n_gb,n_par,n_func,n_op,Ext_fcns,enter_gb);
		n_op++;
		return;
	case SE_BINARY_OP:
		analyze_constraint_se(se->get_left_se(),l_agr,l_gb,l_par,l_func,l_op,Ext_fcns,enter_gb);
		analyze_constraint_se(se->get_right_se(),r_agr,r_gb,r_par, r_func,r_op,Ext_fcns,enter_gb);
		n_agr=l_agr+r_agr;
		n_gb=l_gb+r_gb;
		n_par=l_par+r_par;
		n_func=l_func+r_func;
		n_op=l_op+r_op+1;
		return;
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
		n_agr=1; n_gb = 0; n_par = 0; n_func = 0; n_op = 0;
		return;
	case SE_FUNC:
		if(se->get_aggr_ref() >= 0){
			n_agr=1; n_gb = 0; n_par = 0; n_op = 0;
			if(Ext_fcns)
		 		n_func = Ext_fcns->estimate_fcn_cost(se->get_fcn_id());
			else
				n_func = 1;
			return;
		}
		n_agr=0; n_gb = 0; n_par = 0;  n_op = 0;
		if(Ext_fcns)
	 		n_func = Ext_fcns->estimate_fcn_cost(se->get_fcn_id());
		else
			n_func = 1;
		operand_list = se->get_operands();
		for(p=0;p<operand_list.size();p++){
			analyze_constraint_se(operand_list[p],l_agr,l_gb,l_par,l_func,l_op,Ext_fcns,enter_gb);
			n_agr+=l_agr;
			n_gb+=l_gb;
			n_par+=l_par;
			n_func+=l_func;
			n_op += l_op;
		}
	break;
	}

	return;
}

//		Estimate the cost of a constraint.
//		WARNING a lot of cost assumptions are embedded in the code.
void analyze_constraint_pr(predicate_t *pr,
		int &n_agr, int &n_gb, int &n_par, int &n_func, int &n_op,
		int &n_cmp_s, int &n_cmp_c, int &n_in, int &n_pred, int &n_bool, ext_fcn_list *Ext_fcns, bool enter_gb){
 int l_agr, l_gb, l_par, l_func, l_op, l_cmp_s, l_cmp_c, l_in, l_pred,l_bool;
 int r_agr, r_gb, r_par, r_func, r_op, r_cmp_s, r_cmp_c, r_in, r_pred,r_bool;

int o;
vector<scalarexp_t *> op_list;


	switch(pr->get_operator_type()){
	case PRED_COMPARE:
		analyze_constraint_se(pr->get_left_se(),l_agr,l_gb,l_par,l_func, l_op,Ext_fcns,enter_gb);
		analyze_constraint_se(pr->get_right_se(),r_agr,r_gb,r_par,r_func,r_op,Ext_fcns,enter_gb);
		n_agr=l_agr+r_agr; n_gb=l_gb+r_gb; n_par=l_par+r_par;
		n_func=l_func+r_func; n_op=l_op+r_op;
		if(pr->get_left_se()->get_data_type()->complex_comparison(
			pr->get_right_se()->get_data_type())
	    ){
			n_cmp_s = 0; n_cmp_c=1;
		}else{
			n_cmp_s = 1; n_cmp_c=0;
		}
		n_in = 0; n_pred = 0; n_bool = 0;
		return;
	case PRED_IN:
//			Tread IN predicate as sequence of comparisons
		analyze_constraint_se(pr->get_left_se(),n_agr,n_gb,n_par,n_func,n_op,Ext_fcns,enter_gb);
		if(pr->get_left_se()->get_data_type()->complex_comparison(
			pr->get_right_se()->get_data_type())
	    ){
			n_cmp_s = 0; n_cmp_c=pr->get_lit_vec().size();
		}else{
			n_cmp_s = pr->get_lit_vec().size(); n_cmp_c=0;
		}
		n_in = 0; n_pred = 0; n_bool = 0;
		return;
	case PRED_UNARY_OP:
		analyze_constraint_pr(pr->get_left_pr(),n_agr,n_gb,n_par,n_func,n_op,n_cmp_s,n_cmp_c,n_in,n_pred,n_bool,Ext_fcns,enter_gb);
		n_bool++;
		return;
	case PRED_BINARY_OP:
		analyze_constraint_pr(pr->get_left_pr(),l_agr,l_gb,l_par,l_func,l_op,l_cmp_s,l_cmp_c,l_in,l_pred,l_bool,Ext_fcns,enter_gb);
		analyze_constraint_pr(pr->get_right_pr(),r_agr,r_gb,r_par,r_func,r_op,r_cmp_s,r_cmp_c,r_in,r_pred,r_bool,Ext_fcns,enter_gb);
		n_agr=l_agr+r_agr; n_gb=l_gb+r_gb; n_par=l_par+r_par;
		n_func=l_func+r_func; n_op=l_op+r_op;
		n_cmp_s=l_cmp_s+r_cmp_s; n_cmp_c=l_cmp_c+r_cmp_c;
		n_in=l_in+r_in; n_pred=l_pred+r_pred; n_bool=l_bool+r_bool+1;
		return;
	case PRED_FUNC:
		n_agr=n_gb=n_par=n_func=n_op=n_cmp_s=n_cmp_c=n_in=n_bool=0;
		if(Ext_fcns)
	 		n_pred = Ext_fcns->estimate_fcn_cost(pr->get_fcn_id());
		else
			n_pred = 1;
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			analyze_constraint_se(op_list[o],l_agr,l_gb,l_par,l_func,l_op,Ext_fcns,enter_gb);
			n_agr+=l_agr; n_gb+=l_gb; n_par+=l_par; n_func+=l_func; n_op+=l_op;
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in analyze_cnf_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}
}

void compute_cnf_cost(cnf_elem *c, ext_fcn_list *Ext_fcns){
 int n_agr, n_gb, n_par, n_func, n_op, n_cmp_s, n_cmp_c, n_in, n_pred,n_bool;
	analyze_constraint_pr(c->pr,n_agr, n_gb, n_par, n_func, n_op,
						n_cmp_s, n_cmp_c, n_in, n_pred,n_bool, Ext_fcns,false);

//printf("nfunc=%d n_pred=%d, n_cmp_c=%d, n_op=%d, n_cmp_s=%d,n_bool=%d\n", n_func, n_pred, n_cmp_c, n_op, n_cmp_s, n_bool);
	c->cost = (n_func+n_pred)+10*n_cmp_c+n_op+n_cmp_s+n_bool;
}

bool prefilter_compatible(cnf_elem *c, ext_fcn_list *Ext_fcns){
 int n_agr, n_gb, n_par, n_func, n_op, n_cmp_s, n_cmp_c, n_in, n_pred,n_bool;
	analyze_constraint_pr(c->pr,n_agr, n_gb, n_par, n_func, n_op,
						n_cmp_s, n_cmp_c, n_in, n_pred,n_bool, Ext_fcns,true);
//printf("prefilter_compatible, n_par=%d, n_gb=%d, n_agr=%d, n_func=%d, n_pred=%d, n_comp_c=%d, n_cmp_s=%d, n_bool=%d\n",n_gb,n_par,n_agr,n_func,n_pred,n_cmp_c,n_cmp_s,n_bool);
	if(n_par || n_agr)
		return false;
	int cost = (n_func+n_pred)+10*n_cmp_c+n_op+n_cmp_s+n_bool;
//printf("cost=%d\n",cost);
	return cost<10;
}

//		The prefilter needs to translate constraints on
//		gbvars into constraints involving their underlying SEs.
//		The following two routines attach GB def info.

void insert_gb_def_se(scalarexp_t *se, gb_table *gtbl){
 int p;
 vector<scalarexp_t *> operand_list;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_IFACE_PARAM:
	case SE_PARAM:
	case SE_AGGR_STAR:
		return;
	case SE_COLREF:
		if(se->is_gb() ){
			 se->rhs.scalarp = gtbl->get_def(se->get_gb_ref());
		}
		return;
	case SE_UNARY_OP:
		insert_gb_def_se(se->get_left_se(),gtbl);
		return;
	case SE_BINARY_OP:
		insert_gb_def_se(se->get_left_se(),gtbl);
		insert_gb_def_se(se->get_right_se(),gtbl);
		return;
	case SE_AGGR_SE:
		insert_gb_def_se(se->get_left_se(),gtbl);
		return;
	case SE_FUNC:
		operand_list = se->get_operands();
		for(p=0;p<operand_list.size();p++){
			insert_gb_def_se(operand_list[p],gtbl);
		}
	break;
	}

	return;
}
void insert_gb_def_pr(predicate_t *pr, gb_table *gtbl){
vector<scalarexp_t *> op_list;
int o;

	switch(pr->get_operator_type()){
	case PRED_COMPARE:
		insert_gb_def_se(pr->get_left_se(),gtbl);
		insert_gb_def_se(pr->get_right_se(),gtbl);
		return;
	case PRED_IN:
		insert_gb_def_se(pr->get_left_se(),gtbl);
		return;
	case PRED_UNARY_OP:
		insert_gb_def_pr(pr->get_left_pr(),gtbl);
		return;
	case PRED_BINARY_OP:
		insert_gb_def_pr(pr->get_left_pr(),gtbl);
		insert_gb_def_pr(pr->get_right_pr(),gtbl);
		return;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			insert_gb_def_se(op_list[o],gtbl);
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in insert_gb_def_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}
}

//		Substitute gbrefs with their definitions
void subs_gbrefs_se(scalarexp_t *se, table_list *Schema){
 int p;
 vector<scalarexp_t *> operand_list;
 scalarexp_t *lse,*rse;
 colref_t *cr;
 string b_tbl;
 int b_idx;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_IFACE_PARAM:
	case SE_PARAM:
	case SE_AGGR_STAR:
		return;
	case SE_COLREF:
		cr = se->get_colref();
		b_tbl = Schema->get_basetbl_name(cr->schema_ref,cr->field);
		b_idx = Schema->get_table_ref(b_tbl);
		cr->tablevar_ref = b_idx;
		return;
	case SE_UNARY_OP:
		lse=se->get_left_se();
		if(lse->get_operator_type()==SE_COLREF && lse->is_gb()){
			se->lhs.scalarp = lse->get_right_se();
			subs_gbrefs_se(se,Schema);
			return;
		}
		subs_gbrefs_se(se->get_left_se(),Schema);
		return;
	case SE_BINARY_OP:
		lse=se->get_left_se();
		if(lse->get_operator_type()==SE_COLREF && lse->is_gb()){
			se->lhs.scalarp = lse->get_right_se();
			subs_gbrefs_se(se,Schema);
			return;
		}
		rse=se->get_right_se();
		if(rse->get_operator_type()==SE_COLREF && rse->is_gb()){
			se->rhs.scalarp = rse->get_right_se();
			subs_gbrefs_se(se,Schema);
			return;
		}
		subs_gbrefs_se(se->get_left_se(),Schema);
		subs_gbrefs_se(se->get_right_se(),Schema);
		return;
	case SE_AGGR_SE:
		lse=se->get_left_se();
		if(lse->get_operator_type()==SE_COLREF && lse->is_gb()){
			se->lhs.scalarp = lse->get_right_se();
			subs_gbrefs_se(se,Schema);
			return;
		}
		subs_gbrefs_se(se->get_left_se(),Schema);
		return;
	case SE_FUNC:
		operand_list = se->get_operands();
		for(p=0;p<operand_list.size();p++){
			lse=operand_list[p];
			if(lse->get_operator_type()==SE_COLREF && lse->is_gb()){
				se->param_list[p] = lse->get_right_se();
				subs_gbrefs_se(se,Schema);
				return;
			}
		}
		for(p=0;p<operand_list.size();p++){
			subs_gbrefs_se(operand_list[p],Schema);
		}
	break;
	}

	return;
}

void subs_gbrefs_pr(predicate_t *pr, table_list *Schema){
vector<scalarexp_t *> op_list;
int o;
scalarexp_t *lse,*rse;

	switch(pr->get_operator_type()){
	case PRED_COMPARE:
		lse=pr->get_left_se();
		if(lse->get_operator_type()==SE_COLREF && lse->is_gb()){
			pr->lhs.sexp = lse->get_right_se();
			subs_gbrefs_pr(pr,Schema);
			return;
		}
		rse=pr->get_right_se();
		if(rse->get_operator_type()==SE_COLREF && rse->is_gb()){
			pr->rhs.sexp = rse->get_right_se();
			subs_gbrefs_pr(pr,Schema);
			return;
		}
		subs_gbrefs_se(pr->get_left_se(),Schema);
		subs_gbrefs_se(pr->get_right_se(),Schema);
		return;
	case PRED_IN:
		lse=pr->get_left_se();
		if(lse->get_operator_type()==SE_COLREF && lse->is_gb()){
			pr->lhs.sexp = lse->get_right_se();
			subs_gbrefs_pr(pr,Schema);
			return;
		}
		subs_gbrefs_se(pr->get_left_se(),Schema);
		return;
	case PRED_UNARY_OP:
		subs_gbrefs_pr(pr->get_left_pr(),Schema);
		return;
	case PRED_BINARY_OP:
		subs_gbrefs_pr(pr->get_left_pr(),Schema);
		subs_gbrefs_pr(pr->get_right_pr(),Schema);
		return;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			lse=op_list[o];
			if(lse->get_operator_type()==SE_COLREF && lse->is_gb()){
				pr->param_list[o] = lse->get_right_se();
				subs_gbrefs_pr(pr,Schema);
				return;
			}
			subs_gbrefs_se(op_list[o],Schema);
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in subs_gbrefs_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}
}


//		Search for references to "expensive" fields.
int expensive_refs_se(scalarexp_t *se, table_list *Schema){
 int p;
 vector<scalarexp_t *> operand_list;
 int cnt=0;
table_def *td;
param_list *plist;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_IFACE_PARAM:
	case SE_PARAM:
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
		return 0;
	case SE_COLREF:
		if(se->is_gb())
			return expensive_refs_se(se->rhs.scalarp,Schema);
		td = Schema->get_table(se->lhs.colref->schema_ref);
		plist = td->get_modifier_list(se->lhs.colref->field);
		if(plist->contains_key("expensive"))
			return 1;
		return 0;
	case SE_UNARY_OP:
		return expensive_refs_se(se->get_left_se(),Schema);
	case SE_BINARY_OP:
		cnt += expensive_refs_se(se->get_left_se(),Schema);
		cnt += expensive_refs_se(se->get_right_se(),Schema);
		return cnt;
	case SE_FUNC:
		operand_list = se->get_operands();
		for(p=0;p<operand_list.size();p++){
			cnt += expensive_refs_se(operand_list[p],Schema);
		}
		return cnt;
	break;
	}

	return 0;
}

int expensive_refs_pr(predicate_t *pr, table_list *Schema){
vector<scalarexp_t *> op_list;
int o;
int cnt=0;

	switch(pr->get_operator_type()){
	case PRED_COMPARE:
		cnt += expensive_refs_se(pr->get_left_se(),Schema);
		cnt += expensive_refs_se(pr->get_right_se(),Schema);
		return cnt;
	case PRED_IN:
		return expensive_refs_se(pr->get_left_se(),Schema);
	case PRED_UNARY_OP:
		return expensive_refs_pr(pr->get_left_pr(),Schema);
	case PRED_BINARY_OP:
		cnt += expensive_refs_pr(pr->get_left_pr(),Schema);
		cnt += expensive_refs_pr(pr->get_right_pr(),Schema);
		return cnt;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			cnt += expensive_refs_se(op_list[o],Schema);
		}
		return cnt;
	default:
		fprintf(stderr,"INTERNAL ERROR in expensive_refs_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}
}


//		TODO: allow "cheap" functions and predicates.
bool simple_field_constraint(cnf_elem *c){
	vector<literal_t *> ll;
	int l;
	predicate_t *p = c->pr;
 int l_agr, l_gb, l_par, l_func, l_op;
 int r_agr, r_gb, r_par, r_func, r_op;
 col_id_set left_colids, right_colids;

//			Verify that it is a simple atom
	switch(p->get_operator_type()){
	case PRED_COMPARE:
//				Must be an equality predicate which references
//				which referecnes no aggregates, parameters, functions, or
//				group-by variables, and should be a constraint of
//				a single colref.
//				AND should not require a complex comparison.
		if(p->get_op() != "=") return(false);
		analyze_constraint_se(p->get_left_se(),l_agr, l_gb, l_par, l_func,l_op,NULL,false);
		analyze_constraint_se(p->get_right_se(),r_agr, r_gb, r_par, r_func,l_op,NULL,false);
		if(l_agr>0 || l_gb>0 || l_par>0 || l_func>0 ||
		   r_agr>0 || r_gb>0 || r_par>0 || r_func>0 ) return(false);
//				I will count on there being no gbvars in the constraint.
//				TODO: allow gbvars which are colrefs.
		gather_se_col_ids(p->get_left_se(), left_colids, NULL);
		gather_se_col_ids(p->get_right_se(), right_colids, NULL);
		if(left_colids.size()+right_colids.size() != 1) return(false);


//			Normalize : the colref should be on the lhs.
		if(right_colids.size() > 0){
			p->swap_scalar_operands();
		}

//			Disallow complex (and therefore expensive) comparisons.
		if(p->get_left_se()->get_data_type()->complex_comparison(
			p->get_right_se()->get_data_type() ) )
				return(false);

//			passed all the tests.
		return(true);
	case PRED_IN:
//			LHS must be a non-gbvar colref.
		analyze_constraint_se(p->get_left_se(),l_agr, l_gb, l_par, l_func,l_op,NULL,false);
		if(l_agr>0 || l_gb>0 || l_par>0 || l_func>0 ) return(false);
//				I will count on there being no gbvars in the constraint.
//				TODO: allow gbvars which are colrefs.
		gather_se_col_ids(p->get_left_se(), left_colids, NULL);
		if(left_colids.size() != 1) return(false);
//			Disallow complex (and therefore expensive) comparisons.
		if(p->get_left_se()->get_data_type()->complex_comparison(
			p->get_left_se()->get_data_type() ) )
				return(false);


//			All entries in the IN list must be literals
//			Currently, this is the only possibility.
		return(true);
		break;
	case PRED_UNARY_OP:
		return(false);
	case PRED_BINARY_OP:
		return(false);
	case PRED_FUNC:
		return(false);
	default:
		fprintf(stderr,"INTERNAL ERROR in simple_field_cosntraint, line %d, character %d, unknown predicate operator type %d\n",
			p->get_lineno(), p->get_charno(), p->get_operator_type() );
		exit(1);
	}

	return(false);
}

//		As the name implies, return the colref constrained by the
//		cnf elem.  I will be counting on the LHS being a SE pointing
//		to a colref.

//			This fcn assumes that in fact exactly
//			one colref is constrained.
colref_t *get_constrained_colref(scalarexp_t *se){
 int p;
 vector<scalarexp_t *> operand_list;
colref_t *ret;

	switch(se->get_operator_type()){
	case SE_LITERAL:
		return(NULL);
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return(NULL);
	case SE_COLREF:
		return(se->get_colref());
	case SE_UNARY_OP:
		return(get_constrained_colref(se->get_left_se()));
	case SE_BINARY_OP:
		ret=get_constrained_colref(se->get_left_se());
		if(ret == NULL) return(get_constrained_colref(se->get_right_se()));
		else return ret;
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
		return(NULL);
	case SE_FUNC:
		if(se->get_aggr_ref() >= 0) return NULL;

		operand_list = se->get_operands();
		for(p=0;p<operand_list.size();p++){
			ret=get_constrained_colref(operand_list[p]);
			if(ret != NULL) return(ret);

		}
		return(NULL);
	break;
	}

	return(NULL);
}


colref_t *get_constrained_colref(predicate_t *p){
	return(get_constrained_colref(p->get_left_se()));
}
colref_t *get_constrained_colref(cnf_elem *c){
	return get_constrained_colref(c->pr->get_left_se());
}




/*
void add_colref_constraint_to_cnf(cnf_elem *dst, predicate_t *src_p,
							string target_fld, string target_tbl, int tblref){

//			Make a copy of the predicate to be added.
//			ASSUME no aggregates.
	predicate_t *pr = dup_pr(src_p,NULL);

//			Modify the ref to the base table.
//			ASSUME lhs is the colref
	pr->get_left_se()->get_colref()->set_table_name(target_tbl);
	pr->get_left_se()->get_colref()->set_table_ref(tblref);

	if(dst->pr == NULL) dst->pr = pr;
	else dst->pr = new predicate_t("OR", dst->pr, pr);

}
*/


//////////////////////////////////////////////////////
///////////////		Represent a node in a predicate tree
struct common_pred_node{
	set<int> lftas;
	predicate_t *pr;
	vector<predicate_t *> predecessor_preds;
	vector<common_pred_node *> children;

	string target_tbl;
	string target_fld;
	int target_ref;

	common_pred_node(){
		pr = NULL;
	}
};


predicate_t *make_common_pred(common_pred_node *pn){
  int n;

	if(pn->children.size() == 0){
		if(pn->pr == NULL){
			fprintf(stderr,"INTERNAL ERROR in make_common_pred, pred node ahs no children and no predicate.\n");
			exit(1);
		}
		return( dup_pr(pn->pr,NULL) );
	}

	predicate_t *curr_pr = make_common_pred( pn->children[0] );
    for(n=1;n<pn->children.size();++n){
		curr_pr = new predicate_t("OR", make_common_pred(pn->children[n]),curr_pr);
	}

	if(pn->pr != NULL)
		curr_pr = new predicate_t("AND", dup_pr(pn->pr,NULL), curr_pr);

	return(curr_pr);
}


bool operator<(const cnf_set &c1, const cnf_set &c2){
	if(c1.lfta_id.size() < c2.lfta_id.size())
		return true;
	return false;
}


//		Compute the predicates for the prefilter.
//		the prefilter preds are returned in prefilter_preds.
//		pred_ids is the set of predicates used in the prefilter.
//		the encoding is the lfta index, in the top 16 bits,
//		then the index of the cnf element in the bottom 16 bits.
//		This set of for identifying which preds do not need
//		to be generated in the lftas.
void find_common_filter(vector< vector<cnf_elem *> > &where_list, table_list *Schema, ext_fcn_list *Ext_fcns, vector<cnf_set *> &prefilter_preds, set<unsigned int > &pred_ids){
	int p, p2, l, c;

	vector<cnf_set *> pred_list, sort_list;

//		Create list of tagged, prefilter-safe CNFs.
	for(l=0;l<where_list.size();++l){
		for(c=0;c<where_list[l].size();++c){
			if(prefilter_compatible(where_list[l][c],Ext_fcns)){
				if(expensive_refs_pr(where_list[l][c]->pr,Schema)==0)
					pred_list.push_back(new cnf_set(where_list[l][c]->pr,l,c));
			}
		}
	}

//		Eliminate duplicates
	for(p=0;p<pred_list.size();++p){
		if(pred_list[p]){
			for(p2=p+1;p2<pred_list.size();++p2){
				if(pred_list[p2]){
					if(is_equivalent_pred_base(pred_list[p]->pr, pred_list[p2]->pr,Schema)){
						pred_list[p]->subsume(pred_list[p2]);
						delete pred_list[p2];
						pred_list[p2] = NULL;
					}
				}
			}
		}
	}

//		combine preds that occur in the exact same lftas.
	for(p=0;p<pred_list.size();++p){
		if(pred_list[p]){
			for(p2=p+1;p2<pred_list.size();++p2){
				if(pred_list[p2]){
					if(pred_list[p]->lfta_id == pred_list[p2]->lfta_id){
						pred_list[p]->combine_pred(pred_list[p2]);
						delete pred_list[p2];
						pred_list[p2] = NULL;
					}
				}
			}
		}
	}

//		Compress the list
	for(p=0;p<pred_list.size();++p){
		if(pred_list[p]){
			sort_list.push_back(pred_list[p]);
		}
	}
//		Sort it
	sort(sort_list.begin(), sort_list.end(),compare_cnf_set());

//		Return the top preds, up to 64 of them.
	for(p=0;p<sort_list.size() && p<64;p++){
		prefilter_preds.push_back(sort_list[p]);
		sort_list[p]->add_pred_ids(pred_ids);
	}

//		Substitute gb refs with their defs
//		While I'm at it, substitute base table sch ref for tblref.
	for(p=0;p<prefilter_preds.size() ;p++){
		subs_gbrefs_pr(prefilter_preds[p]->pr,Schema);
	}

}





///////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

//		Find partial functions and register them.
//		Do a DFS so that nested partial fcn calls
//		get evaluated in the right order.
//		Don't drill down into aggregates -- their arguments are evaluated
//		earlier than the select list is.
//
//		Modification for function caching:
//		Pass in a ref counter, and partial fcn indicator.
//		Cache fcns ref'd at least once.
//		pass in NULL for fcn_ref_cnt to turn off fcn caching analysis


void find_partial_fcns(scalarexp_t *se, vector<scalarexp_t *> *pf_list,
		vector<int> *fcn_ref_cnt, vector<bool> *is_partial_fcn,
		ext_fcn_list *Ext_fcns){
	vector<scalarexp_t *> operands;
	int o, f;

	if(se == NULL) return;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return;
	case SE_UNARY_OP:
		find_partial_fcns(se->get_left_se(), pf_list, fcn_ref_cnt, is_partial_fcn, Ext_fcns) ;
		return;
	case SE_BINARY_OP:
		find_partial_fcns(se->get_left_se(), pf_list, fcn_ref_cnt, is_partial_fcn, Ext_fcns);
		find_partial_fcns(se->get_right_se(), pf_list, fcn_ref_cnt, is_partial_fcn, Ext_fcns);
		return;
	case SE_COLREF:
		return;
	case SE_AGGR_STAR:
		return;
	case SE_AGGR_SE:
//		find_partial_fcns(se->get_left_se(), pf_list, Ext_fcns) ;
		return;
	case SE_FUNC:
		if(se->get_aggr_ref() >= 0) return;

		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			find_partial_fcns(operands[o], pf_list, fcn_ref_cnt, is_partial_fcn, Ext_fcns);
		}

		if(Ext_fcns->is_partial(se->get_fcn_id()) || Ext_fcns->get_fcn_cost(se->get_fcn_id()) >= COST_HIGH){
			if(fcn_ref_cnt){
			  for(f=0;f<pf_list->size();++f){
				if(is_equivalent_se(se,(*pf_list)[f])){
					se->set_partial_ref(f);
					(*fcn_ref_cnt)[f]++;
					break;
				}
			  }
			}else{
				f=pf_list->size();
			}
			if(f==pf_list->size() && (Ext_fcns->is_partial(se->get_fcn_id()) ||  fcn_ref_cnt)){
				se->set_partial_ref(pf_list->size());
				pf_list->push_back(se);
				if(fcn_ref_cnt){
					fcn_ref_cnt->push_back(1);
					is_partial_fcn->push_back(Ext_fcns->is_partial(se->get_fcn_id()));
				}
			}
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in find_partial_fcns, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return;
}


void find_partial_fcns_pr(predicate_t *pr,  vector<scalarexp_t *> *pf_list,
		vector<int> *fcn_ref_cnt, vector<bool> *is_partial_fcn,
									ext_fcn_list *Ext_fcns){
	vector<literal_t *> litl;
	vector<scalarexp_t *> op_list;
	int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		find_partial_fcns(pr->get_left_se(), pf_list, fcn_ref_cnt, is_partial_fcn, Ext_fcns) ;
		return;
	case PRED_COMPARE:
		find_partial_fcns(pr->get_left_se(), pf_list, fcn_ref_cnt, is_partial_fcn, Ext_fcns) ;
		find_partial_fcns(pr->get_right_se(), pf_list, fcn_ref_cnt, is_partial_fcn, Ext_fcns) ;
		return;
	case PRED_UNARY_OP:
		find_partial_fcns_pr(pr->get_left_pr(), pf_list, fcn_ref_cnt, is_partial_fcn, Ext_fcns);
		return;
	case PRED_BINARY_OP:
		find_partial_fcns_pr(pr->get_left_pr(), pf_list, fcn_ref_cnt, is_partial_fcn, Ext_fcns) ;
		find_partial_fcns_pr(pr->get_right_pr(), pf_list, fcn_ref_cnt, is_partial_fcn, Ext_fcns) ;
		return;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			find_partial_fcns(op_list[o],pf_list,fcn_ref_cnt, is_partial_fcn, Ext_fcns);
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in find_partial_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return;
}



void find_combinable_preds(predicate_t *pr,  vector<predicate_t *> *pr_list,
								table_list *Schema, ext_fcn_list *Ext_fcns){
	vector<literal_t *> litl;
	vector<scalarexp_t *> op_list;
	int f,o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		return;
	case PRED_COMPARE:
		return;
	case PRED_UNARY_OP:
		find_combinable_preds(pr->get_left_pr(), pr_list, Schema, Ext_fcns);
		return;
	case PRED_BINARY_OP:
		find_combinable_preds(pr->get_left_pr(), pr_list, Schema, Ext_fcns) ;
		find_combinable_preds(pr->get_right_pr(), pr_list, Schema, Ext_fcns) ;
		return;
	case PRED_FUNC:
		if(Ext_fcns->is_combinable(pr->get_fcn_id())){
		  for(f=0;f<pr_list->size();++f){
			if(is_equivalent_pred_base(pr,(*pr_list)[f],Schema)){
				pr->set_combinable_ref(f);
				break;
			}
		  }
		  if(f == pr_list->size()){
			pr->set_combinable_ref(pr_list->size());
			pr_list->push_back(pr);
		  }
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in find_partial_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return;
}


//--------------------------------------------------------------------
//		Collect refs to aggregates.


void collect_agg_refs(scalarexp_t *se, set<int> &agg_refs){
	vector<scalarexp_t *> operands;
	int o;

	if(se == NULL) return;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return;
	case SE_UNARY_OP:
		collect_agg_refs(se->get_left_se(), agg_refs) ;
		return;
	case SE_BINARY_OP:
		collect_agg_refs(se->get_left_se(), agg_refs);
		collect_agg_refs(se->get_right_se(), agg_refs);
		return;
	case SE_COLREF:
		return;
	case SE_AGGR_STAR:
	case SE_AGGR_SE:
		agg_refs.insert(se->get_aggr_ref());
		return;
	case SE_FUNC:
		if(se->get_aggr_ref() >= 0) agg_refs.insert(se->get_aggr_ref());

		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			collect_agg_refs(operands[o], agg_refs);
		}

		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in collect_agg_refs, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return;
}


void collect_aggr_refs_pr(predicate_t *pr,  set<int> &agg_refs){
	vector<literal_t *> litl;
	vector<scalarexp_t *> op_list;
	int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		collect_agg_refs(pr->get_left_se(), agg_refs) ;
		return;
	case PRED_COMPARE:
		collect_agg_refs(pr->get_left_se(), agg_refs) ;
		collect_agg_refs(pr->get_right_se(), agg_refs) ;
		return;
	case PRED_UNARY_OP:
		collect_aggr_refs_pr(pr->get_left_pr(), agg_refs);
		return;
	case PRED_BINARY_OP:
		collect_aggr_refs_pr(pr->get_left_pr(), agg_refs) ;
		collect_aggr_refs_pr(pr->get_right_pr(), agg_refs) ;
		return;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			collect_agg_refs(op_list[o],agg_refs);
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in collect_aggr_refs_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return;
}


//--------------------------------------------------------------------
//		Collect previously registered partial fcn refs.
//		Do a DFS so that nested partial fcn calls
//		get evaluated in the right order.
//		Don't drill down into aggregates -- their arguments are evaluated
//		earlier than the select list is.
//		------------->>> THEN WHY AM I DRILLING DOWN INTO AGGREGATES?

void collect_partial_fcns(scalarexp_t *se, set<int> &pfcn_refs){
	vector<scalarexp_t *> operands;
	int o;

	if(se == NULL) return;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return;
	case SE_UNARY_OP:
		collect_partial_fcns(se->get_left_se(), pfcn_refs) ;
		return;
	case SE_BINARY_OP:
		collect_partial_fcns(se->get_left_se(), pfcn_refs);
		collect_partial_fcns(se->get_right_se(), pfcn_refs);
		return;
	case SE_COLREF:
		return;
	case SE_AGGR_STAR:
		return;
	case SE_AGGR_SE:
//		collect_partial_fcns(se->get_left_se(), pfcn_refs) ;
		return;
	case SE_FUNC:
		if(se->get_aggr_ref() >= 0) return;

		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			collect_partial_fcns(operands[o], pfcn_refs);
		}

		if(se->is_partial()){
			pfcn_refs.insert(se->get_partial_ref());
		}

		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in collect_partial_fcns, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return;
}


void collect_partial_fcns_pr(predicate_t *pr,  set<int> &pfcn_refs){
	vector<literal_t *> litl;
	vector<scalarexp_t *> op_list;
	int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		collect_partial_fcns(pr->get_left_se(), pfcn_refs) ;
		return;
	case PRED_COMPARE:
		collect_partial_fcns(pr->get_left_se(), pfcn_refs) ;
		collect_partial_fcns(pr->get_right_se(), pfcn_refs) ;
		return;
	case PRED_UNARY_OP:
		collect_partial_fcns_pr(pr->get_left_pr(), pfcn_refs);
		return;
	case PRED_BINARY_OP:
		collect_partial_fcns_pr(pr->get_left_pr(), pfcn_refs) ;
		collect_partial_fcns_pr(pr->get_right_pr(), pfcn_refs) ;
		return;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			collect_partial_fcns(op_list[o],pfcn_refs);
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in collect_partial_fcns_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return;
}




///////////////////////////////////////////////////////////////
////////////	Exported Functions	///////////////////////////
///////////////////////////////////////////////////////////////


//		Count and collect refs to interface parameters.

int count_se_ifp_refs(scalarexp_t *se, set<string> &ifpnames){
	vector<scalarexp_t *> operands;
	int o;
	int ret = 0;

	if(se == NULL) return 0;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
		return 0;
	case SE_IFACE_PARAM:
			ifpnames.insert(se->get_ifpref()->to_string());
		return 1;
	case SE_UNARY_OP:
		return count_se_ifp_refs(se->get_left_se(), ifpnames) ;
	case SE_BINARY_OP:
		ret = count_se_ifp_refs(se->get_left_se(), ifpnames);
		ret += count_se_ifp_refs(se->get_right_se(), ifpnames);
		return ret;
	case SE_COLREF:
		return 0;
	case SE_AGGR_STAR:
		return 0;
	case SE_AGGR_SE:
//		collect_partial_fcns(se->get_left_se(), pfcn_refs) ;
		return 0;
	case SE_FUNC:
		if(se->get_aggr_ref() >= 0) return 0;

		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			ret += count_se_ifp_refs(operands[o], ifpnames);
		}

		return ret;
	default:
		fprintf(stderr,"INTERNAL ERROR in count_se_ifp_refs, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return 0;
}


int count_pr_ifp_refs(predicate_t *pr,  set<string> &ifpnames){
	vector<literal_t *> litl;
	vector<scalarexp_t *> op_list;
	int o;
	int ret = 0;
	if(pr == NULL) return 0;

	switch(pr->get_operator_type()){
	case PRED_IN:
		return count_se_ifp_refs(pr->get_left_se(), ifpnames) ;
	case PRED_COMPARE:
		ret = count_se_ifp_refs(pr->get_left_se(), ifpnames) ;
		ret += count_se_ifp_refs(pr->get_right_se(), ifpnames) ;
		return ret;
	case PRED_UNARY_OP:
		return count_pr_ifp_refs(pr->get_left_pr(), ifpnames);
	case PRED_BINARY_OP:
		ret = count_pr_ifp_refs(pr->get_left_pr(), ifpnames) ;
		ret += count_pr_ifp_refs(pr->get_right_pr(), ifpnames) ;
		return ret;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			ret += count_se_ifp_refs(op_list[o],ifpnames);
		}
		return ret;
	default:
		fprintf(stderr,"INTERNAL ERROR in count_pr_ifp_refs, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return 0;
}

//		Resolve ifp refs, convert them to string literals.

int resolve_se_ifp_refs(scalarexp_t *se, string ifm, string ifn, ifq_t *ifdb,  string &err){
	vector<scalarexp_t *> operands;
	vector<string> ifvals;
	int o;
	int ierr;
	string serr;
	int ret = 0;
	literal_t *tmp_l;
	ifpref_t *ir;

	if(se == NULL) return 0;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
		return 0;
	case SE_IFACE_PARAM:
		ir = se->get_ifpref();
		ifvals = ifdb->get_iface_vals(ifm, ifn, ir->get_pname(), ierr, serr);
		if(ierr){
			err += "ERROR looking for parameter "+ir->get_pname()+" in interface "+ifm+"."+ifn+", "+serr+"\n";
			return 1;
		}
		if(ifvals.size() == 0){
			err += "ERROR looking for parameter "+ir->get_pname()+" in interface "+ifm+"."+ifn+", no parameter values.\n";
			return 1;
		}
		if(ifvals.size() > 1){
			err += "ERROR looking for parameter "+ir->get_pname()+" in interface "+ifm+"."+ifn+", multiple parameter values ("+int_to_string(ifvals.size())+").\n";
			return 1;
		}
		tmp_l = new literal_t( ifvals[0]);
		se->convert_to_literal(tmp_l);
		return 0;
	case SE_UNARY_OP:
		return resolve_se_ifp_refs( se->get_left_se(), ifm, ifn,ifdb,err) ;
	case SE_BINARY_OP:
		ret = resolve_se_ifp_refs( se->get_left_se(), ifm, ifn,ifdb,err);
		ret += resolve_se_ifp_refs( se->get_right_se(), ifm, ifn,ifdb,err);
		return ret;
	case SE_COLREF:
		return 0;
	case SE_AGGR_STAR:
		return 0;
	case SE_AGGR_SE:
//		collect_partial_fcns(se->get_left_se(), pfcn_refs) ;
		return 0;
	case SE_FUNC:
		if(se->get_aggr_ref() >= 0) return 0;

		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			ret += resolve_se_ifp_refs(operands[o], ifm, ifn, ifdb,err);
		}

		return ret;
	default:
		fprintf(stderr,"INTERNAL ERROR in resolve_se_ifp_refs, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return 0;
}


int resolve_pr_ifp_refs(predicate_t *pr,  string ifm, string ifn, ifq_t *ifdb,  string &err){
	vector<literal_t *> litl;
	vector<scalarexp_t *> op_list;
	int o;
	int ret = 0;

	switch(pr->get_operator_type()){
	case PRED_IN:
		return resolve_se_ifp_refs(pr->get_left_se(), ifm, ifn, ifdb, err) ;
	case PRED_COMPARE:
		ret = resolve_se_ifp_refs(pr->get_left_se(), ifm, ifn, ifdb, err) ;
		ret += resolve_se_ifp_refs(pr->get_right_se(), ifm, ifn, ifdb, err) ;
		return ret;
	case PRED_UNARY_OP:
		return resolve_pr_ifp_refs(pr->get_left_pr(), ifm, ifn, ifdb, err);
	case PRED_BINARY_OP:
		ret = resolve_pr_ifp_refs(pr->get_left_pr(), ifm, ifn, ifdb, err) ;
		ret += resolve_pr_ifp_refs(pr->get_right_pr(), ifm, ifn, ifdb, err) ;
		return ret;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			ret += resolve_se_ifp_refs(op_list[o],ifm, ifn, ifdb, err);
		}
		return ret;
	default:
		fprintf(stderr,"INTERNAL ERROR in resolve_pr_ifp_refs, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
		exit(1);
	}

	return 0;
}


string impute_query_name(table_exp_t *fta_tree, string default_nm){
	string retval = fta_tree->get_val_of_name("query_name");
	if(retval == "") retval = default_nm;
	if(retval == "") retval = "default_query";
	return(retval);
}

//		Convert the parse tree into an intermediate form,
//		which admits analysis better.
//
//		TODO : rationalize the error return policy.
//
//		TODO : the query_summary_class object contains
//			the parse tree.
//		TODO: revisit the issue when nested subqueries are implemented.
//		One possibility: implement accessor methods to hide the
//		complexity
//		For now: this class contains data structures not in table_exp_t
//		(with a bit of duplication)

//		Return NULL on error.
//		print error messages to stderr.


query_summary_class *analyze_fta(table_exp_t *fta_tree, table_list *schema,
				ext_fcn_list *Ext_fcns, string default_name){
	int i,j, k, retval;

//			Create the summary struct -- no analysis is done here.
	query_summary_class *qs = new query_summary_class(fta_tree);
	qs->query_type = fta_tree->query_type;

//////////////		Do common analysis

//		Extract	query name.  Already imputed for the qnodes.
//	qs->query_name = impute_query_name(fta_tree, default_name);
	qs->query_name = default_name;
//printf("query name is %s\n",qs->query_name.c_str());

//		extract definitions.  Don't grab the query name.

	map<string, string> nmap = fta_tree->get_name_map();
	map<string, string>::iterator nmi;
	for(nmi=nmap.begin(); nmi!=nmap.end(); ++nmi){
		string pname = (*nmi).first;
		if(pname != "query_name" )
			(qs->definitions)[pname] = (*nmi).second;
	}

///
///				FROM analysis

//		First, verify that all the referenced tables are defined.
//		Then, bind the tablerefs in the FROM list to schemas in
//		the schema list.
	tablevar_list_t *tlist = fta_tree->get_from();
	vector<tablevar_t *> tbl_vec = tlist->get_table_list();

	bool found_error = false;
	for(i=0;i<tbl_vec.size();i++){
		int sch_no = schema->find_tbl(tbl_vec[i]->get_schema_name());
		if(sch_no < 0)	{
		  fprintf(stderr,"Error, table <%s> not found in the schema file\n",
			tbl_vec[i]->get_schema_name().c_str() );
		  fprintf(stderr,"\tline=%d, char=%d\n",tbl_vec[i]->get_lineno(),
					tbl_vec[i]->get_charno() );
		  return(NULL);
		}

		tbl_vec[i]->set_schema_ref(sch_no);

//				If accessing a UDOP, mangle the name
//			This needs to be done in translate_fta.cc, not here.
/*
		if(schema->get_schema_type(sch_no) == OPERATOR_VIEW_SCHEMA){
			string mngl_name = tbl_vec[i]->get_schema_name() + silo_nm;
			tbl_vec[i]->set_schema_name(mngl_name);
		}
*/

//			No FTA schema should have an interface defined on it.
		if(tbl_vec[i]->get_interface()!="" && schema->get_schema_type(sch_no) != PROTOCOL_SCHEMA){
			fprintf(stderr,"WARNING: interface %s specified for schema %s, but this schema is a STREAM and does not have an interface.\n",tbl_vec[i]->get_interface().c_str(), tbl_vec[i]->get_schema_name().c_str());
		}
//			Fill in default interface
		if(tbl_vec[i]->get_interface()=="" && schema->get_schema_type(sch_no) == PROTOCOL_SCHEMA){
			tbl_vec[i]->set_interface("default");
			tbl_vec[i]->set_ifq(true);
		}
//			Fill in default machine
		if(tbl_vec[i]->get_interface()!=""  && tbl_vec[i]->get_machine()=="" && schema->get_schema_type(sch_no) == PROTOCOL_SCHEMA && (! tbl_vec[i]->get_ifq())){
			tbl_vec[i]->set_machine(hostname);
		}

		if(schema->get_schema_type(sch_no) == PROTOCOL_SCHEMA){
//			Record the set of interfaces accessed
			string ifstr;
			if(tbl_vec[i]->get_ifq()){
				ifstr = "["+tbl_vec[i]->get_interface()+"]";
			}else{
				if(tbl_vec[i]->get_machine() != "localhost"){
					ifstr = "&apos;"+tbl_vec[i]->get_machine()+"&apos;."+tbl_vec[i]->get_interface();
				}else{
					ifstr = tbl_vec[i]->get_interface();
				}
			}
//printf("ifstr is %s, i=%d, machine=%s, interface=%s\n",ifstr.c_str(),i,tbl_vec[i]->get_machine().c_str(),tbl_vec[i]->get_interface().c_str());
			if(qs->definitions.count("_referenced_ifaces")){
				ifstr = qs->definitions["_referenced_ifaces"]+","+ifstr;
			}
			qs->definitions["_referenced_ifaces"] = ifstr;
		}

	}
	if(found_error) return(NULL);

//			Ensure that all tablevars have are named
//			and that no two tablevars have the same name.
	int tblvar_no = 0;
//		First, gather the set of variable
	set<string> tblvar_names;
	for(i=0;i<tbl_vec.size();i++){
		if(tbl_vec[i]->get_var_name() != ""){
			if(tblvar_names.count(tbl_vec[i]->get_var_name()) > 0){
				fprintf(stderr,"ERROR, query has two table variables named %s.  line=%d, char=%d\n", tbl_vec[i]->get_var_name().c_str(), tbl_vec[i]->get_lineno(), tbl_vec[i]->get_charno());
				return(NULL);
			}
			tblvar_names.insert(tbl_vec[i]->get_var_name());
		}
	}
//		Now generate variable names for unnamed tablevars
	for(i=0;i<tbl_vec.size();i++){
		if(tbl_vec[i]->get_var_name() == ""){
			char tmpstr[200];
			sprintf(tmpstr,"_t%d",tblvar_no);
			string newvar = tmpstr;
			while(tblvar_names.count(newvar) > 0){
				tblvar_no++;
				sprintf(tmpstr,"_t%d",tblvar_no);
				newvar = tmpstr;
			}
			tbl_vec[i]->set_range_var(newvar);
			tblvar_names.insert(newvar);
		}
	}

//		Process inner/outer join properties
	int jprop = fta_tree->get_from()->get_properties();
//		Require explicit INNER_JOIN, ... specification for join queries.
	if(jprop < 0){
		if(qs->query_type != MERGE_QUERY && tbl_vec.size() > 1){
			fprintf(stderr,"ERROR, a join query must specify one of INNER_JOIM, OUTER_JOIN, LEFT_OUTER_JOIN, RIGHT_OUTER_JOIN, FILTER_JOIN.\n");
			return(NULL);
		}
	}

	if(jprop == OUTER_JOIN_PROPERTY){
		for(i=0;i<tbl_vec.size();i++) tbl_vec[i]->set_property(1);
	}
	if(jprop == LEFT_OUTER_JOIN_PROPERTY)
		tbl_vec[0]->set_property(1);
	if(jprop == RIGHT_OUTER_JOIN_PROPERTY)
		tbl_vec[tbl_vec.size()-1]->set_property(1);
	if(jprop == FILTER_JOIN_PROPERTY){
		if(fta_tree->get_from()->get_temporal_range() == 0){
			fprintf(stderr,"ERROR, a filter join must have a non-zero tempoal range.\n");
			return NULL;
		}
		if(tbl_vec.size() != 2){
			fprintf(stderr,"ERROR, a filter join must be between two table variables.\n");
			return NULL;
		}
		colref_t *cr = fta_tree->get_from()->get_colref();
		string field = cr->get_field();

		int fi0 = schema->get_field_idx(tbl_vec[0]->get_schema_name(), field);
		if(fi0 < 0){
			fprintf(stderr,"ERROR, temporal attribute %s for a filter join can't be found in schema %s\n",field.c_str(), tbl_vec[0]->get_schema_name().c_str());
			return NULL;
		}
		cr->set_schema_ref(tbl_vec[0]->get_schema_ref());
		cr->set_tablevar_ref(0);
		string type_name = schema->get_type_name(tbl_vec[0]->get_schema_ref(),field);
		param_list *modifiers = schema->get_modifier_list(cr->get_schema_ref(), field);
		data_type *dt0 = new data_type(type_name, modifiers);
		if(dt0->get_type_str() != "UINT"){
			fprintf(stderr,"ERROR, the temporal attribute in a filter join must be a UINT.\n");
			return NULL;
		}
		if(! dt0->is_increasing()){
			fprintf(stderr,"ERROR, the temporal attribtue in a filter join must be temporal increasing.\n");
			return NULL;
		}
	}



/////////////////////
///		Build the param table
	vector<var_pair_t *> plist = fta_tree->plist;
	int p;
	for(p=0;p<plist.size();++p){
		string pname = plist[p]->name;
		string dtname = plist[p]->val;

		if(pname == ""){
			fprintf(stderr,"ERROR parameter has empty name.\n");
			found_error = true;
		}
		if(dtname == ""){
			fprintf(stderr,"ERROR parameter %s has empty type.\n",pname.c_str());
			found_error = true;
		}
		data_type *dt = new data_type(dtname);
		if(!(dt->is_defined())){
			fprintf(stderr,"ERROR parameter %s has invalid type (%s).\n",pname.c_str(), dtname.c_str());
			found_error = true;
		}

		qs->add_param(pname, dt, false);
	}
	if(found_error) return(NULL);
//		unpack the param table to a global for easier analysis.
	param_tbl=qs->param_tbl;

//////////////////		MERGE specialized analysis

	if(qs->query_type == MERGE_QUERY){
//			Verify that
//				1) there are two *different* streams ref'd in the FROM clause
//					However, only emit a warning.
//					(can't detect a problem if one of the interfaces is the
//					 default interface).
//				2) They have the same layout (e.g. same types but the
//					names can be different
//				3) the two columns can unambiguously be mapped to
//					fields of the two tables, one per table.  Exception:
//					the column names are the same and exist in both tables.
//					FURTHERMORE the positions must be the same
//				4) after mapping, verify that both colrefs are temporal
//					and in the same direction.
		if(tbl_vec.size() < 2){
			fprintf(stderr,"ERROR, a MERGE query operates over at least 2 tables, %lu were supplied.\n",tbl_vec.size() );
			return(NULL);
		}

		vector<field_entry *> fev0 = schema->get_fields(
			tbl_vec[0]->get_schema_name()
		);


		int cv;
		for(cv=1;cv<tbl_vec.size();++cv){
			vector<field_entry *> fev1 = schema->get_fields(
				tbl_vec[cv]->get_schema_name()
			);

			if(fev0.size() != fev1.size()){
				fprintf(stderr,"ERROR, the input stream %s to the merge operator has different schema than input stream %s.\n",tbl_vec[cv]->get_schema_name().c_str(), tbl_vec[0]->get_schema_name().c_str());
				return(NULL);
			}

//			Only need to ensure that the list of types are the same.
//			THe first table supplies the output colnames,
//			and all temporal properties are lost, except for the
//			merge-by columns.
			int f;
			for(f=0;f<fev0.size();++f){
				data_type dt0(fev0[f]->get_type(),fev0[f]->get_modifier_list());
				data_type dt1(fev1[f]->get_type(),fev1[f]->get_modifier_list());
				if(! dt0.equal_subtypes(&dt1) ){
				fprintf(stderr,"ERROR, the input stream %s to the merge operator has different schema than input stream %s.\n",tbl_vec[cv]->get_schema_name().c_str(), tbl_vec[0]->get_schema_name().c_str());
					return(NULL);
				}
			}
		}

//		copy over the merge-by cols.
		qs->mvars = fta_tree->mergevars;

		if(qs->mvars.size() == 0){	// need to discover the merge vars.
			int mergevar_pos = -1;
			int f;
			for(f=0;f<fev0.size();++f){
				data_type dt0(fev0[f]->get_type(),fev0[f]->get_modifier_list());
				if(dt0.is_temporal()){
					mergevar_pos = f;
					break;
				}
			}
			if(mergevar_pos >= 0){
				for(cv=0;cv<tbl_vec.size();++cv){
					vector<field_entry *> fev1 = schema->get_fields(tbl_vec[cv]->get_schema_name());
					qs->mvars.push_back(new colref_t(tbl_vec[cv]->get_var_name().c_str(),fev1[mergevar_pos]->get_name().c_str() ));
				}
			}else{
				fprintf(stderr,"ERROR, no merge-by column found.\n");
				return(NULL);
			}
		}

//			Ensure same number of tables, merge cols.
		if(tbl_vec.size() != qs->mvars.size()){
			fprintf(stderr,"ERROR, merge query has different numbers of table variables (%lu) and merge columns (%lu)\n",tbl_vec.size(), qs->mvars.size());
			return(NULL);
		}

//		Ensure that the merge-by are from different tables
//		also, sort colrefs so that they align with the FROM list using tmp_crl
		set<int> refd_sources;
		vector<colref_t *> tmp_crl(qs->mvars.size(),NULL);
		for(cv=0;cv<qs->mvars.size();++cv){
			int tblvar=infer_tablevar_from_colref(qs->mvars[cv],fta_tree->fm,schema);
			if(tblvar<0){
				fprintf(stderr,"ERROR, Merge column %d (%s) was not found in any of the tables.\n",cv,qs->mvars[cv]->to_string().c_str());
			}
			refd_sources.insert(tblvar);
			tmp_crl[tblvar] = qs->mvars[cv];
		}
		if(refd_sources.size() != qs->mvars.size()){
			fprintf(stderr,"ERROR, The %lu merge columns reference only %lu table variables.\n",qs->mvars.size(), refd_sources.size());
			return(NULL);
		}

//			1-1 mapping, so use tmp_crl as the merge column list.
		qs->mvars = tmp_crl;



//			Look up the colrefs in their schemas, verify that
//			they are at the same place, that they are both temporal
//			in the same way.
//			It seems that this should be done more in the schema objects.
		int fi0 = schema->get_field_idx(tbl_vec[0]->get_schema_name(), qs->mvars[0]->get_field());
		if(fi0 < 0){
			fprintf(stderr,"ERROR, Merge temporal field %s not found.\n",qs->mvars[0]->get_field().c_str());
			exit(1);
		}
		for(cv=1;cv<qs->mvars.size();++cv){
			int fi1 = schema->get_field_idx(tbl_vec[cv]->get_schema_name(), qs->mvars[0]->get_field());
			if(fi0!=fi1){
				fprintf(stderr,"ERROR, the merge columns for table variables %s and %s must be in the same position.\n",tbl_vec[0]->get_var_name().c_str(), tbl_vec[cv]->get_var_name().c_str());
				return NULL;
			}
		}

		field_entry *fe0 = schema->get_field(tbl_vec[0]->get_schema_name(),fi0);
		data_type dt0(fe0->get_type(),fe0->get_modifier_list());
		if( (!dt0.is_temporal()) ){
			fprintf(stderr,"ERROR, merge column %d must be temporal.\n",0);
			return(NULL);
		}
		for(cv=0;cv<qs->mvars.size();++cv){
			field_entry *fe1 = schema->get_field(tbl_vec[cv]->get_schema_name(),fi0);
			data_type dt1(fe1->get_type(),fe1->get_modifier_list());
			if( (!dt1.is_temporal()) ){
				fprintf(stderr,"ERROR, merge column %d must be temporal.\n",cv);
				return(NULL);
			}


			if( dt0.get_temporal() != dt1.get_temporal()){
				fprintf(stderr,"ERROR, the merge columns (0 and %d) must be temporal in the same direction.\n",cv);
				return(NULL);
			}
		}

//			If there is a SLACK specification, verify
//			that it is literal-only and that its type is compatible
//			with that of the merge columns
		qs->slack = fta_tree->slack;
		if(qs->slack){
			if(! literal_only_se(qs->slack)){
				fprintf(stderr,"ERROR, the SLACK expression is not literal-only.\n");
				return NULL;
			}

			assign_data_types(qs->slack, schema, fta_tree, Ext_fcns );
			data_type sdt(&dt0, qs->slack->get_data_type(), string("+"));
			if(sdt.get_type() == undefined_t){
				fprintf(stderr,"ERROR, the SLACK expression data type is not compatible with the data type of the merge columns.\n");
				return NULL;
			}
		}


//			All the tests have passed, there is nothing
//			else to fill in.

	}

//////////////////		SELECT specialized analysis

	if(qs->query_type == SELECT_QUERY){
//		unpack the gb_tbl, aggr_tbl, param_tbl, and complex_literals
//		objects into globals, for easier syntax.
	gb_tbl = qs->gb_tbl;
	aggr_tbl = qs->aggr_tbl;


//		Build the table of group-by attributes.
//		(se processing done automatically).
//		NOTE : Doing the SE processing here is getting cumbersome,
//			I should process these individually.
//		NOTE : I should check for duplicate names.
//		NOTE : I should ensure that the def of one GB does not
//			refrence the value of another.
	vector<extended_gb_t *> gb_list = fta_tree->get_groupby();
	int n_temporal = 0;
	string temporal_gbvars = "";
	map<string, int> gset_gbnames;

//		For generating the set of GB patterns for this aggregation query.
	vector<bool> inner_pattern;
	vector<vector<bool> > pattern_set;
	vector<vector<vector<bool> > > pattern_components;

	vector<gb_t *> r_gbs, c_gbs, g_gbs;
	int n_patterns;

	for(i=0;i<gb_list.size();i++){
		switch(gb_list[i]->type){
		case gb_egb_type:
			retval = gb_tbl->add_gb_attr(
				gb_list[i]->gb, fta_tree->fm, schema,fta_tree, Ext_fcns
			);
			if(retval < 0){
				found_error = true;
			}else{
				if(gb_tbl->get_data_type(i)->is_temporal()){
					n_temporal++;
					if(temporal_gbvars != "") temporal_gbvars+=" ";
					temporal_gbvars += gb_tbl->get_name(i);
				}
			}

			inner_pattern.clear();
			pattern_set.clear();
			inner_pattern.push_back(true);
			pattern_set.push_back(inner_pattern);
			pattern_components.push_back(pattern_set);

			gb_tbl->gb_entry_type.push_back("");
			gb_tbl->gb_entry_count.push_back(1);
			gb_tbl->pattern_components.push_back(pattern_set);

		break;
		case rollup_egb_type:
			r_gbs = gb_list[i]->gb_lists[0]->get_gb_list();
			for(j=0;j<r_gbs.size();++j){
				retval = gb_tbl->add_gb_attr(
					r_gbs[j], fta_tree->fm, schema,fta_tree, Ext_fcns
				);
				if(retval < 0){
					found_error = true;
				}else{		// rollup gb can't be temporal
					gb_tbl->reset_temporal(gb_tbl->size()-1);
				}
			}

			inner_pattern.resize(r_gbs.size());
			pattern_set.clear();
			for(j=0;j<=r_gbs.size();++j){
				for(k=0;k<r_gbs.size();++k){
					if(k < j)
						inner_pattern[k] = true;
					else
						inner_pattern[k] = false;
				}
				pattern_set.push_back(inner_pattern);
			}
			pattern_components.push_back(pattern_set);

			gb_tbl->gb_entry_type.push_back("ROLLUP");
			gb_tbl->gb_entry_count.push_back(r_gbs.size());
			gb_tbl->pattern_components.push_back(pattern_set);
		break;
		case cube_egb_type:
			c_gbs = gb_list[i]->gb_lists[0]->get_gb_list();
			for(j=0;j<c_gbs.size();++j){
				retval = gb_tbl->add_gb_attr(
					c_gbs[j], fta_tree->fm, schema,fta_tree, Ext_fcns
				);
				if(retval < 0){
					found_error = true;
				}else{		// cube gb can't be temporal
					gb_tbl->reset_temporal(gb_tbl->size()-1);
				}
			}

			inner_pattern.resize(c_gbs.size());
			pattern_set.clear();
			n_patterns = 1 << c_gbs.size();
			for(j=0;j<n_patterns;++j){
				int test_bit = 1;
				for(k=0;k<c_gbs.size();++k,test_bit = test_bit << 1){
					if((j & test_bit) != 0)
						inner_pattern[k] = true;
					else
						inner_pattern[k] = false;
				}
				pattern_set.push_back(inner_pattern);
			}
			pattern_components.push_back(pattern_set);

			gb_tbl->gb_entry_type.push_back("CUBE");
			gb_tbl->gb_entry_count.push_back(c_gbs.size());
			gb_tbl->pattern_components.push_back(pattern_set);
		break;
		case gsets_egb_type:
		{
			gset_gbnames.clear();
			for(j=0;j<gb_list[i]->gb_lists.size();++j){
				g_gbs = gb_list[i]->gb_lists[j]->get_gb_list();
				for(k=0;k<g_gbs.size();++k){
					if(g_gbs[k]->type != GB_COLREF){
						fprintf(stderr,"Error, group-by fields in a GROUPING_SETS clause must be table references, not computed values (field is %s\n",g_gbs[k]->name.c_str());
						found_error = true;
					}else{
						if(gset_gbnames.count(g_gbs[k]->name) == 0){
							retval = gb_tbl->add_gb_attr(
								g_gbs[k], fta_tree->fm, schema,fta_tree, Ext_fcns
							);
							if(retval < 0){
								found_error = true;
							}else{		// gsets gb can't be temporal
								gb_tbl->reset_temporal(gb_tbl->size()-1);
							}
							int pos = gset_gbnames.size();
							gset_gbnames[g_gbs[k]->name] = pos;
						}
					}
				}
			}

			if(gset_gbnames.size() > 63){
				fprintf(stderr,"Error, at most 63 distinct fields can be referenced in a GROUPING_SETS clause.\n");
				found_error = true;
			}

			inner_pattern.resize(gset_gbnames.size());
			pattern_set.clear();
			set<unsigned long long int> signatures;
			for(j=0;j<gb_list[i]->gb_lists.size();++j){
				g_gbs = gb_list[i]->gb_lists[j]->get_gb_list();
				set<string> refd_gbs;
				for(k=0;k<g_gbs.size();++k){
					refd_gbs.insert(g_gbs[k]->name);
				}
				fill(inner_pattern.begin(),inner_pattern.end(),false);
				unsigned long long int signature = 0;
				set<string>::iterator ssi;
				for(ssi = refd_gbs.begin(); ssi != refd_gbs.end(); ++ssi){
					inner_pattern[gset_gbnames[(*ssi)]] = true;
					signature |= (1 << gset_gbnames[(*ssi)]);
				}
				if(signatures.count(signature)){
					fprintf(stderr,"Warning, duplicate GROUPING_SETS pattern found, ignoring:\n\t");
					set<string>::iterator ssi;
					for(ssi = refd_gbs.begin(); ssi != refd_gbs.end(); ++ssi){
						fprintf(stderr," %s",(*ssi).c_str());
					}
					fprintf(stderr,"\n");
				}else{
					signatures.insert(signature);
					pattern_set.push_back(inner_pattern);
				}
			}
			pattern_components.push_back(pattern_set);

			gb_tbl->gb_entry_type.push_back("GROUPING_SETS");
			gb_tbl->gb_entry_count.push_back(gset_gbnames.size());
			gb_tbl->pattern_components.push_back(pattern_set);
		}
		break;
		default:
		break;
		}
	}
	if(found_error) return(NULL);
	if(n_temporal > 1){
		fprintf(stderr,"ERROR, query has multiple temporal group-by variables (%s).  Cast away the temporality of all but one of these.\n", temporal_gbvars.c_str());
		return NULL;
	}

//		Compute the set of patterns.  Take the cross product of all pattern components.
	vector<vector<bool> > gb_patterns;
	int n_components = pattern_components.size();
	vector<int> pattern_pos(n_components,0);
	bool done = false;
	while(! done){
		vector<bool> pattern;
		for(j=0;j<n_components;j++){
			pattern.insert(pattern.end(),pattern_components[j][pattern_pos[j]].begin(),
				pattern_components[j][pattern_pos[j]].end());
		}
		gb_patterns.push_back(pattern);
		for(j=0;j<n_components;j++){
			pattern_pos[j]++;
			if(pattern_pos[j] >= pattern_components[j].size())
				pattern_pos[j] = 0;
			else
				break;
		}
		if(j >= n_components)
			done = true;
	}
	gb_tbl->gb_patterns = gb_patterns;


//		Process the supergroup, if any.
	vector<colref_t *> sgb = fta_tree->get_supergb();
	for(i=0;i<sgb.size();++i){
		int gbr = gb_tbl->find_gb(sgb[i],fta_tree->fm, schema);
		if(gbr < 0){
			fprintf(stderr, "ERROR, supergroup attribute %s is not defined as a group-by variable.\n",sgb[i]->to_string().c_str());
			found_error = true;
		}
		if(qs->sg_tbl.count(gbr)){
			fprintf(stderr,"WARNING, duplicate supergroup attribute %s.\n",sgb[i]->to_string().c_str());
		}
		qs->sg_tbl.insert(gbr);
	}
	if(found_error) return(NULL);

	if(qs->sg_tbl.size() > 0 && gb_tbl->gb_patterns.size()>0){
		fprintf(stderr,"Error, SUPERGROUP incompatible with CUBE, ROLLUP, and GROUPING_SETS.\n");
		return NULL;
	}



	predicate_t *wh = fta_tree->get_where();
	predicate_t *hv = fta_tree->get_having();
	predicate_t *cw = fta_tree->get_cleaning_when();
	predicate_t *cb = fta_tree->get_cleaning_by();
	predicate_t *closew = fta_tree->get_closing_when();

	if(closew != NULL  && gb_tbl->gb_patterns.size()>1){
		fprintf(stderr,"Error, CLOSING_WHEN incompatible with CUBE, ROLLUP, and GROUPING_SETS.\n");
		return NULL;
	}



//		Verify that all column references are valid, and if so assign
//		the data type.

	vector<select_element *> sl_list = fta_tree->get_sl_vec();
	for(i=0;i<sl_list.size();i++){
		retval = verify_colref(sl_list[i]->se, fta_tree->fm, schema, gb_tbl);
		if(retval < 0) found_error = true;
	}
	if(wh != NULL)
		retval = verify_predicate_colref(wh, fta_tree->fm, schema, gb_tbl);
	if(retval < 0) found_error = true;
	if(hv != NULL)
		retval = verify_predicate_colref(hv, fta_tree->fm, schema, gb_tbl);
	if(retval < 0) found_error = true;
	if(cw != NULL)
		retval = verify_predicate_colref(cw, fta_tree->fm, schema, gb_tbl);
	if(retval < 0) found_error = true;
	if(cb != NULL)
		retval = verify_predicate_colref(cb, fta_tree->fm, schema, gb_tbl);
	if(retval < 0) found_error = true;
	if(closew != NULL)
		retval = verify_predicate_colref(closew, fta_tree->fm, schema, gb_tbl);
	if(retval < 0) found_error = true;

	if(found_error) return(NULL);

//		Verify that all of the scalar expressions
//		and comparison predicates have compatible types.

	n_temporal = 0;
	string temporal_output_fields;
	for(i=0;i<sl_list.size();i++){
		retval = assign_data_types(sl_list[i]->se, schema, fta_tree, Ext_fcns );
		if(retval < 0) found_error = true;
		if(sl_list[i]->se->get_data_type()->is_temporal()){
			n_temporal++;
			temporal_output_fields += " "+int_to_string(i);
		}
	}
	if(n_temporal > 1){
		fprintf(stderr,"ERROR, query has multiple temporal output fields (positions%s).  Cast away the temporality of all but one of these.\n", temporal_output_fields.c_str());
		found_error=true;
	}
	if(wh != NULL)
		retval = assign_predicate_data_types(wh, schema, fta_tree, Ext_fcns);
	if(retval < 0) found_error = true;
	if(hv != NULL)
		retval = assign_predicate_data_types(hv, schema, fta_tree, Ext_fcns);
	if(retval < 0) found_error = true;
	if(cw != NULL)
		retval = assign_predicate_data_types(cw, schema, fta_tree, Ext_fcns);
	if(retval < 0) found_error = true;
	if(cb != NULL)
		retval = assign_predicate_data_types(cb, schema, fta_tree, Ext_fcns);
	if(retval < 0) found_error = true;
	if(closew != NULL)
		retval = assign_predicate_data_types(closew, schema, fta_tree, Ext_fcns);
	if(retval < 0) found_error = true;

	if(found_error) return(NULL);

//			Impute names for the unnamed columns.
	set<string> curr_names;
	int s;
	for(s=0;s<sl_list.size();++s){
		curr_names.insert(sl_list[s]->name);
	}
	for(s=0;s<sl_list.size();++s){
		if(sl_list[s]->name == "")
			sl_list[s]->name = impute_colname(curr_names, sl_list[s]->se);
	}


//		Check the aggregates.
//		No aggrs allowed in the WHERE predicate.
//		(no aggrs in the GB defs, but that is examined elsewhere)
//		Therefore, aggregates are allowed only the select clause.
//
//		The query is an aggregation query if there is a group-by clause, or
//		if any aggregate is referenced.  If there is a group-by clause,
//		at least one aggregate must be referenced.
//		If the query is an aggregate query, the scalar expressions in
//		the select clause can reference only constants, aggregates, or group-by
//		attributes.
//		Also, if the query is an aggregate query, build a table referencing
//		the aggregates.
//
//		No nested aggregates allowed.
//

//		First, count references in the WHERE predicate.
//		(if there are any references, report an error).
//			can ref group vars, tuple fields, and stateful fcns.

	if(wh != NULL){
		retval = count_aggr_pred(wh, true);
		if(retval > 0){
			fprintf(stderr,"ERROR, no aggregate references are allowed in the WHERE clause.\n");
			return(NULL);
		}
	}

//		NOTE : Here I need an analysis of the having clause
//		to verify that it only refs GB attrs and aggregates.
//			(also, superaggregates, stateful fcns)
	if(hv!=NULL){
		retval = verify_having_pred(hv, "HAVING", Ext_fcns);
		if(retval < 0) return(NULL);
	}

//		Cleaning by has same reference rules as Having
	if(cb!=NULL){
		retval = verify_having_pred(cb, "CLEANING_BY", Ext_fcns);
		if(retval < 0) return(NULL);
	}

//		Cleaning when has same reference rules as Having,
//		except that references to non-superaggregates are not allowed.
//		This is tested for when "CLEANING_BY" is passed in as the clause.
	if(cw!=NULL){
		retval = verify_having_pred(cw, "CLEANING_WHEN", Ext_fcns);
		if(retval < 0) return(NULL);
	}

//		CLOSING_WHEN : same rules as HAVING
	if(closew!=NULL){
		retval = verify_having_pred(closew, "CLOSING_WHEN", Ext_fcns);
		if(retval < 0) return(NULL);
	}


//		Collect aggregates in the HAVING and CLEANING clauses
	if(hv != NULL){
		build_aggr_tbl_fm_pred(hv, aggr_tbl, Ext_fcns);
	}
	if(cw != NULL){
		build_aggr_tbl_fm_pred(cw, aggr_tbl, Ext_fcns);
	}
	if(cb != NULL){
		build_aggr_tbl_fm_pred(cb, aggr_tbl, Ext_fcns);
	}
	if(closew != NULL){
		build_aggr_tbl_fm_pred(closew, aggr_tbl, Ext_fcns);
	}

//		Collect aggregate refs in the SELECT clause.

	for(i=0;i<sl_list.size();i++)
		build_aggr_tbl_fm_se(sl_list[i]->se, aggr_tbl, Ext_fcns);


//		Collect references to states of stateful functions
	if(wh != NULL){
		gather_fcn_states_pr(wh, qs->states_refd, Ext_fcns);
	}
	if(hv != NULL){
		gather_fcn_states_pr(hv, qs->states_refd, Ext_fcns);
	}
	if(cw != NULL){
		gather_fcn_states_pr(cw, qs->states_refd, Ext_fcns);
	}
	if(cb != NULL){
		gather_fcn_states_pr(cb, qs->states_refd, Ext_fcns);
	}
	if(closew != NULL){			// should be no stateful fcns here ...
		gather_fcn_states_pr(closew, qs->states_refd, Ext_fcns);
	}
	for(i=0;i<sl_list.size();i++)
		gather_fcn_states_se(sl_list[i]->se, qs->states_refd, Ext_fcns);


//		If this is an aggregate query, it had normally references
//		some aggregates.  Its not necessary though, just emit a warning.
//		(acts as SELECT DISTINCT)

	bool is_aggr_query = gb_tbl->size() > 0 || aggr_tbl->size() > 0;
	if(is_aggr_query && aggr_tbl->size() == 0){
		fprintf(stderr,"Warning, query contains a group-by clause but does not reference aggregates..\n");
	}

//		If this is an aggregate query,
//			1) verify that the SEs in the SELECT clause reference
//				only constants, aggregates, and group-by attributes.
//			2) No aggregate scalar expression references an aggregate
//				or any stateful function.
//			3) either it references both CLEANING clauses or neither.
//			4) all superaggregates must have the superaggr_allowed property.
//			5) all aggregates ref'd in the CLEANING_WHEN ad CLEANING_BY
//			   clauses must have the multiple_output property.


	if(is_aggr_query){
		if(gb_list.size() == 0){
			fprintf(stderr,"ERROR, aggregation queries must have at least one group-by variable (which should be temporal).\n");
			return NULL;
		}
//			Ensure that at least one gbvar is temporal
		if(! fta_tree->name_exists("no_temporal_aggr")){
			bool found_temporal = false;
    		for(i=0;i<gb_tbl->size();i++){
				if(gb_tbl->get_data_type(i)->is_temporal()){
					found_temporal = true;
				}
			}
			if(! found_temporal){
				fprintf(stderr,"ERROR, at least one of the group-by variables must be temporal (unless no_temporal_aggr is set)\n");
				exit(1);
			}
		}

		if((!cb && cw) || (cb && !cw)){
			fprintf(stderr,"ERROR, an aggregate query must either include both a CLEANING_WHEN and a CLEANING_BY clause, or neither.\n");
			return(NULL);
		}

		bool refs_running = false;
		int a;
		for(a=0; a<aggr_tbl->size(); ++a){
			refs_running |= aggr_tbl->is_running_aggr(a);
		}

		if(closew){
			if(cb || cw){
				fprintf(stderr, "ERROR, cannot reference both CLOSING_WHEN and either CLEANING_WHEN or CLEANING_BY.\n");
				return(NULL);
			}
			if(!refs_running){
				fprintf(stderr, "ERROR, if you reference CLOSING_WHEN you must reference at least one running window aggregate.\n");
				return(NULL);
			}
		}

		if(refs_running && !closew){
				fprintf(stderr, "ERROR, if you reference a running window aggregate you must reference a CLOSING_WHEN clause.\n");
			return(NULL);
		}

		bool st_ok = true;
		for(i=0;i<sl_list.size();i++){
			bool ret_bool = verify_aggr_query_se(sl_list[i]->se);
			st_ok = st_ok && ret_bool;
		}
		if(! st_ok)
			return(NULL);

		for(i=0;i<aggr_tbl->size();i++){
			if(aggr_tbl->is_superaggr(i)){
				if(! aggr_tbl->superaggr_allowed(i)){
					fprintf(stderr,"ERROR, aggregate %s cannot be a superaggregate\n",aggr_tbl->get_op(i).c_str());
					return NULL;
				}
			}
			if(aggr_tbl->is_builtin(i)){
				if(count_aggr_se(aggr_tbl->get_aggr_se(i), true) > 0){
					fprintf(stderr,"ERROR no nested aggregation allowed.\n");
					return(NULL);
				}
			}else{
				vector<scalarexp_t *> opl = aggr_tbl->get_operand_list(i);
				int o;
				for(o=0;o<opl.size();++o){
					if(count_aggr_se(opl[o], true) > 0){
						fprintf(stderr,"ERROR no nested aggregation allowed.\n");
						return(NULL);
					}
				}
			}
		}
	}else{
//			Ensure that non-aggregate query doesn't reference some things
		if(cb || cw){
			fprintf(stderr,"ERROR, a non-aggregate query may not reference a CLEANING_WHEN or a CLEANING_BY clause.\n");
			return(NULL);
		}
		if(closew){
			fprintf(stderr,"ERROR, a non-aggregate query may not reference a CLOSING_WHEN clause.\n");
			return(NULL);
		}
		if(qs->states_refd.size()){
			fprintf(stderr,"ERROR, a non-aggregate query may not refernece stateful functions.\n");
			return(NULL);
		}
	}



//		Convert the predicates into CNF.  OK to pass NULL ptr.
	make_cnf_from_pr(wh, qs->wh_cnf);
	make_cnf_from_pr(hv, qs->hav_cnf);
	make_cnf_from_pr(cb, qs->cb_cnf);
	make_cnf_from_pr(cw, qs->cw_cnf);
	make_cnf_from_pr(closew, qs->closew_cnf);

//		Analyze the predicates.

	for(i=0;i<qs->wh_cnf.size();i++)
		analyze_cnf(qs->wh_cnf[i]);
	for(i=0;i<qs->hav_cnf.size();i++)
		analyze_cnf(qs->hav_cnf[i]);
	for(i=0;i<qs->cb_cnf.size();i++)
		analyze_cnf(qs->cb_cnf[i]);
	for(i=0;i<qs->cw_cnf.size();i++)
		analyze_cnf(qs->cw_cnf[i]);
	for(i=0;i<qs->closew_cnf.size();i++)
		analyze_cnf(qs->closew_cnf[i]);


//			At this point, the old analysis program
//			gathered all refs to partial functions,
//			complex literals, and parameters accessed via a handle.
//			I think its better to delay this
//			until code generation time, as the query will be
//			in general split.

    }

	return(qs);
}

///////////////////////////////////////////////////////////////////////

//		Expand gbvars with their definitions.

scalarexp_t *expand_gbvars_se(scalarexp_t *se, gb_table &gb_tbl){
	int o;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return se;
	case SE_UNARY_OP:
		se->lhs.scalarp = expand_gbvars_se(se->get_left_se(),gb_tbl);
		return se;
	case SE_BINARY_OP:
		se->lhs.scalarp = expand_gbvars_se(se->get_left_se(),gb_tbl);
		se->rhs.scalarp = expand_gbvars_se(se->get_right_se(),gb_tbl);
		return se;
	case SE_COLREF:
		if( se->is_gb() ){
			return( dup_se(gb_tbl.get_def(se->get_gb_ref()),NULL) );
		}
		return se;
//			don't descend into aggr defs.
	case SE_AGGR_STAR:
		return se;
	case SE_AGGR_SE:
		return se;
	case SE_FUNC:
		for(o=0;o<se->param_list.size();o++){
			se->param_list[o] = expand_gbvars_se(se->param_list[o], gb_tbl);
		}
		return se;
	default:
		fprintf(stderr,"INTERNAL ERROR in expand_gbvars, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return se;
}

void expand_gbvars_pr(predicate_t *pr, gb_table &gb_tbl){
	vector<scalarexp_t *> op_list;
	int o;
	bool found = false;

	switch(pr->get_operator_type()){
	case PRED_IN:
		pr->lhs.sexp = expand_gbvars_se(pr->get_left_se(), gb_tbl);
		return;
	case PRED_COMPARE:
		pr->lhs.sexp = expand_gbvars_se(pr->get_left_se(),gb_tbl) ;
		pr->rhs.sexp = expand_gbvars_se(pr->get_right_se(),gb_tbl) ;
		return;
	case PRED_UNARY_OP:
		expand_gbvars_pr(pr->get_left_pr(),gb_tbl) ;
		return;
	case PRED_BINARY_OP:
		expand_gbvars_pr(pr->get_left_pr(),gb_tbl) ;
		expand_gbvars_pr(pr->get_right_pr(),gb_tbl) ;
		return;
	case PRED_FUNC:
		for(o=0;o<pr->param_list.size();++o){
			pr->param_list[o] = expand_gbvars_se(pr->param_list[o],gb_tbl) ;
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in expand_gbvars_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
	}
	return;
}




//		return true if the se / pr contains any gbvar on the list.


bool contains_gb_se(scalarexp_t *se, set<int> &gref_set){
	vector<scalarexp_t *> operands;
	int o;
	bool found = false;

	switch(se->get_operator_type()){
	case SE_LITERAL:
	case SE_PARAM:
	case SE_IFACE_PARAM:
		return false;
	case SE_UNARY_OP:
		return contains_gb_se(se->get_left_se(),gref_set);
	case SE_BINARY_OP:
		return( contains_gb_se(se->get_left_se(),gref_set) ||
			contains_gb_se(se->get_right_se(),gref_set) );
	case SE_COLREF:
		if( se->is_gb() ){
			return( gref_set.count(se->get_gb_ref()) > 0);
		}
		return false;
//			don't descend into aggr defs.
	case SE_AGGR_STAR:
		return false;
	case SE_AGGR_SE:
		return false;
	case SE_FUNC:
		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			found = found || contains_gb_se(operands[o], gref_set);
		}
		return found;
	default:
		fprintf(stderr,"INTERNAL ERROR in contains_gb_se, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
	return false;
}


bool contains_gb_pr(predicate_t *pr, set<int> &gref_set){
	vector<scalarexp_t *> op_list;
	int o;
	bool found = false;

	switch(pr->get_operator_type()){
	case PRED_IN:
		return contains_gb_se(pr->get_left_se(), gref_set);
	case PRED_COMPARE:
		return (contains_gb_se(pr->get_left_se(),gref_set)
			|| contains_gb_se(pr->get_right_se(),gref_set) );
	case PRED_UNARY_OP:
		return contains_gb_pr(pr->get_left_pr(),gref_set) ;
	case PRED_BINARY_OP:
		return (contains_gb_pr(pr->get_left_pr(),gref_set)
			|| contains_gb_pr(pr->get_right_pr(),gref_set) );
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			found = found ||contains_gb_se(op_list[o],gref_set) ;
		}
		return found;
	default:
		fprintf(stderr,"INTERNAL ERROR in contains_gb_pr, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
	}

	return found;
}


//		Gather the set of columns accessed in this se.
//		Descend into aggregate functions.

void gather_se_col_ids(scalarexp_t *se, col_id_set &cid_set, gb_table *gtbl){
	col_id ci;
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
		gather_se_col_ids(se->get_left_se(),cid_set,gtbl);
		return;
	case SE_BINARY_OP:
		gather_se_col_ids(se->get_left_se(),cid_set,gtbl);
		gather_se_col_ids(se->get_right_se(),cid_set,gtbl);
		return;
	case SE_COLREF:
		if(! se->is_gb() ){
			ci.load_from_colref(se->get_colref() );
			if(ci.tblvar_ref < 0){
				fprintf(stderr,"INTERNAL WARNING: unbound colref (%s) accessed.\n",ci.field.c_str());
			}
			cid_set.insert(ci);
		}else{
			if(gtbl==NULL){
				fprintf(stderr,"INTERNAL ERROR: gbvar ref in gather_se_col_ids, but gtbl is NULL.\n");
				exit(1);
			}
			gather_se_col_ids(gtbl->get_def(se->get_gb_ref()),cid_set,gtbl);
		}
		return;
	case SE_AGGR_STAR:
		return;
	case SE_AGGR_SE:
		gather_se_col_ids(se->get_left_se(),cid_set,gtbl);
		return;
	case SE_FUNC:
		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			gather_se_col_ids(operands[o], cid_set,gtbl);
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in gather_se_col_ids, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
}


//		Gather the set of columns accessed in this se.

void gather_pr_col_ids(predicate_t *pr, col_id_set &cid_set, gb_table *gtbl){
	vector<scalarexp_t *> op_list;
	int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		gather_se_col_ids(pr->get_left_se(), cid_set,gtbl);
		return;
	case PRED_COMPARE:
		gather_se_col_ids(pr->get_left_se(),cid_set,gtbl) ;
		gather_se_col_ids(pr->get_right_se(),cid_set,gtbl) ;
		return;
	case PRED_UNARY_OP:
		gather_pr_col_ids(pr->get_left_pr(),cid_set,gtbl) ;
		return;
	case PRED_BINARY_OP:
		gather_pr_col_ids(pr->get_left_pr(),cid_set,gtbl) ;
		gather_pr_col_ids(pr->get_right_pr(),cid_set,gtbl) ;
		return;
	case PRED_FUNC:
		op_list = pr->get_op_list();
		for(o=0;o<op_list.size();++o){
			gather_se_col_ids(op_list[o],cid_set,gtbl) ;
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in gather_pr_col_ids, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
	}
}




//		Gather the set of special operator or comparison functions referenced by this se.

void gather_se_opcmp_fcns(scalarexp_t *se, set<string> &fcn_set){
	col_id ci;
	data_type *ldt, *rdt;
	int o;
	vector<scalarexp_t *> operands;

	switch(se->get_operator_type()){
	case SE_LITERAL:
		if( se->get_literal()->constructor_name() != "")
			fcn_set.insert( se->get_literal()->constructor_name() );
		return;
	case SE_PARAM:
		return;
//			SE_IFACE_PARAM should not exist when this is called.
	case SE_UNARY_OP:
		ldt = se->get_left_se()->get_data_type();
		if(ldt->complex_operator(se->get_op()) ){
			fcn_set.insert( ldt->get_complex_operator(se->get_op()) );
		}
		gather_se_opcmp_fcns(se->get_left_se(),fcn_set);
		return;
	case SE_BINARY_OP:
		ldt = se->get_left_se()->get_data_type();
		rdt = se->get_right_se()->get_data_type();

		if(ldt->complex_operator(rdt, se->get_op()) ){
			fcn_set.insert( ldt->get_complex_operator(rdt, se->get_op()) );
		}
		gather_se_opcmp_fcns(se->get_left_se(),fcn_set);
		gather_se_opcmp_fcns(se->get_right_se(),fcn_set);
		return;
	case SE_COLREF:
		return;
	case SE_AGGR_STAR:
		return;
	case SE_AGGR_SE:
		gather_se_opcmp_fcns(se->get_left_se(),fcn_set);
		return;
	case SE_FUNC:
		operands = se->get_operands();
		for(o=0;o<operands.size();o++){
			gather_se_opcmp_fcns(operands[o], fcn_set);
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in gather_se_opcmp_fcns, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
}


//		Gather the set of special operator or comparison functions referenced by this se.

void gather_pr_opcmp_fcns(predicate_t *pr, set<string> &fcn_set){
	data_type *ldt, *rdt;
	vector<scalarexp_t *> operands;
	int o;

	switch(pr->get_operator_type()){
	case PRED_IN:
		ldt = pr->get_left_se()->get_data_type();
		if(ldt->complex_comparison(ldt) ){
			fcn_set.insert( ldt->get_comparison_fcn(ldt) );
		}
		gather_se_opcmp_fcns(pr->get_left_se(), fcn_set);
		return;
	case PRED_COMPARE:
		ldt = pr->get_left_se()->get_data_type();
		rdt = pr->get_right_se()->get_data_type();
		if(ldt->complex_comparison(rdt) ){
			fcn_set.insert( ldt->get_comparison_fcn(rdt) );
		}
		gather_se_opcmp_fcns(pr->get_left_se(),fcn_set) ;
		gather_se_opcmp_fcns(pr->get_right_se(),fcn_set) ;
		return;
	case PRED_UNARY_OP:
		gather_pr_opcmp_fcns(pr->get_left_pr(),fcn_set) ;
		return;
	case PRED_BINARY_OP:
		gather_pr_opcmp_fcns(pr->get_left_pr(),fcn_set) ;
		gather_pr_opcmp_fcns(pr->get_right_pr(),fcn_set) ;
		return;
	case PRED_FUNC:
		operands = pr->get_op_list();
		for(o=0;o<operands.size();o++){
			gather_se_opcmp_fcns(operands[o], fcn_set);
		}
		return;
	default:
		fprintf(stderr,"INTERNAL ERROR in verify_predicate_colref, line %d, character %d, unknown predicate operator type %d\n",
			pr->get_lineno(), pr->get_charno(), pr->get_operator_type() );
	}
}




//		find the temporal variable divisor if any.
//		Only forms allowed : temporal_colref, temporal_colref/const
//		temporal_colref/const + const


long long int find_temporal_divisor(scalarexp_t *se, gb_table *gbt,string &fnm){
	long long int retval = 0;
	data_type *ldt, *rdt;
	int o;
	vector<scalarexp_t *> operands;
	scalarexp_t *t_se, *c_se;
	string the_op;

	switch(se->get_operator_type()){
	case SE_LITERAL:
		return(-1);
	case SE_PARAM:
		return(-1);
//			SE_IFACE_PARAM should not exist when this is called.
	case SE_UNARY_OP:
		return(-1);
	case SE_BINARY_OP:
		ldt = se->get_left_se()->get_data_type();
		if(ldt->is_temporal()){
			t_se = se->get_left_se();
			c_se = se->get_right_se();
		}else{
			t_se = se->get_left_se();
			c_se = se->get_right_se();
		}
		if((! t_se->get_data_type()->is_temporal()) ||  c_se->get_data_type()->is_temporal())
			return -1;

		the_op = se->get_op();
		if(the_op == "+" || the_op == "-")
			return find_temporal_divisor(t_se, gbt,fnm);
		if(the_op == "/"){
			if(t_se->get_operator_type() == SE_COLREF && c_se->get_operator_type() == SE_LITERAL){
				fnm = t_se->get_colref()->get_field();
				string lits = c_se->get_literal()->to_string();
				sscanf(lits.c_str(),"%qd",&retval);
				return retval;
			}
		}

		return -1;
	case SE_COLREF:
		if(se->is_gb()){
			return find_temporal_divisor(gbt->get_def(se->get_gb_ref()), gbt,fnm);
		}
		if(se->get_data_type()->is_temporal()){
			fnm = se->get_colref()->get_field();
			return 1;
		}
		return 0;
	case SE_AGGR_STAR:
		return -1;
	case SE_AGGR_SE:
		return -1;
	case SE_FUNC:
		return -1;
	default:
		fprintf(stderr,"INTERNAL ERROR in find_temporal_divisor, line %d, character %d: unknown operator type %d\n",
				se->get_lineno(), se->get_charno(),se->get_operator_type());
		exit(1);
	}
}


//			impute_colnames:
//			Create meaningful but unique names for the columns.
string impute_colname(vector<select_element *> &sel_list, scalarexp_t *se){
	set<string> curr_names;
	int s;
	for(s=0;s<sel_list.size();++s){
		curr_names.insert(sel_list[s]->name);
	}
	return impute_colname(curr_names, se);
}

string impute_colname(set<string> &curr_names, scalarexp_t *se){
string ret;
scalarexp_t *seo;
vector<scalarexp_t *> operand_list;
string opstr;

	switch(se->get_operator_type()){
	case SE_LITERAL:
		ret = "Literal";
		break;
    case SE_PARAM:
		ret = "Param_" + se->get_param_name();
		break;
    case SE_IFACE_PARAM:
		ret = "Iparam_" + se->get_ifpref()->get_pname();
		break;
    case SE_COLREF:
		ret =  se->get_colref()->get_field() ;
		break;
    case SE_UNARY_OP:
    case SE_BINARY_OP:
		ret = "Field";
		break;
    case SE_AGGR_STAR:
		ret = "Cnt";
		break;
    case SE_AGGR_SE:
		ret = se->get_op();
		seo = se->get_left_se();
		switch(se->get_left_se()->get_operator_type()){
		case SE_PARAM:
			ret += "_PARAM_"+seo->get_param_name();
			break;
		case SE_IFACE_PARAM:
			ret += "_IPARAM_"+seo->get_ifpref()->get_pname();
			break;
		case SE_COLREF:
			opstr =  seo->get_colref()->get_field();
			if(strncmp(ret.c_str(), opstr.c_str(),ret.size())){
				ret += "_" + opstr;
			}else{
				ret = opstr;
			}
			break;
		case SE_AGGR_STAR:
		case SE_AGGR_SE:
			opstr = seo->get_op();
			if(strncmp(ret.c_str(), opstr.c_str(),ret.size())){
				ret += "_" + seo->get_op();
			}else{
				ret = opstr;
			}
			break;
		case SE_FUNC:
			opstr = seo->get_op();
			ret += "_" + seo->get_op();
			break;
    	case SE_UNARY_OP:
    	case SE_BINARY_OP:
			ret += "_SE";
			break;
		default:
			ret += "_";
			break;
		}
		break;
	case SE_FUNC:
		ret = se->get_op();
		operand_list = se->get_operands();
		if(operand_list.size() > 0){
			seo = operand_list[0];
			switch(seo->get_operator_type()){
			case SE_PARAM:
				ret += "_PARAM_"+seo->get_param_name();
				break;
			case SE_IFACE_PARAM:
				ret += "_IPARAM_"+seo->get_ifpref()->get_pname();
				break;
			case SE_COLREF:
				ret += "_" + seo->get_colref()->get_field();
				break;
			case SE_AGGR_STAR:
			case SE_AGGR_SE:
			case SE_FUNC:
				ret += "_" + seo->get_op();
				break;
    		case SE_UNARY_OP:
    		case SE_BINARY_OP:
				ret += "_SE";
			break;
			default:
				ret += "_";
				break;
			}
		}else{
			ret += "_func";
		}
		break;
	}

	if(ret == "Field"){
		if(curr_names.count("Field0") == 0)
			ret = "Field0";
	}
	int iter = 1;
	string base = ret;
	while(curr_names.count(ret) > 0){
		char tmpstr[500];
		sprintf(tmpstr,"%s%d",base.c_str(),iter);
		ret = tmpstr;
		iter++;
	}


	curr_names.insert(ret);
	return(ret);

}



//////////////////////////////////////////////////////////////////////
//////////////		Methods of defined classes ///////////////////////
//////////////////////////////////////////////////////////////////////

//		helper fcn to enable col_id as map key.

  bool operator<(const col_id &cr1, const col_id &cr2){
	if(cr1.tblvar_ref < cr2.tblvar_ref) return(true);
	if(cr1.tblvar_ref == cr2.tblvar_ref)
 	   return (cr1.field < cr2.field);
 	return(false);
  }


//		Process the GB variables.
//		At parse time, GB vars are either GB_COLREF,
//		or GB_COMPUTED if the AS keyword is used.
//		Cast GB vars as named entities with a SE as
//		their definition (the colref in the case of GB_COLREF).
//
//		TODO: if there is a gbref in a gbdef,
//		then I won't be able to compute the value without
//		a complex dependence analysis.  So verify that there is no
//		gbref in any of the GBdefs.
//		BUT: a GBVAR_COLREF should be converted to a regular colref,
//		which is not yet done.
//
//		TODO : sort out issue of GBVAR naming and identification.
//		Determine where it is advantageous to convert GV_COLREF
//		GBVARS to colrefs -- e.g. in group definition, in the WHERE clause,
//		etc.
//
//		return -1 if there is a problem.

int gb_table::add_gb_attr(
						  gb_t *gb,
						  tablevar_list_t *fm,
						  table_list *schema,
						  table_exp_t *fta_tree,
						  ext_fcn_list *Ext_fcns
						  ){
	colref_t *cr;
	int retval;
	gb_table_entry *entry;

	if(gb->type == GB_COLREF){
		if(gb->table != "")
			cr = new colref_t(
				gb->interface.c_str(),gb->table.c_str(), gb->name.c_str()
			);
		else
			cr = new colref_t(gb->name.c_str());

		int tablevar_ref = infer_tablevar_from_colref(cr, fm, schema);
		if(tablevar_ref < 0) return(tablevar_ref);

		cr->set_tablevar_ref(tablevar_ref);
		cr->set_schema_ref(fm->get_schema_ref(tablevar_ref));
		cr->set_interface("");
		cr->set_table_name(fm->get_tablevar_name(tablevar_ref));

		entry = new gb_table_entry();
		entry->name.field = cr->get_field();
		entry->name.tblvar_ref = tablevar_ref;
		entry->definition = new scalarexp_t(cr);
		entry->ref_type = GBVAR_COLREF;
	}else{
		entry = new gb_table_entry();
		entry->name.field = gb->name;
		entry->name.tblvar_ref = -1;
		entry->definition = gb->def;
		entry->ref_type = GBVAR_SE;
	}

	retval = verify_colref(entry->definition, fm, schema, NULL);
	if(retval < 0) return(retval);

	retval = assign_data_types(entry->definition, schema, fta_tree, Ext_fcns);
	if(retval < 0) return(retval);

//		Verify that the gbvar def references no aggregates and no gbvars.
	if(count_gb_se(entry->definition) > 0){
		fprintf(stderr,"ERROR, group-by variable %s references other group-by variables in its definition.\n",entry->name.field.c_str() );
		return(-1);
	}
	if(count_aggr_se(entry->definition, true) > 0){
		fprintf(stderr,"ERROR, group-by variable %s references aggregates in its definition.\n",entry->name.field.c_str() );
		return(-1);
	}

//			Check for duplicates
	int i;
	for(i=0;i<gtbl.size();++i){
		if(entry->name.field == gtbl[i]->name.field){
			fprintf(stderr,"ERROR, duplicate group-by variable name %s, positions %d and %lu.\n",entry->name.field.c_str(),i,gtbl.size());
			return -1;
		}
	}


	gtbl.push_back(entry);

	return(1);
}


//			Try to determine if the colref is actually
//			a gbvar ref.
//			a) if no tablename associated with the colref,
//				1) try to find a matching GB_COMPUTED gbvar.
//				2) failing that, try to match to a single tablevar
//				3) if successful, search among GB_COLREF
//			b) else, try to match the tablename to a single tablevar
//				if successful, search among GB_COLREF
int gb_table::find_gb(colref_t *cr, tablevar_list_t *fm, table_list *schema){
	string c_field = cr->get_field();
	int c_tblref;
	int n_tbl;
	int i;
	vector<int> candidates;

	if(cr->uses_default_table()){
		for(i=0;i<gtbl.size();i++){
			if(gtbl[i]->ref_type==GBVAR_SE && c_field == gtbl[i]->name.field){
				return(i);
			}
		}
		candidates = find_source_tables(c_field, fm, schema);
		if(candidates.size() != 1) return(-1); // can't find unique tablevar
		for(i=0;i<gtbl.size();i++){
			if(gtbl[i]->ref_type==GBVAR_COLREF &&
				  c_field == gtbl[i]->name.field &&
				  candidates[0] == gtbl[i]->name.tblvar_ref){
				return(i);
			}
		}
		return(-1); // colref is not in gb table.
	}

//			A table name must have been given.
	vector<tablevar_t *> fm_tbls = fm->get_table_list();
	string interface = cr->get_interface();
	string table_name = cr->get_table_name();


//			if no interface name is given, try to search for the table
//			name among the tablevar names first.
	if(interface==""){
		for(i=0;i<fm_tbls.size();++i){
			if(table_name == fm_tbls[i]->get_var_name() && interface == fm_tbls[i]->get_interface())
				candidates.push_back(i);
		}
		if(candidates.size()>1) return(-1);
		if(candidates.size()==1){
			for(i=0;i<gtbl.size();i++){
				if(gtbl[i]->ref_type==GBVAR_COLREF &&
				  	c_field == gtbl[i]->name.field &&
				  	candidates[0] == gtbl[i]->name.tblvar_ref){
					return(i);
				}
			}
			return(-1);  // match semantics of bind to tablevar name first
		}
	}

//		Interface name given, or no interface but no
//		no tablevar match.  Try to match on schema name.
	for(i=0;i<fm_tbls.size();++i){
		if(table_name == fm_tbls[i]->get_var_name() && interface == fm_tbls[i]->get_interface())
			candidates.push_back(i);
	}
	if(candidates.size() != 1) return(-1);
	for(i=0;i<gtbl.size();i++){
		if(gtbl[i]->ref_type==GBVAR_COLREF &&
		  	c_field == gtbl[i]->name.field &&
		  	candidates[0] == gtbl[i]->name.tblvar_ref){
			return(i);
		}
	}

//		No match found.
	return(-1);

}



bool aggr_table_entry::fta_legal(ext_fcn_list *Ext_fcns){
	if(is_builtin()){
		if( (op == "COUNT") || (op == "SUM") || (op == "MIN") ||
			(op == "MAX") || (op == "AND_AGGR") || (op == "OR_AGGR") ||
			(op == "XOR_AGGR") )
				return(true);
	}else{
		return Ext_fcns->fta_legal(fcn_id);
	}
	return(false);
}


//		Return the set of subaggregates required to compute
//		the desired aggregate.  THe operand of the subaggregates
//		can only be * or the scalarexp used in the superaggr.
//		This is indicated by the use_se vector.

//		Is this code generation specific?

vector<string> aggr_table_entry::get_subaggr_fcns(vector<bool> &use_se){
	vector<string> ret;

	if(op == "COUNT"){
		ret.push_back("COUNT");
		use_se.push_back(false);
	}
	if(op == "SUM"){
		ret.push_back("SUM");
		use_se.push_back(true);
	}
	if(op == "AVG"){
		ret.push_back("SUM");
		ret.push_back("COUNT");
		use_se.push_back(true);
		use_se.push_back(false);
	}
	if(op == "MIN"){
		ret.push_back("MIN");
		use_se.push_back(true);
	}
	if(op == "MAX"){
		ret.push_back("MAX");
		use_se.push_back(true);
	}
	if(op == "AND_AGGR"){
		ret.push_back("AND_AGGR");
		use_se.push_back(true);
	}
	if(op == "OR_AGGR"){
		ret.push_back("OR_AGGR");
		use_se.push_back(true);
	}
	if(op == "XOR_AGGR"){
		ret.push_back("XOR_AGGR");
		use_se.push_back(true);
	}

	return(ret);
}

//			Code generation specific?

vector<data_type *> aggr_table_entry::get_subaggr_dt(){
	vector<data_type *> ret;
	data_type *dt;

	if(op == "COUNT"){
		dt = new data_type("Int"); // was Uint
		ret.push_back( dt );
	}
	if(op == "SUM"){
		dt = new data_type();
		dt->set_aggr_data_type( "SUM",operand->get_data_type() );
		ret.push_back(dt);
	}
	if(op == "AVG"){
		dt = new data_type();
		dt->set_aggr_data_type( "SUM",operand->get_data_type() );
		ret.push_back( dt );
		dt = new data_type("Int");
		ret.push_back( dt );
	}
	if(op == "MIN"){
		dt = new data_type();
		dt->set_aggr_data_type( "MIN",operand->get_data_type() );
		ret.push_back( dt );
	}
	if(op == "MAX"){
		dt = new data_type();
		dt->set_aggr_data_type( "MAX",operand->get_data_type() );
		ret.push_back( dt );
	}
	if(op == "AND_AGGR"){
		dt = new data_type();
		dt->set_aggr_data_type( "AND_AGGR",operand->get_data_type() );
		ret.push_back( dt );
	}
	if(op == "OR_AGGR"){
		dt = new data_type();
		dt->set_aggr_data_type( "OR_AGGR",operand->get_data_type() );
		ret.push_back( dt );
	}
	if(op == "XOR_AGGR"){
		dt = new data_type();
		dt->set_aggr_data_type( "XOR_AGGR",operand->get_data_type() );
		ret.push_back( dt );
	}

	return(ret);
}

//		Code generation specific?

scalarexp_t *aggr_table_entry::make_superaggr_se(vector<scalarexp_t *> se_refs){
	scalarexp_t *se_l, *se_r, *ret_se = NULL;

	if(op == "COUNT"){
		ret_se = scalarexp_t::make_se_aggr("SUM", se_refs[0]);
		return(ret_se);
	}
	if(op == "SUM"){
		ret_se = scalarexp_t::make_se_aggr("SUM", se_refs[0]);
		return(ret_se);
	}
	if(op == "AVG"){
		se_l = scalarexp_t::make_se_aggr("SUM", se_refs[0]);
		se_r = scalarexp_t::make_se_aggr("SUM", se_refs[1]);

		ret_se = new scalarexp_t("/", se_l, se_r);
		return(ret_se);
	}
	if(op == "MIN"){
		ret_se = scalarexp_t::make_se_aggr("MIN", se_refs[0]);
		return(ret_se);
	}
	if(op == "MAX"){
		ret_se = scalarexp_t::make_se_aggr("MAX", se_refs[0]);
		return(ret_se);
	}
	if(op == "AND_AGGR"){
		ret_se = scalarexp_t::make_se_aggr("AND_AGGR", se_refs[0]);
		return(ret_se);
	}
	if(op == "OR_AGGR"){
		ret_se = scalarexp_t::make_se_aggr("OR_AGGR", se_refs[0]);
		return(ret_se);
	}
	if(op == "XOR_AGGR"){
		ret_se = scalarexp_t::make_se_aggr("XOR_AGGR", se_refs[0]);
		return(ret_se);
	}

	return(ret_se);

}


//		Add a built-in aggr.
int aggregate_table::add_aggr(string op, scalarexp_t *se, bool is_super){
	int i;

	for(i=0;i<agr_tbl.size();i++){
		if(agr_tbl[i]->is_builtin() && op == agr_tbl[i]->op
		  && is_equivalent_se(se,agr_tbl[i]->operand) ){
//		  && is_super == agr_tbl[i]->is_superaggr())
			if(is_super) agr_tbl[i]->set_super(true);
			return(i);
		}
	}

	aggr_table_entry *ate = new aggr_table_entry(op, se, is_super);
	agr_tbl.push_back(ate);
	return(agr_tbl.size() - 1);
}

//		add a UDAF
int aggregate_table::add_aggr(string op, int fcn_id, vector<scalarexp_t *> opl, data_type *sdt, bool is_super, bool is_running, bool has_lfta_bailout){
	int i,o;

	for(i=0;i<agr_tbl.size();i++){
		if((! agr_tbl[i]->is_builtin()) && fcn_id == agr_tbl[i]->fcn_id
				&& opl.size() == agr_tbl[i]->oplist.size() ){
//				&& is_super == agr_tbl[i]->is_superaggr() ){
			for(o=0;o<opl.size();++o){
				if(! is_equivalent_se(opl[o],agr_tbl[i]->oplist[o]) )
					break;
			}
			if(o == opl.size()){
				if(is_super) agr_tbl[i]->set_super(true);
				return i;
			}
		}
	}

	aggr_table_entry *ate = new aggr_table_entry(op, fcn_id, opl, sdt,is_super,is_running, has_lfta_bailout);
	agr_tbl.push_back(ate);
	return(agr_tbl.size() - 1);
}


int cplx_lit_table::add_cpx_lit(literal_t *l, bool is_handle_ref){
	int i;

	for(i=0;i<cplx_lit_tbl.size();i++){
		if(l->is_equivalent(cplx_lit_tbl[i])){
			hdl_ref_tbl[i] = hdl_ref_tbl[i] | is_handle_ref;
			return(i);
		}
	}

	cplx_lit_tbl.push_back(l);
	hdl_ref_tbl.push_back(is_handle_ref);
	return(cplx_lit_tbl.size() - 1);
}



//------------------------------------------------------------
//		parse_fta code


gb_t *gb_t::duplicate(){
	gb_t *ret = new gb_t(interface.c_str(), table.c_str(), name.c_str());
	ret->type = type;
	ret->lineno = lineno;
	ret->charno = charno;
	if(def != NULL)
		ret->def = dup_se(def,NULL);
	return ret;
}
