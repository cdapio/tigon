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

#ifndef __ANALYZE_FTA_H_DEFINED__
#define __ANALYZE_FTA_H_DEFINED__



#include "parse_fta.h"
#include "parse_schema.h"
#include"type_objects.h"
#include "parse_ext_fcns.h"
#include "iface_q.h"


#include<set>
#include<algorithm>
using namespace std;


//		Represent a component of a predicate,
//		these components are ANDed together.
//		Often they are simple enough to analyze.
class cnf_elem{
public:
	int is_atom;	// i.e., is an atomic predicate
	int eq_pred;	// And, it is a test for equality
	int in_pred;	// Or, it is a an IN predicate

	int pr_gb;		// the predicate refs a gb column.
	int pr_attr;	// the predicate refs a table column.
	int pr_aggr;	// the predicate refs an aggregate

	int l_simple;	// the lhs is simple (just a colref)
	int l_gb;		// the lhs refs a gb var
	int l_attr;		// the lhs refs a table column
	int l_aggr;		// the lhs refs an aggregate.

	int r_simple;	// the rhs is simple (just a colref)
	int r_gb;		// the rhs refs a gb var
	int r_attr;		// the rhs refs a table column
	int r_aggr;		// the rhs refs an aggregate.

	int cost;		// an estiamte of the evaluation cost.

   predicate_t *pr;

	cnf_elem(predicate_t *p){
		is_atom = 0;
		eq_pred = 0; in_pred = 0;
		pr_gb = pr_attr = pr_aggr = 0;
		l_simple = l_gb = l_attr = l_aggr = 0;
		r_simple = r_gb = r_attr = r_aggr = 0;
		pr = p;
	};

	cnf_elem(){ is_atom = 0;
		eq_pred = 0; in_pred = 0;
		pr_gb = pr_attr = pr_aggr = 0;
		l_simple = l_gb = l_attr = l_aggr = 0;
		r_simple = r_gb = r_attr = r_aggr = 0;
		pr = NULL;
	};



	void swap_scalar_operands(){
	int tmp;
	if(in_pred) return;	// can't swap IN predicates, se list always on right.
	if(! is_atom) return;	// can only swap scalar expressions.

	tmp = l_simple; l_simple = r_simple; r_simple = tmp;
	tmp = l_gb; l_gb = r_gb; r_gb = tmp;
	tmp = l_attr; l_attr = r_attr; r_attr = tmp;
	tmp = l_aggr; l_aggr = r_aggr; r_aggr = tmp;

	pr->swap_scalar_operands();
  }
};


//		A GB variable is identified by its name and
//		its source table variable -- so that joins can be expressed.
//		GB Vars defined AS a scalar expression have a null tableref.

struct col_id{
  std::string field;
  int tblvar_ref;
//		Also carry the schema ref -- used to get data type info
//		when defining variables.
  int schema_ref;

  col_id(){tblvar_ref = -1;};
  col_id(colref_t *cr){
	  field = cr->field;
	  tblvar_ref = cr-> get_tablevar_ref();
	  schema_ref = cr->get_schema_ref();
  };



  void load_from_colref(colref_t *cr){
	  field = cr->field;
	  tblvar_ref = cr-> get_tablevar_ref();
	  schema_ref = cr->get_schema_ref();
  };
};

struct lt_col_id{
  bool operator()(const col_id &cr1, const col_id &cr2) const{
	if(cr1.tblvar_ref < cr2.tblvar_ref) return(1);
	if(cr1.tblvar_ref == cr2.tblvar_ref)
 	   return (cr1.field < cr2.field);
 	return(0);
  }
};

  bool operator<(const col_id &cr1, const col_id &cr2);


//		Every GB variable is represented by its name
//		(defined above) and the scalar expression which
//		defines its value.  If the GB var is devlared as a
//		colref, the add_gb_attr method of gb_table will
//		create an SE consisting of the colref.
//			NOTE : using col_id is dangerous when the
//					agregation operator is defiend over a self-join

#define GBVAR_COLREF 1
#define GBVAR_SE 2

struct gb_table_entry{
	col_id name;		// tblref in col_id refers to the tablevar.
	scalarexp_t *definition;
	int ref_type;

	gb_table_entry(){
		definition = NULL;
	};
	data_type *get_data_type(){
		return(definition->get_data_type());
	};
	void reset_temporal(){
		if(definition) definition->reset_temporal();
	}
};


//		The GB table is the symbol table containing
//		the GB vars.  This object puts a wrapper around
//		access to the table.

class gb_table{
private:
	std::vector<gb_table_entry *> gtbl;

public:
	std::vector<vector<bool> > gb_patterns;
//		rollup, cube, and grouping_sets cannot be readily reconstructed by
//		analyzing the patterns, so explicitly record them here.
//		used only so that sgah_qpn::to_query_string produces 
//		something meaningful.
	std::vector<std::string> gb_entry_type;
	std::vector<int> gb_entry_count;
	std::vector<std::vector<std::vector<bool> > > pattern_components;

	gb_table(){};

	int add_gb_attr(gb_t *c, tablevar_list_t *fm, table_list *schema,
					table_exp_t *fta_tree, ext_fcn_list *Ext_fcns);

	void add_gb_var(std::string n, int t, scalarexp_t *d, int rt){
		gb_table_entry *entry = new gb_table_entry();
		entry->name.field = n;
		entry->name.tblvar_ref = t;
		entry->definition = d;
		entry->ref_type = rt;
		gtbl.push_back(entry);
	};
	data_type *get_data_type(int g){return(gtbl[g]->get_data_type());	};
	int find_gb(colref_t *c, tablevar_list_t *fm, table_list *schema);
	scalarexp_t *get_def(int i){return gtbl[i]->definition;	};
	std::string get_name(int i){return gtbl[i]->name.field;	};
	int get_tblvar_ref(int i){return gtbl[i]->name.tblvar_ref;	};
	int get_reftype(int i){return gtbl[i]->ref_type;	};
	int size(){return(gtbl.size());	};
	void set_pattern_info(gb_table *g){
		gb_patterns = g->gb_patterns;
		gb_entry_type = g->gb_entry_type;
		gb_entry_count = g->gb_entry_count;
		pattern_components = g->pattern_components;
	}
	void reset_temporal(int i){
		gtbl[i]->reset_temporal();
	}
};



//			Represent an aggregate to be calculated.
//			The unique identifier is the aggregate fcn, and the
//			expression aggregated over.
//			TODO: unify udaf with built-in better
//				(but then again, the core dumps help with debugging)

struct aggr_table_entry{
	std::string op;
	scalarexp_t *operand;
	std::vector<scalarexp_t *> oplist;
	int fcn_id;		// -1 for built-in aggrs.
	data_type *sdt;	// storage data type.
	data_type *rdt; // return value data type.
	bool is_superag;
	bool is_running;
	bool bailout;

	aggr_table_entry(){ sdt = NULL; rdt = NULL; is_superag = false;};
	aggr_table_entry(std::string o, scalarexp_t *se, bool s){
		op = o; operand = se; fcn_id = -1; sdt = NULL;
		if(se){
			rdt = new data_type();
			rdt->set_aggr_data_type(o,se->get_data_type());
		}else{
			rdt = new data_type("Uint");
		}
		is_superag = s;
		is_running = false;
		bailout = false;
	};
	aggr_table_entry(std::string o, int f, std::vector<scalarexp_t *> s, data_type *st, bool spr, bool r, bool b_o){
		op = o;
		fcn_id = f;
		operand = NULL;
		oplist = s;
		sdt = st;
		rdt = NULL;
		is_superag = spr;
		is_running = r;
		bailout = b_o;
	}

	bool fta_legal(ext_fcn_list *Ext_fcns);
    bool is_star_aggr(){return operand==NULL;};
	bool is_builtin(){return fcn_id < 0;};
	int get_fcn_id(){return fcn_id;};
	bool is_superaggr(){return is_superag;};
	bool is_running_aggr(){return is_running;};
	bool has_bailout(){return bailout;};
    void set_super(bool s){is_superag = s;};

	std::vector<std::string> get_subaggr_fcns(std::vector<bool> &use_se);
	std::vector<data_type *> get_subaggr_dt();
	scalarexp_t *make_superaggr_se(std::vector<scalarexp_t *> se_refs);
	data_type *get_storage_type(){return sdt;};
	std::vector<scalarexp_t *> get_operand_list(){return oplist;};

	aggr_table_entry *duplicate(){
		aggr_table_entry *dup = new aggr_table_entry();
		dup->op = op;
		dup->operand = operand;
		dup->oplist = oplist;
		dup->fcn_id = fcn_id;
		dup->sdt = sdt;
		dup->rdt = rdt;
		dup->is_superag = is_superag;
		dup->is_running = is_running;
		dup->bailout = bailout;
		return dup;
	}

	static bool superaggr_allowed(std::string op, ext_fcn_list *Ext_fcns){
		if(op == "SUM" || op == "COUNT" || op == "MIN" || op=="MAX")
			return true;
		return false;
	}

	bool superaggr_allowed(){
		if(is_builtin())
			return aggr_table_entry::superaggr_allowed(op,NULL);
//		Currently, no UDAFS allowed as superaggregates
		return false;
	}

	static bool multiple_return_allowed(bool is_built_in, ext_fcn_list *Ext_fcns, int fcn_id){
		if(is_built_in)
			return true;
//			Currently, no UDAFS allowed as stateful fcn params.
//			in cleaning_when, cleaning_by clauses.
		if(Ext_fcns->is_running_aggr(fcn_id) || Ext_fcns->multiple_returns(fcn_id))
			return true;
		return false;
	}

};


class aggregate_table{
public:
	std::vector<aggr_table_entry *> agr_tbl;

	aggregate_table(){};

	int add_aggr(std::string op, scalarexp_t *se, bool is_spr);
	int add_aggr(std::string op, int fcn_id, std::vector<scalarexp_t *> opl, data_type *sdt, bool is_spr, bool is_running, bool has_lfta_bailout);
	int add_aggr(aggr_table_entry *a){agr_tbl.push_back(a); return agr_tbl.size(); };
	int size(){return(agr_tbl.size());	};
	scalarexp_t *get_aggr_se(int i){return agr_tbl[i]->operand;	};

	data_type *get_data_type(int i){
//		if(agr_tbl[i]->op == "COUNT")
//			 return new data_type("uint");
//		else
			if(! agr_tbl[i]->rdt){
				fprintf(stderr,"INTERNAL ERROR in aggregate_table::get_data_type : rdt is NULL.\n");
				exit(1);
			}
			return agr_tbl[i]->rdt;
	};

	std::string get_op(int i){return agr_tbl[i]->op;};

	bool fta_legal(int i,ext_fcn_list *Ext_fcns){return agr_tbl[i]->fta_legal(Ext_fcns);	};
    bool is_star_aggr(int i){return agr_tbl[i]->is_star_aggr(); };
	bool is_builtin(int i){return agr_tbl[i]->is_builtin(); };
	int get_fcn_id(int i){return agr_tbl[i]->get_fcn_id();};
	bool is_superaggr(int i){return agr_tbl[i]->is_superaggr();};
	bool is_running_aggr(int i){return agr_tbl[i]->is_running_aggr();};
	bool has_bailout(int i){return agr_tbl[i]->has_bailout();};
//	bool multiple_return_allowed(int i){
//		return agr_tbl[i]->multiple_return_allowed(agr_tbl[i]->is_builtin(),NULL);
//	};
	bool superaggr_allowed(int i){return agr_tbl[i]->superaggr_allowed();};
	std::vector<scalarexp_t *> get_operand_list(int i){
		return agr_tbl[i]->get_operand_list();
	}

	std::vector<std::string> get_subaggr_fcns(int i, std::vector<bool> &use_se){
		return(agr_tbl[i]->get_subaggr_fcns(use_se) );
	};

	std::vector<data_type *> get_subaggr_dt(int i){
		return(agr_tbl[i]->get_subaggr_dt() );
	}
	data_type *get_storage_type(int i){
		return( agr_tbl[i]->get_storage_type());
	}

	aggr_table_entry *duplicate(int i){
		return agr_tbl[i]->duplicate();
	};

	scalarexp_t *make_superaggr_se(int i, std::vector<scalarexp_t *> se_refs){
		return(agr_tbl[i]->make_superaggr_se(se_refs) );
	};
};


class cplx_lit_table{
	std::vector<literal_t *> cplx_lit_tbl;
	std::vector<bool> hdl_ref_tbl;

public:
	cplx_lit_table(){};

	int add_cpx_lit(literal_t *, bool);
	int size(){return cplx_lit_tbl.size();};
	literal_t *get_literal(int i){return cplx_lit_tbl[i];};
	bool is_handle_ref(int i){return hdl_ref_tbl[i];};

};

enum param_handle_type { cplx_lit_e, litval_e, param_e};

class handle_param_tbl_entry{
private:

public:
	std::string fcn_name;
	int param_slot;
	literal_t *litval;
	int complex_literal_idx;
	std::string param_name;
	param_handle_type val_type;
    std::string type_name;

	handle_param_tbl_entry(std::string fname, int slot,
							literal_t *l, std::string tnm){
		fcn_name = fname; param_slot = slot;
		val_type = litval_e;
		litval = l;
		type_name = tnm;
	};
	handle_param_tbl_entry(std::string fname, int slot, std::string p, std::string tnm){
		fcn_name = fname; param_slot = slot;
		val_type = param_e;
		param_name = p;
		type_name = tnm;
	};
	handle_param_tbl_entry(std::string fname, int slot, int ci, std::string tnm){
		fcn_name = fname; param_slot = slot;
		val_type = cplx_lit_e;
		complex_literal_idx = ci;
		type_name = tnm;
	};

	std::string lfta_registration_fcn(){
		char tmps[500];
		sprintf(tmps,"register_handle_for_%s_slot_%d",fcn_name.c_str(),param_slot);
		return(tmps);
	};
	std::string hfta_registration_fcn(){return lfta_registration_fcn();};

	std::string lfta_deregistration_fcn(){
		char tmps[500];
		sprintf(tmps,"deregister_handle_for_%s_slot_%d",fcn_name.c_str(),param_slot);
		return(tmps);
	};
	std::string hfta_rdeegistration_fcn(){return lfta_registration_fcn();};

};


class param_table{
//			Store the data in vectors to preserve
//			listing order.
	std::vector<std::string> pname;
	std::vector<data_type *> pdtype;
	std::vector<bool> phandle;

	int find_name(std::string n){
		int p;
		for(p=0;p<pname.size();++p){
			if(pname[p] == n) break;
		}
		if(p == pname.size()) return(-1);
		return(p);
	};

public:
	param_table(){};

//			Add the param with the given name and associated
//			data type and handle use.
//			if its a new name, return true.
//			Else, take OR of use_handle, return false.
//			(builds param table and collects handle references).
	bool add_param(std::string pnm, data_type *pdt, bool use_handle){
		int p = find_name(pnm);
		if(p == -1){
			pname.push_back(pnm);
			pdtype.push_back(pdt);
			phandle.push_back(use_handle);
			return(true);
        }else{
			if(use_handle) phandle[p] = use_handle;
			return(false);
		}

	};

	int size(){return pname.size();	};

	std::vector<std::string> get_param_names(){
		return(pname);
	};

	data_type *get_data_type(std::string pnm){
		int p = find_name(pnm);
		if(p >= 0){
			return(pdtype[p]);
		}
		return(new data_type("undefined_type"));
	};

	bool handle_access(std::string pnm){
		int p = find_name(pnm);
		if(p >= 0){
			return(phandle[p]);
		}
		return(false);
	};

};


//			Two columns are the same if they come from the
//			same source, and have the same field name.
//			(use to determine which fields must be unpacked).
//			NOTE : dangerous in the presence of self-joins.
typedef std::set<col_id, lt_col_id> col_id_set;

bool contains_gb_pr(predicate_t *pr, std::set<int> &gref_set);
bool contains_gb_se(scalarexp_t *se, std::set<int> &gref_set);


void gather_se_col_ids(scalarexp_t *se, col_id_set &cid_set,  gb_table *gtbl);
void gather_pr_col_ids(predicate_t *pr, col_id_set &cid_set, gb_table *gtbl);

void gather_se_opcmp_fcns(scalarexp_t *se, std::set<std::string> &fcn_set);
void gather_pr_opcmp_fcns(predicate_t *pr, std::set<std::string> &fcn_set);

////////////////////////////////////////////
//		Structures to record usage of operator views.

class opview_entry{
public:
	std::string parent_qname;
	std::string root_name;
	std::string view_name;
	std::string exec_fl;		// name of executable file (if any)
	std::string udop_alias;		// unique ID of the UDOP
	std::string mangler;
	int liveness_timeout;		// maximum delay between hearbeats from udop
	int pos;
	std::vector<std::string> subq_names;

	opview_entry(){mangler="";};
};

class opview_set{
public:
	std::vector<opview_entry *> opview_list;

	int append(opview_entry *opv){opview_list.push_back(opv);
								  return opview_list.size()-1;};
	int size(){return opview_list.size();};
	opview_entry *get_entry(int i){ return opview_list[i];};
};

/////////////////////////////////////////////////////////////////
//		Wrap up all of the analysis of the FTA into this package.
//
//		There is some duplication between query_summary_class,
//		but I'd rather avoid pushing analysis structures
//		into the parse modules.
//
//		TODO: revisit the issue when nested subqueries are implemented.
//		One possibility: implement accessor methods to hide the
//		complexity
//		For now: this class contains data structures not in table_exp_t
//		(with a bit of duplication)

struct query_summary_class{
	std::string query_name;			// the name of the query, becomes the name
									// of the output.
	int query_type;		// e.g. SELECT, MERGE, etc.

	gb_table *gb_tbl;			// Table of all group-by attributes.
	std::set<int> sg_tbl;		// Names of the superGB attributes
	aggregate_table *aggr_tbl;	// Table of all referenced aggregates.
	std::set<std::string> states_refd;	// states ref'd by stateful fcns.


//		copied from parse tree, then into query node.
//		interpret the string representation of data type into a type_object
	param_table *param_tbl;		// Table of all query parameters.

//		There is stuff that is not copied over by analyze_fta,
//		it still resides in fta_tree.
    table_exp_t *fta_tree;		// The (decorated) parse tree.


//		This is copied from the parse tree (except for "query_name")
//		then copied to the query node in query_plan.cc:145
	std::map<std::string, std::string> definitions;	// additional definitions.


//		CNF representation of the where and having predicates.
	std::vector<cnf_elem *> wh_cnf;
	std::vector<cnf_elem *> hav_cnf;
	std::vector<cnf_elem *> cb_cnf;
	std::vector<cnf_elem *> cw_cnf;
	std::vector<cnf_elem *> closew_cnf;




//		For MERGE type queries.
	std::vector<colref_t *> mvars;
	scalarexp_t *slack;

////////			Constructors
	query_summary_class(){
		init();
	}

	query_summary_class(table_exp_t *f){
		fta_tree = f;
		init();
	}

/////			Methods

	bool add_param(std::string pnm, data_type *pdt, bool use_handle){
		return( param_tbl->add_param(pnm,pdt,use_handle));
	};


private:
	void init(){
//		Create the gb_tbl, aggr_tbl, and param_tbl objects
		gb_tbl = new gb_table();
		aggr_tbl = new aggregate_table();
		param_tbl = new param_table();

	}
};

int verify_colref(scalarexp_t *se, tablevar_list_t *fm,
					table_list *schema, gb_table *gtbl);
std::string impute_query_name(table_exp_t *fta_tree, std::string default_nm);

query_summary_class *analyze_fta(table_exp_t *fta_tree, table_list *schema, ext_fcn_list *Ext_fcns, std::string qname);
bool is_equivalent_se(scalarexp_t *se1, scalarexp_t *se2);
bool is_equivalent_se_base(scalarexp_t *se1, scalarexp_t *se2, table_list *Schema);
bool is_equivalent_pred_base(predicate_t *p1, predicate_t *p2, table_list *Schema);

bool is_equivalent_class_pred_base(predicate_t *p1, predicate_t *p2, table_list *Schema,ext_fcn_list *Ext_fcns);
void analyze_cnf(cnf_elem *c);

void find_partial_fcns(scalarexp_t *se, std::vector<scalarexp_t *> *pf_list, std::vector<int> *fcn_ref_cnt, std::vector<bool> *is_partial_fcn, ext_fcn_list *Ext_fcns);
void find_partial_fcns_pr(predicate_t *pr, std::vector<scalarexp_t *> *pf_list,std::vector<int> *fcn_ref_cnt, std::vector<bool> *is_partial_fcn, ext_fcn_list *Ext_fcns);
void collect_partial_fcns(scalarexp_t *se, std::set<int> &pfcn_refs);
void collect_partial_fcns_pr(predicate_t *pr,  std::set<int> &pfcn_refs);

void find_combinable_preds(predicate_t *pr,  vector<predicate_t *> *pr_list,
								table_list *Schema, ext_fcn_list *Ext_fcns);

void collect_agg_refs(scalarexp_t *se, std::set<int> &agg_refs);
void collect_aggr_refs_pr(predicate_t *pr,  std::set<int> &agg_refs);

int bind_to_schema_pr(predicate_t *pr, tablevar_list_t *fm, table_list *schema);
int bind_to_schema_se(scalarexp_t *se, tablevar_list_t *fm, table_list *schema);

temporal_type compute_se_temporal(scalarexp_t *se, std::map<col_id, temporal_type> &tcol);


bool find_complex_literal_se(scalarexp_t *se, ext_fcn_list *Ext_fcns,
								cplx_lit_table *complex_literals);
void find_complex_literal_pr(predicate_t *pr, ext_fcn_list *Ext_fcns,
								cplx_lit_table *complex_literals);
void find_param_handles_se(scalarexp_t *se, ext_fcn_list *Ext_fcns,
						std::vector<handle_param_tbl_entry *> &handle_tbl);
void find_param_handles_pr(predicate_t *pr, ext_fcn_list *Ext_fcns,
						std::vector<handle_param_tbl_entry *> &handle_tbl);

//		Copy a scalar expression / predicate tree
scalarexp_t *dup_se(scalarexp_t *se, aggregate_table *aggr_tbl);
select_element *dup_select(select_element *sl, aggregate_table *aggr_tbl);
predicate_t *dup_pr(predicate_t *pr, aggregate_table *aggr_tbl);

//		Expand gbvars
void expand_gbvars_pr(predicate_t *pr, gb_table &gb_tbl);
scalarexp_t *expand_gbvars_se(scalarexp_t *se, gb_table &gb_tbl);


//		copy a query
table_exp_t *dup_table_exp(table_exp_t *te);


//		Tie colrefs to a new range var
void bind_colref_se(scalarexp_t *se,
				  std::vector<tablevar_t *> &fm,
				  int prev_ref, int new_ref
				 );
void bind_colref_pr(predicate_t *pr,
				  std::vector<tablevar_t *> &fm,
				  int prev_ref, int new_ref
				 );


std::string impute_colname(std::vector<select_element *> &sel_list, scalarexp_t *se);
std::string impute_colname(std::set<std::string> &curr_names, scalarexp_t *se);

bool is_literal_or_param_only(scalarexp_t *se);


void build_aggr_tbl_fm_pred(predicate_t *pr, aggregate_table *agr_tbl);
void build_aggr_tbl_fm_se(scalarexp_t *se, aggregate_table *agr_tbl);

void compute_cnf_cost(cnf_elem *cm, ext_fcn_list *Ext_fcns);
struct compare_cnf_cost{
  bool operator()(const cnf_elem *c1, const cnf_elem *c2) const{
	if(c1->cost < c2->cost) return(true);
 	return(false);
  }
};

void get_tablevar_ref_se(scalarexp_t *se, std::vector<int> &reflist);
void get_tablevar_ref_pr(predicate_t *pr, std::vector<int> &reflist);

long long int find_temporal_divisor(scalarexp_t *se, gb_table *gbt, std::string &fnm);


int count_se_ifp_refs(scalarexp_t *se, std::set<std::string> &ifpnames);
int count_pr_ifp_refs(predicate_t *pr, std::set<std::string> &ifpnames);
int resolve_se_ifp_refs(scalarexp_t *se, std::string ifm, std::string ifn, ifq_t *ifdb,  std::string &err);
int resolve_pr_ifp_refs(predicate_t *pr, std::string ifm, std::string ifn, ifq_t *ifdb,  std::string &err);

int pred_refs_sfun(predicate_t *pr);
int se_refs_sfun(scalarexp_t *se);


//		Represent preds for the prefilter, and
//		the LFTAs which reference them.
struct cnf_set{
	predicate_t *pr;
	std::set<int> lfta_id;
	std::set<unsigned int> pred_id;

	cnf_set(){};
	cnf_set(predicate_t *p, unsigned int l, unsigned int i){
		pr = dup_pr(p,NULL);		// NEED TO UPDATE dup_se TO PROPAGATE
		lfta_id.insert(l);
		pred_id.insert((l << 16)+i);
	}
	void subsume(cnf_set *c){
		std::set<int>::iterator ssi;
		for(ssi=c->lfta_id.begin();ssi!=c->lfta_id.end();++ssi){
			lfta_id.insert((*ssi));
		}
		std::set<unsigned int >::iterator spi;
		for(spi=c->pred_id.begin();spi!=c->pred_id.end();++spi){
			pred_id.insert((*spi));
		}
	}
	void combine_pred(cnf_set *c){
		pr = new predicate_t("AND",pr,c->pr);
		std::set<unsigned int >::iterator spi;
		for(spi=c->pred_id.begin();spi!=c->pred_id.end();++spi){
			pred_id.insert((*spi));
		}
	}

	void add_pred_ids(set<unsigned int> &pred_set){
		std::set<unsigned int >::iterator spi;
		for(spi=pred_id.begin();spi!=pred_id.end();++spi){
			pred_set.insert((*spi));
		}
	}

	static unsigned int make_lfta_key(unsigned int l){
		return l << 16;
	}


	~cnf_set(){};
};
struct compare_cnf_set{
  bool operator()(const cnf_set *c1, const cnf_set *c2) const{
	if(c1->lfta_id.size() > c2->lfta_id.size()) return(true);
 	return(false);
  }
};

bool operator<(const cnf_set &c1, const cnf_set &c2);


void find_common_filter(std::vector< std::vector<cnf_elem *> > &where_list, table_list *Schema, ext_fcn_list *Ext_fcns, std::vector<cnf_set *> &prefilter_preds, std::set<unsigned int> &pred_ids);


cnf_elem *find_common_filter(std::vector< std::vector<cnf_elem *> > &where_list, table_list *Schema);

std::string int_to_string(int i);
void insert_gb_def_pr(predicate_t *pr, gb_table *gtbl);
void insert_gb_def_se(scalarexp_t *se, gb_table *gtbl);




#endif

