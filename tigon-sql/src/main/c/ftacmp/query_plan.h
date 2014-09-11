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
#ifndef __QUERY_PLAN_H__
#define __QUERY_PLAN_H__

#include<vector>
#include<string>
#include<map>
using namespace std;

#include"analyze_fta.h"
#include"iface_q.h"
#include"parse_partn.h"
#include"generate_utils.h"

//		Identify the format of the input, output streams.
#define UNKNOWNFORMAT 0
#define NETFORMAT 1
#define HOSTFORMAT 2

///////////////////////////////////////////////////
//	representation of an output operator specification

struct ospec_str{
	string query;
	string operator_type;
	string operator_param;
	string output_directory;
	int bucketwidth;
	string partitioning_flds;
	int n_partitions;
};


////////////////////////////////////////////////////
//	Input representation of a query

struct query_node{
	int idx;
	std::set<int> reads_from;
	std::set<int> sources_to;
	std::vector<std::string> refd_tbls;
	std::vector<var_pair_t *> params;
	std::string name;
	std::string file;
	std::string mangler;		// for UDOPs
	bool touched;
	table_exp_t *parse_tree;
	int n_consumers;
	bool is_udop;
	bool is_externally_visible;
	bool inferred_visible_node;

	set<int> subtree_roots;

	query_node(){
		idx = -1;
		touched = false;
		parse_tree = NULL;
		n_consumers = 0;
		is_externally_visible = false;
		inferred_visible_node = false;
		mangler="";
	};
	query_node(int i, std::string qnm, std::string flnm, table_exp_t *pt){
		idx = i;
		touched = false;
		name = qnm;
		file = flnm;
		parse_tree = pt;
		n_consumers = 0;
		is_udop = false;
		is_externally_visible = pt->get_visible();
		inferred_visible_node = false;
		mangler="";

		tablevar_list_t *fm = parse_tree->get_from();
		refd_tbls =  fm->get_table_names();

		params  = pt->plist;
	};
	query_node(int ix, std::string udop_name,table_list *Schema){
		idx = ix;
		touched = false;
		name = udop_name;
		file = udop_name;
		parse_tree = NULL;
		n_consumers = 0;
		is_udop = true;
		is_externally_visible = true;
		inferred_visible_node = false;
		mangler="";

		int sid = Schema->find_tbl(udop_name);
		std::vector<subquery_spec *> subq = Schema->get_subqueryspecs(sid);
		int i;
		for(i=0;i<subq.size();++i){
			refd_tbls.push_back(subq[i]->name);
		}
	};
};

struct hfta_node{
	std::string name;
	std::string source_name;
	std::vector<int> query_node_indices;
	std::set<int> reads_from;
	std::set<int> sources_to;
	bool is_udop;
	bool inferred_visible_node;
	int n_parallel;
	int parallel_idx;
	bool do_generation;	// false means, ignore it.

	hfta_node(){
		is_udop = false;
		inferred_visible_node = false;
		n_parallel = 1;
		parallel_idx = 0;
		do_generation = true;
	}
};






#define SPX_QUERY 1
#define SGAH_QUERY 2

// the following selectivity estimates are used by our primitive rate estimators
#define SPX_SELECTIVITY 1.0
#define SGAH_SELECTIVITY 0.1
#define RSGAH_SELECTIVITY 0.1
#define SGAHCWCB_SELECTIVITY 0.1
#define MRG_SELECTIVITY 1.0
#define JOIN_EQ_HASH_SELECTIVITY 1.0

// the the output rate of the interface is not given we are going to use
// this default value
#define DEFAULT_INTERFACE_RATE 100


//			Define query plan nodes
//			These nodes are intended for query modeling
//			and transformation rather than for code generation.


//			Query plan node base class.
//			It has an ID, can return its type,
//			and can be linked into lists with the predecessors
//			and successors.
//			To add : serialize, unserialize?

class qp_node{
public:
  int id;
  std::vector<int> predecessors;
  std::vector<int> successors;
  std::string node_name;

//		For error reporting without exiting the program.
  int error_code;
  std::string err_str;

//			These should be moved to the containing stream_query object.
  std::map<std::string, std::string> definitions;
  param_table *param_tbl;

//		The value of a field in terms of protocol fields (if any).
  std::map<std::string, scalarexp_t *> protocol_map;

  qp_node(){
	error_code = 0;
  	id = -1;
  	param_tbl = new param_table();
  };
  qp_node(int i){
	error_code = 0;
  	id = i;
  	param_tbl = new param_table();
  };

  int get_id(){return(id);};
  void set_id(int i){id = i;	};

  int get_error_code(){return error_code;};
  std::string get_error_str(){return err_str;};

  virtual std::string node_type() = 0;

//		For code generation, does the operator xform its input.
  virtual bool makes_transform() = 0;

//		For linking, what external libraries does the operator depend on?
  virtual std::vector<std::string> external_libs() = 0;

  void set_node_name(std::string n){node_name = n;};
  std::string get_node_name(){return node_name;};

  void set_definitions(std::map<std::string, std::string> &def){
	  definitions = def;
  };
  std::map<std::string, std::string> get_definitions(){return definitions;};


//		call to create the mapping from field name to se in protocol fields.
//		Pass in qp_node of data sources, in order.
  virtual void create_protocol_se(std::vector<qp_node *> q_sources,table_list *Schema)=0;
//		get the protocol map.  the parameter is the return value.
  std::map<std::string, scalarexp_t *> *get_protocol_se(){return &protocol_map;}

//		Each qp node must be able to return a description
//		of the tuples it creates.
//		TODO: the get_output_tls method should subsume the get_fields
//			method, but in fact it really just returns the
//			operator name.
  virtual table_def *get_fields() = 0;	// Should be vector?
//		Get the from clause
  virtual std::vector<tablevar_t *> get_input_tbls() = 0;
//		this is a confused function, it acutally return the output
//		table name.
  virtual std::vector<tablevar_t *> get_output_tbls() = 0;

  std::string get_val_of_def(std::string def){
  	if(definitions.count(def) > 0) return definitions[def];
  	return("");
  };
  void set_definition(std::string def, std::string val){
	definitions[def]=val;
  }

//		Associate colrefs in SEs with tables
//		at code generation time.
  virtual void bind_to_schema(table_list *Schema) = 0;

//		Get colrefs of the operator, currently only meaningful for lfta
//		operators, and only interested in colrefs with extraction fcns
  virtual col_id_set get_colrefs(bool ext_fcns_only,table_list *Schema)=0;

  virtual std::string to_query_string() = 0;
  virtual std::string generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, std::vector<bool> &needs_xform) = 0;
  virtual std::string generate_functor_name() = 0;

  virtual std::string generate_operator(int i, std::string params) = 0;
  virtual std::string get_include_file() = 0;

  virtual cplx_lit_table *get_cplx_lit_tbl(ext_fcn_list *Ext_fcns) = 0;
  virtual std::vector<handle_param_tbl_entry *> get_handle_param_tbl(ext_fcn_list *Ext_fcns) = 0;

//		Split this node into LFTA and HFTA nodes.
//		Four possible outcomes:
//		1) the qp_node reads from a protocol, but does not need to
//			split (can be evaluated as an LFTA).
//			The lfta node is the only element in the return vector,
//			and hfta_returned is false.
//		2) the qp_node reads from no protocol, and therefore cannot be split.
//			THe hfta node is the only element in the return vector,
//			and hfta_returned is true.
//		3) reads from at least one protocol, but cannot be split : failure.
//			return vector is empty, the error conditions are written
//			in the qp_node.
//		4) The qp_node splits into an hfta node and one or more LFTA nodes.
//			the return vector has two or more elements, and hfta_returned
//			is true.  The last element is the HFTA.
	virtual std::vector<qp_node *> split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx) = 0;


//		Ensure that any refs to interface params have been split away.
	virtual int count_ifp_refs(std::set<std::string> &ifpnames)=0;



//		Tag the data sources which are views,
//		return the (optimized) source queries and
//		record the view access in opview_set
	virtual std::vector<table_exp_t *> extract_opview(table_list *Schema, std::vector<query_node *> &qnodes, opview_set &opviews, std::string rootnm, std::string silo_nm) = 0;

  param_table *get_param_tbl(){return param_tbl;};

//			The "where" clause is a pre-filter
  virtual  std::vector<cnf_elem *> get_where_clause() = 0;
//			To be more explicit, use get_filter_preds
  virtual  std::vector<cnf_elem *> get_filter_clause() = 0;

  void add_predecessor(int i){predecessors.push_back(i);};
  void remove_predecessor(int i){
	std::vector<int>::iterator vi;
	for(vi=predecessors.begin(); vi!=predecessors.end();++vi){
		if((*vi) == i){
			predecessors.erase(vi);
			return;
		}
	}
  };
  void add_successor(int i){successors.push_back(i);};
  std::vector<int> get_predecessors(){return predecessors;};
  int n_predecessors(){return predecessors.size();};
  std::vector<int> get_successors(){return successors;};
  int n_successors(){return successors.size();};
  void clear_predecessors(){predecessors.clear();};
  void clear_successors(){successors.clear();};

  // the following method is used for distributed query optimization
  double get_rate_estimate();


  // used for cloning query nodes
  virtual qp_node* make_copy(std::string suffix) = 0;
};



//		Select, project, transform (xform) query plan node.
//		represent the following query fragment
//			select scalar_expression_1, ..., scalar_expression_k
//			from S
//			where predicate
//
//		the predicates and the scalar expressions can reference
//		attributes of S and also functions.
class spx_qpn: public qp_node{
public:
	tablevar_t *table_name;					//	Source table
	std::vector<cnf_elem *> where;			// selection predicate
	std::vector<select_element *> select_list;	//	Select list



	std::string node_type(){return("spx_qpn");	};
    bool makes_transform(){return true;};
	std::vector<std::string> external_libs(){
		std::vector<std::string> ret;
		return ret;
	}

	void bind_to_schema(table_list *Schema);
	col_id_set get_colrefs(bool ext_fcns_only,table_list *Schema);

	std::string to_query_string();
  	std::string generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, std::vector<bool> &needs_xform);
  	std::string generate_functor_name();
	std::string generate_operator(int i, std::string params);
  	std::string get_include_file(){return("#include <selection_operator.h>\n");};

    std::vector<select_element *> get_select_list(){return select_list;};
    std::vector<scalarexp_t *> get_select_se_list(){
		std::vector<scalarexp_t *> ret;
		int i;
		for(i=0;i<select_list.size();++i) ret.push_back(select_list[i]->se);
		return ret;
	};
    std::vector<cnf_elem *> get_where_clause(){return where;};
    std::vector<cnf_elem *> get_filter_clause(){return where;};
	cplx_lit_table *get_cplx_lit_tbl(ext_fcn_list *Ext_fcns);
    std::vector<handle_param_tbl_entry *> get_handle_param_tbl(ext_fcn_list *Ext_fcns);

	table_def *get_fields();
	std::vector<tablevar_t *> get_input_tbls();
	std::vector<tablevar_t *> get_output_tbls();

	std::vector<qp_node *> split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx);
	virtual std::vector<table_exp_t *> extract_opview(table_list *Schema, std::vector<query_node *> &qnodes, opview_set &opviews, std::string rootnm, std::string silo_nm);
//		Ensure that any refs to interface params have been split away.
	int count_ifp_refs(std::set<std::string> &ifpnames);
	int resolve_if_params(ifq_t *ifdb, std::string &err);

	spx_qpn(){
	};
	spx_qpn(query_summary_class *qs,table_list *Schema){
//				Get the table name.
//				NOTE the colrefs have the table ref (an int)
//				embedded in them.  Would it make sense
//				to grab the whole table list?
		tablevar_list_t *fm = qs->fta_tree->get_from();
		std::vector<tablevar_t *> tbl_vec = fm->get_table_list();
		if(tbl_vec.size() != 1){
			char tmpstr[200];
			sprintf(tmpstr,"INTERNAL ERROR building SPX node: query defined over %lu tables.\n",tbl_vec.size() );
			err_str = tmpstr;
			error_code = 1;
		}
		table_name = (tbl_vec[0]);

//				Get the select list.
		select_list = qs->fta_tree->get_sl_vec();

//				Get the selection predicate.
		where = qs->wh_cnf;


//				Get the parameters
		param_tbl = qs->param_tbl;



	};

	// the following method is used for distributed query optimization
	double get_rate_estimate();


	qp_node* make_copy(std::string suffix){
		spx_qpn *ret = new spx_qpn();

		ret->param_tbl = new param_table();
		std::vector<std::string> param_names = param_tbl->get_param_names();
		int pi;
		for(pi=0;pi<param_names.size();pi++){
			data_type *dt = param_tbl->get_data_type(param_names[pi]);
			ret->param_tbl->add_param(param_names[pi],dt->duplicate(),
							param_tbl->handle_access(param_names[pi]));
		}
		ret->definitions = definitions;
		ret->node_name = node_name + suffix;

		// make shallow copy of all fields
		ret->where = where;
		ret->select_list = select_list;

		return ret;
	};
	void create_protocol_se(vector<qp_node *> q_sources, table_list *Schema);

};



//		Select, group-by, aggregate.
//		Representing
//			Select SE_1, ..., SE_k
//			From T
//			Where predicate
//			Group By gb1, ..., gb_n
//			Having predicate
//
//		NOTE : the samlping operator is sgahcwcb_qpn.
//
//		For now, must have group-by variables and aggregates.
//		The scalar expressions which are output must be a function
//		of the groub-by variables and the aggregates.
//		The group-by variables can be references to columsn of T,
//		or they can be scalar expressions.
class sgah_qpn: public qp_node{
public:
	tablevar_t *table_name;				// source table
	std::vector<cnf_elem *> where;		// selection predicate
	std::vector<cnf_elem *> having;		// post-aggregation predicate
	std::vector<select_element *> select_list;	// se's of output
	gb_table gb_tbl;			// Table of all group-by attributes.
	aggregate_table aggr_tbl;	// Table of all referenced aggregates.

	std::vector<scalarexp_t *> gb_sources;	// pre-compute for partitioning.

	int lfta_disorder;		// maximum disorder in the steam between lfta, hfta
	int hfta_disorder;		// maximum disorder in the  hfta

//		rollup, cube, and grouping_sets cannot be readily reconstructed by
//		analyzing the patterns, so explicitly record them here.
//		used only so that to_query_string produces something meaningful.
	std::vector<std::string> gb_entry_type;
	std::vector<int> gb_entry_count;

	std::vector<scalarexp_t *> get_gb_sources(){return gb_sources;}

	std::string node_type(){return("sgah_qpn");	};
    bool makes_transform(){return true;};
	std::vector<std::string> external_libs(){
		std::vector<std::string> ret;
		return ret;
	}

	void bind_to_schema(table_list *Schema);
	col_id_set get_colrefs(bool ext_fcns_only,table_list *Schema);

	std::string to_query_string();
  	std::string generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, std::vector<bool> &needs_xform);
  	std::string generate_functor_name();

	std::string generate_operator(int i, std::string params);
  	std::string get_include_file(){
			if(hfta_disorder <= 1){
				return("#include <groupby_operator.h>\n");
			}else{
				return("#include <groupby_operator_oop.h>\n");
			}
	};

    std::vector<select_element *> get_select_list(){return select_list;};
    std::vector<scalarexp_t *> get_select_se_list(){
		std::vector<scalarexp_t *> ret;
		int i;
		for(i=0;i<select_list.size();++i) ret.push_back(select_list[i]->se);
		return ret;
	};
    std::vector<cnf_elem *> get_where_clause(){return where;};
    std::vector<cnf_elem *> get_filter_clause(){return where;};
    std::vector<cnf_elem *> get_having_clause(){return having;};
    gb_table *get_gb_tbl(){return &gb_tbl;};
    aggregate_table *get_aggr_tbl(){return &aggr_tbl;};
	cplx_lit_table *get_cplx_lit_tbl(ext_fcn_list *Ext_fcns);
    std::vector<handle_param_tbl_entry *> get_handle_param_tbl(ext_fcn_list *Ext_fcns);

//				table which represents output tuple.
	table_def *get_fields();
	std::vector<tablevar_t *> get_input_tbls();
	std::vector<tablevar_t *> get_output_tbls();


	sgah_qpn(){
		lfta_disorder = 1;
		hfta_disorder = 1;
	};
	sgah_qpn(query_summary_class *qs,table_list *Schema){
		lfta_disorder = 1;
		hfta_disorder = 1;

//				Get the table name.
//				NOTE the colrefs have the tablevar ref (an int)
//				embedded in them.  Would it make sense
//				to grab the whole table list?
		tablevar_list_t *fm = qs->fta_tree->get_from();
		std::vector<tablevar_t *> tbl_vec = fm->get_table_list();
		if(tbl_vec.size() != 1){
			char tmpstr[200];
			sprintf(tmpstr,"INTERNAL ERROR building SGAH node: query defined over %lu tables.\n",tbl_vec.size() );
			err_str=tmpstr;
			error_code = 1;
		}
		table_name = (tbl_vec[0]);

//				Get the select list.
		select_list = qs->fta_tree->get_sl_vec();

//				Get the selection and having predicates.
		where = qs->wh_cnf;
		having = qs->hav_cnf;

//				Build a new GB var table (don't share, might need to modify)
		int g;
		for(g=0;g<qs->gb_tbl->size();g++){
			gb_tbl.add_gb_var(qs->gb_tbl->get_name(g),
				qs->gb_tbl->get_tblvar_ref(g), qs->gb_tbl->get_def(g),
				qs->gb_tbl->get_reftype(g)
			);
		}
		gb_tbl.set_pattern_info(qs->gb_tbl);
//		gb_tbl.gb_entry_type = qs->gb_tbl->gb_entry_type;
//		gb_tbl.gb_entry_count = qs->gb_tbl->gb_entry_count;
//		gb_tbl.pattern_components = qs->gb_tbl->pattern_components;

//				Build a new aggregate table. (don't share, might need
//				to modify).
		int a;
		for(a=0;a<qs->aggr_tbl->size();a++){
			aggr_tbl.add_aggr(
//				qs->aggr_tbl->get_op(a), qs->aggr_tbl->get_aggr_se(a)
				qs->aggr_tbl->duplicate(a)
			);
		}


//				Get the parameters
		param_tbl = qs->param_tbl;

	};



	std::vector<qp_node *> split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx);
	virtual std::vector<table_exp_t *> extract_opview(table_list *Schema, std::vector<query_node *> &qnodes, opview_set &opviews, std::string rootnm, std::string silo_nm);
//		Ensure that any refs to interface params have been split away.
	int count_ifp_refs(std::set<std::string> &ifpnames);
	int resolve_if_params(ifq_t *ifdb, std::string &err);

	// the following method is used for distributed query optimization
	double get_rate_estimate();


	qp_node* make_copy(std::string suffix){
		sgah_qpn *ret = new sgah_qpn();

		ret->param_tbl = new param_table();
		std::vector<std::string> param_names = param_tbl->get_param_names();
		int pi;
		for(pi=0;pi<param_names.size();pi++){
			data_type *dt = param_tbl->get_data_type(param_names[pi]);
			ret->param_tbl->add_param(param_names[pi],dt->duplicate(),
							param_tbl->handle_access(param_names[pi]));
		}
		ret->definitions = definitions;

		ret->node_name = node_name + suffix;

		// make shallow copy of all fields
		ret->where = where;
		ret->having = having;
		ret->select_list = select_list;
		ret->gb_tbl = gb_tbl;
		ret->aggr_tbl = aggr_tbl;

		return ret;
	};

//		Split aggregation into two HFTA components - sub and superaggregation
//		If unable to split the aggreagates, split into selection and aggregation
//		If resulting low-level query is empty (e.g. when aggregates cannot be split and
//		where clause is empty) empty vector willb e returned
	virtual std::vector<qp_node *> split_node_for_hfta(ext_fcn_list *Ext_fcns, table_list *Schema);

	void create_protocol_se(vector<qp_node *> q_sources, table_list *Schema);

};




//		Select, group-by, aggregate. with running aggregates
//		Representing
//			Select SE_1, ..., SE_k
//			From T
//			Where predicate
//			Group By gb1, ..., gb_n
//			Closing When predicate
//			Having predicate
//
//		NOTE : the sampling operator is sgahcwcb_qpn.
//
//		For now, must have group-by variables and aggregates.
//		The scalar expressions which are output must be a function
//		of the groub-by variables and the aggregates.
//		The group-by variables can be references to columsn of T,
//		or they can be scalar expressions.
class rsgah_qpn: public qp_node{
public:
	tablevar_t *table_name;				// source table
	std::vector<cnf_elem *> where;		// selection predicate
	std::vector<cnf_elem *> having;		// post-aggregation predicate
	std::vector<cnf_elem *> closing_when;		// group closing predicate
	std::vector<select_element *> select_list;	// se's of output
	gb_table gb_tbl;			// Table of all group-by attributes.
	aggregate_table aggr_tbl;	// Table of all referenced aggregates.

	std::vector<scalarexp_t *> gb_sources;	// pre-compute for partitioning.

	int lfta_disorder;		// maximum disorder allowed in stream between lfta, hfta
	int hfta_disorder;		// maximum disorder allowed in hfta

	std::vector<scalarexp_t *> get_gb_sources(){return gb_sources;}


	std::string node_type(){return("rsgah_qpn");	};
    bool makes_transform(){return true;};
	std::vector<std::string> external_libs(){
		std::vector<std::string> ret;
		return ret;
	}

	void bind_to_schema(table_list *Schema);
	col_id_set get_colrefs(bool ext_fcns_only,table_list *Schema){
		fprintf(stderr,"INTERNAL ERROR, calling rsgah_qpn::get_colrefs\n");
		exit(1);
	}

	std::string to_query_string();
  	std::string generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, std::vector<bool> &needs_xform);
  	std::string generate_functor_name();

	std::string generate_operator(int i, std::string params);
  	std::string get_include_file(){return("#include <running_gb_operator.h>\n");};

    std::vector<select_element *> get_select_list(){return select_list;};
    std::vector<scalarexp_t *> get_select_se_list(){
		std::vector<scalarexp_t *> ret;
		int i;
		for(i=0;i<select_list.size();++i) ret.push_back(select_list[i]->se);
		return ret;
	};
    std::vector<cnf_elem *> get_where_clause(){return where;};
    std::vector<cnf_elem *> get_filter_clause(){return where;};
    std::vector<cnf_elem *> get_having_clause(){return having;};
    std::vector<cnf_elem *> get_closing_when_clause(){return closing_when;};
    gb_table *get_gb_tbl(){return &gb_tbl;};
    aggregate_table *get_aggr_tbl(){return &aggr_tbl;};
	cplx_lit_table *get_cplx_lit_tbl(ext_fcn_list *Ext_fcns);
    std::vector<handle_param_tbl_entry *> get_handle_param_tbl(ext_fcn_list *Ext_fcns);

//				table which represents output tuple.
	table_def *get_fields();
	std::vector<tablevar_t *> get_input_tbls();
	std::vector<tablevar_t *> get_output_tbls();


	rsgah_qpn(){
		lfta_disorder = 1;
		hfta_disorder = 1;
	};
	rsgah_qpn(query_summary_class *qs,table_list *Schema){
		lfta_disorder = 1;
		hfta_disorder = 1;

//				Get the table name.
//				NOTE the colrefs have the tablevar ref (an int)
//				embedded in them.  Would it make sense
//				to grab the whole table list?
		tablevar_list_t *fm = qs->fta_tree->get_from();
		std::vector<tablevar_t *> tbl_vec = fm->get_table_list();
		if(tbl_vec.size() != 1){
			char tmpstr[200];
			sprintf(tmpstr,"INTERNAL ERROR buildingR SGAH node: query defined over %lu tables.\n",tbl_vec.size() );
			err_str=tmpstr;
			error_code = 1;
		}
		table_name = (tbl_vec[0]);

//				Get the select list.
		select_list = qs->fta_tree->get_sl_vec();

//				Get the selection and having predicates.
		where = qs->wh_cnf;
		having = qs->hav_cnf;
		closing_when = qs->closew_cnf;

//				Build a new GB var table (don't share, might need to modify)
		int g;
		for(g=0;g<qs->gb_tbl->size();g++){
			gb_tbl.add_gb_var(qs->gb_tbl->get_name(g),
				qs->gb_tbl->get_tblvar_ref(g), qs->gb_tbl->get_def(g),
				qs->gb_tbl->get_reftype(g)
			);
		}

//				Build a new aggregate table. (don't share, might need
//				to modify).
		int a;
		for(a=0;a<qs->aggr_tbl->size();a++){
			aggr_tbl.add_aggr(
//				qs->aggr_tbl->get_op(a), qs->aggr_tbl->get_aggr_se(a)
				qs->aggr_tbl->duplicate(a)
			);
		}


//				Get the parameters
		param_tbl = qs->param_tbl;

	};



	std::vector<qp_node *> split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx);
	std::vector<qp_node *> split_node_for_hfta(ext_fcn_list *Ext_fcns, table_list *Schema);
	virtual std::vector<table_exp_t *> extract_opview(table_list *Schema, std::vector<query_node *> &qnodes, opview_set &opviews, std::string rootnm, std::string silo_nm);
//		Ensure that any refs to interface params have been split away.
	int count_ifp_refs(std::set<std::string> &ifpnames);
	int resolve_if_params(ifq_t *ifdb, std::string &err){ return 0;}

	// the following method is used for distributed query optimization
	double get_rate_estimate();

	qp_node* make_copy(std::string suffix){
		rsgah_qpn *ret = new rsgah_qpn();

		ret->param_tbl = new param_table();
		std::vector<std::string> param_names = param_tbl->get_param_names();
		int pi;
		for(pi=0;pi<param_names.size();pi++){
			data_type *dt = param_tbl->get_data_type(param_names[pi]);
			ret->param_tbl->add_param(param_names[pi],dt->duplicate(),
							param_tbl->handle_access(param_names[pi]));
		}
		ret->definitions = definitions;

		ret->node_name = node_name + suffix;

		// make shallow copy of all fields
		ret->where = where;
		ret->having = having;
		ret->closing_when = closing_when;
		ret->select_list = select_list;
		ret->gb_tbl = gb_tbl;
		ret->aggr_tbl = aggr_tbl;

		return ret;
	};
	void create_protocol_se(vector<qp_node *> q_sources, table_list *Schema);
};


//		forward reference
class filter_join_qpn;


//		(temporal) Merge query plan node.
//		represent the following query fragment
//			Merge c1:c2
//			from T1 _t1, T2 _t2
//
//		T1 and T2 must have compatible schemas,
//		that is the same types in the same slots.
//		c1 and c2 must be colrefs from T1 and T2,
//		both ref'ing the same slot.  Their types
//		must be temporal and the same kind of temporal.
//		in the output, no other field is temporal.
//		the field names ofthe output are drawn from T1.
class mrg_qpn: public qp_node{
public:
	std::vector<tablevar_t *> fm;					//	Source table
	std::vector<colref_t *> mvars;					// the merge-by columns.
	scalarexp_t *slack;

	table_def *table_layout;				// the output schema
	int merge_fieldpos;						// position of merge field,
											// convenience for manipulation.

	int disorder;		// max disorder seen in the input / allowed in the output


	// partition definition for merges that combine streams partitioned over multiple interfaces
	partn_def_t* partn_def;



	std::string node_type(){return("mrg_qpn");	};
    bool makes_transform(){return false;};
	std::vector<std::string> external_libs(){
		std::vector<std::string> ret;
		return ret;
	}

	void bind_to_schema(table_list *Schema);
	col_id_set get_colrefs(bool ext_fcns_only,table_list *Schema){
		fprintf(stderr,"INTERNAL ERROR, calling mrg_qpn::get_colrefs\n");
		exit(1);
	}

	std::string to_query_string();
  	std::string generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, std::vector<bool> &needs_xform);
  	std::string generate_functor_name();
	std::string generate_operator(int i, std::string params);
  	std::string get_include_file(){
		if(disorder>1)
			return("#include <merge_operator_oop.h>\n");
		return("#include <merge_operator.h>\n");
	};

	cplx_lit_table *get_cplx_lit_tbl(ext_fcn_list *Ext_fcns);
    std::vector<handle_param_tbl_entry *> get_handle_param_tbl(ext_fcn_list *Ext_fcns);

	table_def *get_fields();
	std::vector<tablevar_t *> get_input_tbls();
	std::vector<tablevar_t *> get_output_tbls();

	std::vector<qp_node *> split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx);
	virtual std::vector<table_exp_t *> extract_opview(table_list *Schema, std::vector<query_node *> &qnodes,  opview_set &opviews, std::string rootnm, std::string silo_nm);
//		Ensure that any refs to interface params have been split away.
	int count_ifp_refs(std::set<std::string> &ifpnames);

//			No predicates, return an empty clause
    std::vector<cnf_elem *> get_where_clause(){
		 std::vector<cnf_elem *> t;
		return(t);
	};
    std::vector<cnf_elem *> get_filter_clause(){
		return get_where_clause();
	}

	mrg_qpn(){
		partn_def = NULL;
	};

	void set_disorder(int d){
		disorder = d;
	}

	mrg_qpn(query_summary_class *qs,table_list *Schema){
		disorder = 1;

//				Grab the elements of the query node.
		fm = qs->fta_tree->get_from()->get_table_list();
		mvars = qs->mvars;
		slack = qs->slack;

//			sanity check
		if(fm.size() != mvars.size()){
			fprintf(stderr,"INTERNAL ERROR in mrg_qpn::mrg_qpn.  fm.size() = %lu, mvars.size() = %lu\n",fm.size(),mvars.size());
			exit(1);
		}

//				Get the parameters
		param_tbl = qs->param_tbl;

//				Need to set the node name now, so that the
//				schema (table_layout) can be properly named.
//				TODO: Setting the name of the table might best be done
//				via the set_node_name method, because presumably
//				thats when the node name is really known.
//				This should propogate to the table_def table_layout
		node_name=qs->query_name;

/*
int ff;
printf("instantiating merge node, name = %s, %d sources.\n\t",node_name.c_str(), fm.size());
for(ff=0;ff<fm.size();++ff){
printf("%s ",fm[ff]->to_string().c_str());
}
printf("\n");
*/


//		Create the output schema.
//		strip temporal properites form all fields except the merge field.
		std::vector<field_entry *> flva = Schema->get_fields(fm[0]->get_schema_name());
		field_entry_list *fel = new field_entry_list();
		int f;
		for(f=0;f<flva.size();++f){
			field_entry *fe;
			data_type dt(flva[f]->get_type().c_str(), flva[f]->get_modifier_list());
			if(flva[f]->get_name() == mvars[0]->get_field()){
				merge_fieldpos = f;
//				if(slack != NULL) dt.reset_temporal();
			}else{
				dt.reset_temporal();
			}

			param_list *plist = new param_list();
			std::vector<std::string> param_strings = dt.get_param_keys();
			int p;
			for(p=0;p<param_strings.size();++p){
				std::string v = dt.get_param_val(param_strings[p]);
				if(v != "")
					plist->append(param_strings[p].c_str(),v.c_str());
				else
					plist->append(param_strings[p].c_str());
			}


			fe=new field_entry(
				dt.get_type_str().c_str(), flva[f]->get_name().c_str(),"",plist, flva[f]->get_unpack_fcns());
			fel->append_field(fe);
		}




		table_layout = new table_def(
			node_name.c_str(), NULL, NULL, fel, STREAM_SCHEMA
		);

		partn_def = NULL;
	};


/////////////////////////////////////////////
///		Created for de-siloing.  to be removed?  or is it otherwise useful?
//		Merge existing set of sources (de-siloing)
	mrg_qpn(std::string n_name, std::vector<std::string> &src_names,table_list *Schema){
		int i,f;

		disorder = 1;

//				Construct the fm list
		for(f=0;f<src_names.size();++f){
			int tbl_ref = Schema->get_table_ref(src_names[f]);
			if(tbl_ref < 0){
				fprintf(stderr,"INTERNAL ERROR, can't find %s in the schema when constructing no-silo merge node %s\n",src_names[f].c_str(), n_name.c_str());
				exit(1);
			}
			table_def *src_tbl = Schema->get_table(tbl_ref);
			tablevar_t *fm_t = new tablevar_t(src_names[f].c_str());
			string range_name = "_t" + int_to_string(f);
			fm_t->set_range_var(range_name);
			fm_t->set_schema_ref(tbl_ref);
			fm.push_back(fm_t);
		}

//		Create the output schema.
//		strip temporal properites form all fields except the merge field.
		std::vector<field_entry *> flva = Schema->get_fields(fm[0]->get_schema_name());
		field_entry_list *fel = new field_entry_list();
		bool temporal_found = false;
		for(f=0;f<flva.size();++f){
			field_entry *fe;
			data_type dt(flva[f]->get_type().c_str(), flva[f]->get_modifier_list());
			if(dt.is_temporal() && !temporal_found){
				merge_fieldpos = f;
				temporal_found = true;
			}else{
				dt.reset_temporal();
			}

			param_list *plist = new param_list();
			std::vector<std::string> param_strings = dt.get_param_keys();
			int p;
			for(p=0;p<param_strings.size();++p){
				std::string v = dt.get_param_val(param_strings[p]);
				if(v != "")
					plist->append(param_strings[p].c_str(),v.c_str());
				else
					plist->append(param_strings[p].c_str());
			}

			fe=new field_entry(
				dt.get_type_str().c_str(), flva[f]->get_name().c_str(),"",plist,
				flva[f]->get_unpack_fcns()
			);
			fel->append_field(fe);
		}

		if(! temporal_found){
			fprintf(stderr,"ERROR, can't find temporal field of the sources when constructing no-silo merge node %s\n",n_name.c_str());
			exit(1);
		}

		node_name=n_name;
		table_layout = new table_def(
			node_name.c_str(), NULL, NULL, fel, STREAM_SCHEMA
		);

		partn_def = NULL;
		param_tbl = new param_table();

//			Construct mvars
		for(f=0;f<fm.size();++f){
			std::vector<field_entry *> flv_f = Schema->get_fields(fm[f]->get_schema_name());
			data_type dt_f(flv_f[merge_fieldpos]->get_type().c_str(),
			     flva[merge_fieldpos]->get_modifier_list());

			colref_t *mcr = new colref_t(fm[f]->get_var_name().c_str(),
				flv_f[merge_fieldpos]->get_name().c_str());
			mvars.push_back(mcr);
		}

//		literal_t *s_lit = new literal_t("5",LITERAL_INT);
//		slack = new scalarexp_t(s_lit);
		slack = NULL;

	};
//			end de-siloing
////////////////////////////////////////

	void resolve_slack(scalarexp_t *t_se, std::string fname, std::vector<std::pair<std::string, std::string> > &sources,ifq_t *ifdb, gb_table *gbt);


//			Merge filter_join LFTAs.

	mrg_qpn(filter_join_qpn *spx, std::string n_name, std::vector<std::string> &sources, std::vector<std::pair<std::string, std::string> > &ifaces, ifq_t *ifdb);

//			Merge selection LFTAs.

	mrg_qpn(spx_qpn *spx, std::string n_name, std::vector<std::string> &sources, std::vector<std::pair<std::string, std::string> > &ifaces, ifq_t *ifdb){

		disorder = 1;

		param_tbl = spx->param_tbl;
		int i;
		node_name = n_name;
		field_entry_list *fel = new field_entry_list();
		merge_fieldpos = -1;




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

//		Merge aggregation LFTAs

	mrg_qpn(sgah_qpn *sgah, std::string n_name, std::vector<std::string> &sources, std::vector<std::pair< std::string, std::string> > &ifaces, ifq_t *ifdb){

		disorder = 1;

		param_tbl = sgah->param_tbl;
		int i;
		node_name = n_name;
		field_entry_list *fel = new field_entry_list();
		merge_fieldpos = -1;
		for(i=0;i<sgah->select_list.size();++i){
			data_type *dt = sgah->select_list[i]->se->get_data_type()->duplicate();
			if(dt->is_temporal()){
				if(merge_fieldpos < 0){
					merge_fieldpos = i;
				}else{
					fprintf(stderr,"Warning: Merge subquery %s found two temporal fields (%s, %s), using %s\n", n_name.c_str(), sgah->select_list[merge_fieldpos]->name.c_str(), sgah->select_list[i]->name.c_str(), sgah->select_list[merge_fieldpos]->name.c_str() );
					dt->reset_temporal();
				}
			}

			field_entry *fe = dt->make_field_entry(sgah->select_list[i]->name);
			fel->append_field(fe);
			delete dt;
		}
		if(merge_fieldpos<0){
			fprintf(stderr,"ERROR, no temporal attribute for merge subquery %s\n",n_name.c_str());
			exit(1);
		}
		table_layout = new table_def( n_name.c_str(), NULL, NULL, fel, STREAM_SCHEMA);

//				NEED TO HANDLE USER_SPECIFIED SLACK
		this->resolve_slack(sgah->select_list[merge_fieldpos]->se,
				sgah->select_list[merge_fieldpos]->name, ifaces, ifdb,
				&(sgah->gb_tbl));
		if(this->slack == NULL)
			fprintf(stderr,"Zero slack.\n");
		else
			fprintf(stderr,"slack is %s\n",slack->to_string().c_str());


		for(i=0;i<sources.size();i++){
			std::string rvar = "_m"+int_to_string(i);
			mvars.push_back(new colref_t(rvar.c_str(), sgah->select_list[merge_fieldpos]->name.c_str()));
			mvars[i]->set_tablevar_ref(i);
			fm.push_back(new tablevar_t(sources[i].c_str()));
			fm[i]->set_range_var(rvar);
		}

		param_tbl = new param_table();
		std::vector<std::string> param_names = sgah->param_tbl->get_param_names();
		int pi;
		for(pi=0;pi<param_names.size();pi++){
			data_type *dt = sgah->param_tbl->get_data_type(param_names[pi]);
			param_tbl->add_param(param_names[pi],dt->duplicate(),
							sgah->param_tbl->handle_access(param_names[pi]));
		}
		definitions = sgah->definitions;

	}

	qp_node *make_copy(std::string suffix){
		mrg_qpn *ret = new mrg_qpn();
		ret->slack = slack;
		ret->disorder = disorder;

		ret->param_tbl = new param_table();
		std::vector<std::string> param_names = param_tbl->get_param_names();
		int pi;
		for(pi=0;pi<param_names.size();pi++){
			data_type *dt = param_tbl->get_data_type(param_names[pi]);
			ret->param_tbl->add_param(param_names[pi],dt->duplicate(),
							param_tbl->handle_access(param_names[pi]));
		}
		ret->definitions = definitions;

		ret->node_name = node_name + suffix;
		ret->table_layout = table_layout->make_shallow_copy(ret->node_name);
		ret->merge_fieldpos = merge_fieldpos;

		return ret;
	};

	std::vector<mrg_qpn *> split_sources();

	// the following method is used for distributed query optimization
	double get_rate_estimate();


	// get partition definition for merges that combine streams partitioned over multiple interfaces
	// return NULL for regular merges
	partn_def_t* get_partn_definition(map<string, int> lfta_names, vector<string> interface_names, vector<string> machine_names, ifq_t *ifaces_db, partn_def_list_t *partn_parse_result) {
		if (partn_def)
			return partn_def;

		int err;
		string err_str;
		string partn_name;

		vector<tablevar_t *> input_tables = get_input_tbls();
		for (int i = 0; i <  input_tables.size(); ++i) {
			tablevar_t * table = input_tables[i];

			vector<string> partn_names = ifaces_db->get_iface_vals(table->get_machine(), table->get_interface(),"iface_partition",err,err_str);
			if (partn_names.size() != 1)	// can't have more than one value of partition attribute
				return NULL;
			string new_partn_name = partn_names[0];

			// need to make sure that all ifaces belong to the same partition
			if (!i)
				partn_name = new_partn_name;
			else if (new_partn_name != partn_name)
				return NULL;
		}

		// now find partition definition corresponding to partn_name
		partn_def = partn_parse_result->get_partn_def(partn_name);
		return partn_def;
	};

	void set_partn_definition(partn_def_t* def) {
		partn_def = def;
	}

	bool is_multihost_merge() {

		bool is_multihost = false;

		// each input table must be have machine attribute be non-empty
		// and there should be at least 2 different values of machine attributes
		vector<tablevar_t *> input_tables = get_input_tbls();
		string host = input_tables[0]->get_machine();
		for  (int i = 1; i < input_tables.size(); ++i) {
			string new_host = input_tables[i]->get_machine();
			if (new_host == "")
				return false;
			if (new_host != host)
				is_multihost = true;
		}
		return is_multihost;
	}

	void create_protocol_se(vector<qp_node *> q_sources, table_list *Schema);
};


//		eq_temporal, hash join query plan node.
//		represent the following query fragment
//			select scalar_expression_1, ..., scalar_expression_k
//			from T0 t0, T1 t1
//			where predicate
//
//		the predicates and the scalar expressions can reference
//		attributes of t0 and t1 and also functions.
//		The predicate must contain CNF elements to enable the
//		efficient evaluation of the query.
//		1) at least one predicate of the form
//			(temporal se in t0) = (temporal se in t1)
//		2) at least one predicate of the form
//			(non-temporal se in t0) = (non-temporal se in t1)
//
class join_eq_hash_qpn: public qp_node{
public:
	std::vector<tablevar_t *> from;					//	Source tables
	std::vector<select_element *> select_list;	//	Select list
	std::vector<cnf_elem *> prefilter[2];		// source prefilters
	std::vector<cnf_elem *> temporal_eq;		// define temporal window
	std::vector<cnf_elem *> hash_eq;			// define hash key
	std::vector<cnf_elem *> postfilter;			// final filter on hash matches.

	std::vector<cnf_elem *> where;				// all the filters
												// useful for summary analysis

	std::vector<scalarexp_t *> hash_src_r, hash_src_l;

	std::vector<scalarexp_t *> get_hash_r(){return hash_src_r;}
	std::vector<scalarexp_t *> get_hash_l(){return hash_src_l;}

	std::string node_type(){return("join_eq_hash_qpn");	};
    bool makes_transform(){return true;};
	std::vector<std::string> external_libs(){
		std::vector<std::string> ret;
		return ret;
	}

	void bind_to_schema(table_list *Schema);
	col_id_set get_colrefs(bool ext_fcns_only,table_list *Schema){
		fprintf(stderr,"INTERNAL ERROR, calling join_eq_hash_qpn::get_colrefs\n");
		exit(1);
	}

	std::string to_query_string();
  	std::string generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, std::vector<bool> &needs_xform);
  	std::string generate_functor_name();
	std::string generate_operator(int i, std::string params);
  	std::string get_include_file(){return("#include <join_eq_hash_operator.h>\n");};

    std::vector<select_element *> get_select_list(){return select_list;};
    std::vector<scalarexp_t *> get_select_se_list(){
		std::vector<scalarexp_t *> ret;
		int i;
		for(i=0;i<select_list.size();++i) ret.push_back(select_list[i]->se);
		return ret;
	};
//			Used for LFTA only
    std::vector<cnf_elem *> get_where_clause(){
		 std::vector<cnf_elem *> t;
		return(t);
	};
    std::vector<cnf_elem *> get_filter_clause(){
		return get_where_clause();
	}

	cplx_lit_table *get_cplx_lit_tbl(ext_fcn_list *Ext_fcns);
    std::vector<handle_param_tbl_entry *> get_handle_param_tbl(ext_fcn_list *Ext_fcns);

	table_def *get_fields();
	std::vector<tablevar_t *> get_input_tbls();
	std::vector<tablevar_t *> get_output_tbls();

	std::vector<qp_node *> split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx);
	virtual std::vector<table_exp_t *> extract_opview(table_list *Schema, std::vector<query_node *> &qnodes,  opview_set &opviews, std::string rootnm, std::string silo_nm);
//		Ensure that any refs to interface params have been split away.
	int count_ifp_refs(std::set<std::string> &ifpnames);

	join_eq_hash_qpn(){
	};
	join_eq_hash_qpn(query_summary_class *qs,table_list *Schema){
		int w;
//				Get the table name.
//				NOTE the colrefs have the table ref (an int)
//				embedded in them.  Would it make sense
//				to grab the whole table list?
		from = qs->fta_tree->get_from()->get_table_list();
		if(from.size() != 2){
			char tmpstr[200];
			sprintf(tmpstr,"ERROR building join_eq_hash node: query defined over %lu tables, but joins must be between two sources.\n",from.size() );
			err_str = tmpstr;
			error_code = 1;
		}

//				Get the select list.
		select_list = qs->fta_tree->get_sl_vec();

//				Get the selection predicate.
		where = qs->wh_cnf;
		for(w=0;w<where.size();++w){
			analyze_cnf(where[w]);
			std::vector<int> pred_tbls;
			get_tablevar_ref_pr(where[w]->pr,pred_tbls);
//				Prefilter if refs only one tablevar
			if(pred_tbls.size()==1){
				prefilter[pred_tbls[0]].push_back(where[w]);
				continue;
			}
//				refs nothing -- might be sampling, do it as postfilter.
			if(pred_tbls.size()==0){
				postfilter.push_back(where[w]);
				continue;
			}
//				See if it can be a hash or temporal predicate.
//				NOTE: synchronize with the temporality checking
//				done at join_eq_hash_qpn::get_fields
			if(where[w]->is_atom && where[w]->eq_pred){
				std::vector<int> sel_tbls, ser_tbls;
				get_tablevar_ref_se(where[w]->pr->get_left_se(),sel_tbls);
				get_tablevar_ref_se(where[w]->pr->get_right_se(),ser_tbls);
				if(sel_tbls.size()==1 && ser_tbls.size()==1 && sel_tbls[0] != ser_tbls[0]){
//						make channel 0 SE on LHS.
					if(sel_tbls[0] != 0)
						where[w]->pr->swap_scalar_operands();

					data_type *dtl=where[w]->pr->get_left_se()->get_data_type();
					data_type *dtr=where[w]->pr->get_right_se()->get_data_type();
					if( (dtl->is_increasing() && dtr->is_increasing()) ||
					    (dtl->is_decreasing() && dtr->is_decreasing()) )
							temporal_eq.push_back(where[w]);
					else
							hash_eq.push_back(where[w]);
					continue;

				}
			}
//				All tests failed, fallback is postfilter.
			postfilter.push_back(where[w]);
		}

		if(temporal_eq.size()==0){
			err_str = "ERROR in join query: can't find temporal equality predicate to define a join window.\n";
			error_code = 1;
		}

//				Get the parameters
		param_tbl = qs->param_tbl;

	};

	// the following method is used for distributed query optimization
	double get_rate_estimate();


	qp_node* make_copy(std::string suffix){
		join_eq_hash_qpn *ret = new join_eq_hash_qpn();

		ret->param_tbl = new param_table();
		std::vector<std::string> param_names = param_tbl->get_param_names();
		int pi;
		for(pi=0;pi<param_names.size();pi++){
			data_type *dt = param_tbl->get_data_type(param_names[pi]);
			ret->param_tbl->add_param(param_names[pi],dt->duplicate(),
							param_tbl->handle_access(param_names[pi]));
		}
		ret->definitions = definitions;

		ret->node_name = node_name + suffix;

		// make shallow copy of all fields
		ret->where = where;
		ret->from = from;
		ret->select_list = select_list;
		ret->prefilter[0] = prefilter[0];
		ret->prefilter[1] = prefilter[1];
		ret->postfilter = postfilter;
		ret->temporal_eq = temporal_eq;
		ret->hash_eq = hash_eq;

		return ret;
	};
	void create_protocol_se(vector<qp_node *> q_sources, table_list *Schema);

};


// ---------------------------------------------
//		eq_temporal, hash join query plan node.
//		represent the following query fragment
//			select scalar_expression_1, ..., scalar_expression_k
//			FILTER_JOIN(col, range) from T0 t0, T1 t1
//			where predicate
//
//		t0 is the output range variable, t1 is the filtering range
//		variable.  Both must alias a PROTOCOL.
//		The scalar expressions in the select clause may
//		reference t0 only.
//		The predicates are classified as follows
//		prefilter predicates:
//		  a cheap predicate in t0 such that there is an equivalent
//		  predicate in t1.  Cost decisions about pushing to
//		  lfta prefilter made later.
//		t0 predicates (other than prefilter predicates)
//			-- cheap vs. expensive sorted out at genereate time,
//				the constructor isn't called with the function list.
//		t1 predicates (other than prefiler predicates).
//		equi-join predicates of the form:
//			(se in t0) = (se in t1)
//
//		There must be at least one equi-join predicate.
//		No join predicates other than equi-join predicates
//		  are allowed.
//		Warn on temporal equi-join predicates.
//		t1 predicates should not be expensive ... warn?
//
class filter_join_qpn: public qp_node{
public:
	std::vector<tablevar_t *> from;					//	Source tables
		colref_t *temporal_var;			// join window in FROM
		unsigned int temporal_range;	// metadata.
	std::vector<select_element *> select_list;	//	Select list
	std::vector<cnf_elem *> shared_pred;		// prefilter preds
	std::vector<cnf_elem *> pred_t0;			// main (R) preds
	std::vector<cnf_elem *> pred_t1;			// filtering (S) preds
	std::vector<cnf_elem *> hash_eq;			// define hash key
	std::vector<cnf_elem *> postfilter;			// ref's no table.

	std::vector<cnf_elem *> where;				// all the filters
												// useful for summary analysis

	std::vector<scalarexp_t *> hash_src_r, hash_src_l;
	std::vector<scalarexp_t *> get_hash_r(){return hash_src_r;}
	std::vector<scalarexp_t *> get_hash_l(){return hash_src_l;}


	bool use_bloom;			// true => bloom filter, false => limited hash

	std::string node_type(){return("filter_join");	};
    bool makes_transform(){return true;};
	std::vector<std::string> external_libs(){
		std::vector<std::string> ret;
		return ret;
	}

	void bind_to_schema(table_list *Schema);
	col_id_set get_colrefs(bool ext_fcns_only,table_list *Schema);

	std::string to_query_string();
  	std::string generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, std::vector<bool> &needs_xform){
		fprintf(stderr,"INTERNAL ERROR, filter_join_qpn::generate_functor called\n");
		exit(1);
	}
  	std::string generate_functor_name(){
		fprintf(stderr,"INTERNAL ERROR, filter_join_qpn::generate_functor_name called\n");
		exit(1);
	}
	std::string generate_operator(int i, std::string params){
		fprintf(stderr,"INTERNAL ERROR, filter_join_qpn::generate_operator called\n");
		exit(1);
	}
  	std::string get_include_file(){return("#include <join_eq_hash_operator.h>\n");};

    std::vector<select_element *> get_select_list(){return select_list;};
    std::vector<scalarexp_t *> get_select_se_list(){
		std::vector<scalarexp_t *> ret;
		int i;
		for(i=0;i<select_list.size();++i) ret.push_back(select_list[i]->se);
		return ret;
	};
//			Used for LFTA only
    std::vector<cnf_elem *> get_where_clause(){return where;}
    std::vector<cnf_elem *> get_filter_clause(){return shared_pred;}

	cplx_lit_table *get_cplx_lit_tbl(ext_fcn_list *Ext_fcns);
    std::vector<handle_param_tbl_entry *> get_handle_param_tbl(ext_fcn_list *Ext_fcns);

	table_def *get_fields();
	std::vector<tablevar_t *> get_input_tbls();
	std::vector<tablevar_t *> get_output_tbls();

	std::vector<qp_node *> split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx);
	int resolve_if_params(ifq_t *ifdb, std::string &err);

	virtual std::vector<table_exp_t *> extract_opview(table_list *Schema, std::vector<query_node *> &qnodes,  opview_set &opviews, std::string rootnm, std::string silo_nm);
//		Ensure that any refs to interface params have been split away.
	int count_ifp_refs(std::set<std::string> &ifpnames);


	filter_join_qpn(){
	};
	filter_join_qpn(query_summary_class *qs,table_list *Schema){
		int i,w;
//				Get the table name.
//				NOTE the colrefs have the table ref (an int)
//				embedded in them.  Would it make sense
//				to grab the whole table list?
		from = qs->fta_tree->get_from()->get_table_list();
		temporal_var = qs->fta_tree->get_from()->get_colref();
		temporal_range = qs->fta_tree->get_from()->get_temporal_range();
		if(from.size() != 2){
			char tmpstr[200];
			sprintf(tmpstr,"ERROR building filter_join_qpn node: query defined over %lu tables, but joins must be between two sources.\n",from.size() );
			err_str += tmpstr;
			error_code = 1;
		}

//				Get the select list.
		select_list = qs->fta_tree->get_sl_vec();
//				Verify that only t0 is referenced.
		bool bad_ref = false;
		for(i=0;i<select_list.size();i++){
			vector<int> sel_tbls;
			get_tablevar_ref_se(select_list[i]->se,sel_tbls);
			if((sel_tbls.size() == 2) || (sel_tbls.size()==1 && sel_tbls[0]==1))
				bad_ref = true;
		}
		if(bad_ref){
			err_str += "ERROR building filter_join_qpn node: query references range variable "+from[1]->variable_name+", but only the first range variable ("+from[0]->variable_name+" can be referenced.\n";
			error_code = 1;
		}


//				Get the selection predicate.
		where = qs->wh_cnf;
		std::vector<cnf_elem *> t0_only, t1_only;
		for(w=0;w<where.size();++w){
			analyze_cnf(where[w]);
			std::vector<int> pred_tbls;
			get_tablevar_ref_pr(where[w]->pr,pred_tbls);
//				Collect the list of preds by src var,
//				extract the shared preds later.
			if(pred_tbls.size()==1){
				if(pred_tbls[0] == 0){
					t0_only.push_back(where[w]);
				}else{
					t1_only.push_back(where[w]);
				}
				continue;
			}
//				refs nothing -- might be sampling, do it as postfilter.
			if(pred_tbls.size()==0){
				postfilter.push_back(where[w]);
				continue;
			}
//				See if it can be a hash or temporal predicate.
//				NOTE: synchronize with the temporality checking
//				done at join_eq_hash_qpn::get_fields
			if(where[w]->is_atom && where[w]->eq_pred){
				std::vector<int> sel_tbls, ser_tbls;
				get_tablevar_ref_se(where[w]->pr->get_left_se(),sel_tbls);
				get_tablevar_ref_se(where[w]->pr->get_right_se(),ser_tbls);
				if(sel_tbls.size()==1 && ser_tbls.size()==1 && sel_tbls[0] != ser_tbls[0]){
//						make channel 0 SE on LHS.
					if(sel_tbls[0] != 0)
						where[w]->pr->swap_scalar_operands();

					hash_eq.push_back(where[w]);

					data_type *dtl=where[w]->pr->get_left_se()->get_data_type();
					data_type *dtr=where[w]->pr->get_right_se()->get_data_type();
					if( (dtl->is_increasing() && dtr->is_increasing()) ||
					    (dtl->is_decreasing() && dtr->is_decreasing()) )
						err_str += "Warning, a filter join should not have join predicates on temporal fields.\n";
					continue;

				}
			}
//				All tests failed, fallback is postfilter.
			err_str += "ERROR, join predicates in a filter join should have the form (scalar expression in "+from[0]->variable_name+") = (scalar expression in "+from[1]->variable_name+").\n";
			error_code = 3;
		}
//		Classify the t0_only and t1_only preds.
		set<int> matched_pred;
		int v;
		for(w=0;w<t0_only.size();w++){
			for(v=0;v<t1_only.size();++v)
				if(is_equivalent_pred_base(t0_only[w]->pr,t1_only[v]->pr,Schema))
					break;
			if(v<t1_only.size()){
				shared_pred.push_back(t0_only[w]);
				matched_pred.insert(v);
			}else{
				pred_t0.push_back(t0_only[w]);
			}
		}
		for(v=0;v<t1_only.size();++v){
			if(matched_pred.count(v) == 0)
				pred_t1.push_back(t1_only[v]);
		}


//				Get the parameters
		param_tbl = qs->param_tbl;
		definitions = qs->definitions;

//				Determine the algorithm
		if(this->get_val_of_def("algorithm") == "hash"){
			use_bloom = false;
		}else{
			use_bloom = true;
		}
	};

	// the following method is used for distributed query optimization
	double get_rate_estimate();


	qp_node* make_copy(std::string suffix){
		filter_join_qpn *ret = new filter_join_qpn();

		ret->param_tbl = new param_table();
		std::vector<std::string> param_names = param_tbl->get_param_names();
		int pi;
		for(pi=0;pi<param_names.size();pi++){
			data_type *dt = param_tbl->get_data_type(param_names[pi]);
			ret->param_tbl->add_param(param_names[pi],dt->duplicate(),
							param_tbl->handle_access(param_names[pi]));
		}
		ret->definitions = definitions;

		ret->node_name = node_name + suffix;

		// make shallow copy of all fields
		ret->where = where;
		ret->from = from;
		ret->temporal_range = temporal_range;
		ret->temporal_var = temporal_var;
		ret->select_list = select_list;
		ret->shared_pred = shared_pred;
		ret->pred_t0 = pred_t0;
		ret->pred_t1 = pred_t1;
		ret->postfilter = postfilter;
		ret->hash_eq = hash_eq;

		return ret;
	};
	void create_protocol_se(vector<qp_node *> q_sources, table_list *Schema);

};


enum output_file_type_enum {regular, gzip, bzip};

class output_file_qpn: public qp_node{
public:
	std::string source_op_name;					//	Source table
	std::vector<field_entry *> fields;
	ospec_str *output_spec;
	vector<tablevar_t *> fm;
	std::string hfta_query_name;
	std::string filestream_id;
	bool eat_input;
	std::vector<std::string> params;
	bool do_gzip;
	output_file_type_enum compression_type;

	int n_streams;		// Number of output streams
	int n_hfta_clones;	// number of hfta clones
	int parallel_idx;	// which close this produces output for.
	std::vector<int> hash_flds;	// fields used to hash the output.

	std::string node_type(){return("output_file_qpn");	};
    bool makes_transform(){return false;};
	std::vector<std::string> external_libs(){
		std::vector<std::string> ret;
		switch(compression_type){
		case gzip:
			ret.push_back("-lz");
		break;
		case bzip:
			ret.push_back("-lbz2");
		break;
		default:
		break;
		}
		return ret;
	}

	void bind_to_schema(table_list *Schema){}
	col_id_set get_colrefs(bool ext_fcns_only,table_list *Schema){
		col_id_set ret;
		return ret;
	}

	std::string to_query_string(){return "// output_file_operator \n";}
  	std::string generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, std::vector<bool> &needs_xform);
  	std::string generate_functor_name();
	std::string generate_operator(int i, std::string params);
  	std::string get_include_file(){
		switch(compression_type){
		case gzip:
			return("#include <zfile_output_operator.h>\n");
		default:
			return("#include <file_output_operator.h>\n");
		}
		return("#include <file_output_operator.h>\n");
	};

    std::vector<cnf_elem *> get_where_clause(){std::vector<cnf_elem *> ret; return ret;};
    std::vector<cnf_elem *> get_filter_clause(){std::vector<cnf_elem *> ret; return ret;};
	cplx_lit_table *get_cplx_lit_tbl(ext_fcn_list *Ext_fcns){cplx_lit_table *ret = new cplx_lit_table(); return ret;}
    std::vector<handle_param_tbl_entry *> get_handle_param_tbl(ext_fcn_list *Ext_fcns){std::vector<handle_param_tbl_entry *> ret; return ret;}

	table_def *get_fields(){
		field_entry_list *fel = new field_entry_list();
		int i;
		for(i=0;i<fields.size();++i)
			fel->append_field(fields[i]);
		return new table_def(node_name.c_str(), NULL, NULL, fel, STREAM_SCHEMA);
	}
	std::vector<tablevar_t *> get_input_tbls();
	std::vector<tablevar_t *> get_output_tbls();

	std::vector<qp_node *> split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx){
		std::vector<qp_node *> ret; ret.push_back(this); hfta_returned = true; return ret;
	}
	std::vector<table_exp_t *> extract_opview(table_list *Schema, std::vector<query_node *> &qnodes, opview_set &opviews, std::string rootnm, std::string silo_nm){
		std::vector<table_exp_t *> ret; return ret;
	}
//		Ensure that any refs to interface params have been split away.
	int count_ifp_refs(std::set<std::string> &ifpnames){return 0;}
	int resolve_if_params(ifq_t *ifdb, std::string &err){return 0;};


	output_file_qpn(std::string src_op, std::string qn, std::string fs_id, table_def *src_tbl_def, ospec_str *ospec, bool ei){
		source_op_name = src_op;
		node_name = source_op_name + "_output";
		filestream_id = fs_id;
		fields = src_tbl_def->get_fields();
		output_spec = ospec;
		fm.push_back(new tablevar_t(source_op_name.c_str()));
		hfta_query_name = qn;
		eat_input = ei;

		do_gzip = false;
		compression_type = regular;
		if(ospec->operator_type == "zfile")
			compression_type = gzip;

		n_streams = 1;
		parallel_idx = 0;
		n_hfta_clones = 1;

		char buf[1000];
		strncpy(buf, output_spec->operator_param.c_str(),1000);
		buf[999] = '\0';
		char *words[100];
		int nwords = split_string(buf, ':', words,100);
		int i;
		for(i=0;i<nwords;i++){
			params.push_back(words[i]);
		}
		for(i=0;i<params.size();i++){
			if(params[i] == "gzip")
				do_gzip = true;
		}
	}

//		Set output splitting parameters
	bool set_splitting_params(int np, int ix, int ns, std::string split_flds, std::string &err_report){
		n_streams = ns;
		n_hfta_clones = np;
		parallel_idx = ix;

		if(split_flds != ""){
			string err_flds = "";
			char *tmpstr = strdup(split_flds.c_str());
			char *words[100];
			int nwords = split_string(tmpstr,':',words,100);
			int i,j;
			for(i=0;i<nwords;++i){
				string target = words[i];
				for(j=0;j<fields.size();++j){
					if(fields[j]->get_name() == target){
						hash_flds.push_back(j);
						break;
					}
				}
				if(j==fields.size()){
					err_flds += " "+target;
				}
			}
			if(err_flds != ""){
				err_report += "ERROR in "+hfta_query_name+", a file output operator needs to split the output but these splitting fileds are not part of the output:"+err_flds;
				return true;
			}
		}
		return false;
	}

	// the following method is used for distributed query optimization
	double get_rate_estimate(){return 1.0;}


	qp_node* make_copy(std::string suffix){
//		output_file_qpn *ret = new output_file_qpn();
		output_file_qpn *ret = new output_file_qpn(source_op_name, hfta_query_name, filestream_id, this->get_fields(), output_spec, eat_input);
		return ret;
	}

	void create_protocol_se(vector<qp_node *> q_sources, table_list *Schema){}

};



//

// ---------------------------------------------


//		Select, group-by, aggregate, sampling.
//		Representing
//			Select SE_1, ..., SE_k
//			From T
//			Where predicate
//			Group By gb1, ..., gb_n
//			[Subgroup gb_i1, .., gb_ik]
//			Cleaning_when  predicate
//			Cleaning_by predicate
//			Having predicate
//
//		For now, must have group-by variables and aggregates.
//		The scalar expressions which are output must be a function
//		of the groub-by variables and the aggregates.
//		The group-by variables can be references to columsn of T,
//		or they can be scalar expressions.
class sgahcwcb_qpn: public qp_node{
public:
	tablevar_t *table_name;				// source table
	std::vector<cnf_elem *> where;		// selection predicate
	std::vector<cnf_elem *> having;		// post-aggregation predicate
	std::vector<select_element *> select_list;	// se's of output
	gb_table gb_tbl;			// Table of all group-by attributes.
	std::set<int> sg_tbl;		// Names of the superGB attributes
	aggregate_table aggr_tbl;	// Table of all referenced aggregates.
	std::set<std::string> states_refd;	// states ref'd by stateful fcns.
	std::vector<cnf_elem *> cleanby;
	std::vector<cnf_elem *> cleanwhen;

	std::vector<scalarexp_t *> gb_sources;	// pre-compute for partitioning.

	std::vector<scalarexp_t *> get_gb_sources(){return gb_sources;}

	std::string node_type(){return("sgahcwcb_qpn");	};
    bool makes_transform(){return true;};
	std::vector<std::string> external_libs(){
		std::vector<std::string> ret;
		return ret;
	}

	void bind_to_schema(table_list *Schema);
	col_id_set get_colrefs(bool ext_fcns_only,table_list *Schema){
		fprintf(stderr,"INTERNAL ERROR, calling sgahcwcb_qpn::get_colrefs\n");
		exit(1);
	}

	std::string to_query_string();
  	std::string generate_functor(table_list *schema, ext_fcn_list *Ext_fcns, std::vector<bool> &needs_xform);
  	std::string generate_functor_name();

	std::string generate_operator(int i, std::string params);
  	std::string get_include_file(){return("#include <clean_operator.h>\n");};

    std::vector<select_element *> get_select_list(){return select_list;};
    std::vector<scalarexp_t *> get_select_se_list(){
		std::vector<scalarexp_t *> ret;
		int i;
		for(i=0;i<select_list.size();++i) ret.push_back(select_list[i]->se);
		return ret;
	};
    std::vector<cnf_elem *> get_where_clause(){return where;};
    std::vector<cnf_elem *> get_filter_clause(){return where;};
    std::vector<cnf_elem *> get_having_clause(){return having;};
    gb_table *get_gb_tbl(){return &gb_tbl;};
    aggregate_table *get_aggr_tbl(){return &aggr_tbl;};
	cplx_lit_table *get_cplx_lit_tbl(ext_fcn_list *Ext_fcns);
    std::vector<handle_param_tbl_entry *> get_handle_param_tbl(ext_fcn_list *Ext_fcns);

//				table which represents output tuple.
	table_def *get_fields();
	std::vector<tablevar_t *> get_input_tbls();
	std::vector<tablevar_t *> get_output_tbls();


	sgahcwcb_qpn(){
	};
	sgahcwcb_qpn(query_summary_class *qs,table_list *Schema){
//				Get the table name.
//				NOTE the colrefs have the tablevar ref (an int)
//				embedded in them.  Would it make sense
//				to grab the whole table list?
		tablevar_list_t *fm = qs->fta_tree->get_from();
		std::vector<tablevar_t *> tbl_vec = fm->get_table_list();
		if(tbl_vec.size() != 1){
			char tmpstr[200];
			sprintf(tmpstr,"INTERNAL ERROR building SGAHCWCB node: query defined over %lu tables.\n",tbl_vec.size() );
			err_str=tmpstr;
			error_code = 1;
		}
		table_name = (tbl_vec[0]);

//				Get the select list.
		select_list = qs->fta_tree->get_sl_vec();

//				Get the selection and having predicates.
		where = qs->wh_cnf;
		having = qs->hav_cnf;
		cleanby = qs->cb_cnf;
		cleanwhen = qs->cw_cnf;

//				Build a new GB var table (don't share, might need to modify)
		int g;
		for(g=0;g<qs->gb_tbl->size();g++){
			gb_tbl.add_gb_var(qs->gb_tbl->get_name(g),
				qs->gb_tbl->get_tblvar_ref(g), qs->gb_tbl->get_def(g),
				qs->gb_tbl->get_reftype(g)
			);
		}

//				Build a new aggregate table. (don't share, might need
//				to modify).
		int a;
		for(a=0;a<qs->aggr_tbl->size();a++){
			aggr_tbl.add_aggr(
//				qs->aggr_tbl->get_op(a), qs->aggr_tbl->get_aggr_se(a)
				qs->aggr_tbl->duplicate(a)
			);
		}

		sg_tbl = qs->sg_tbl;
		states_refd = qs->states_refd;


//				Get the parameters
		param_tbl = qs->param_tbl;

	};



	std::vector<qp_node *> split_node_for_fta(ext_fcn_list *Ext_fcns, table_list *Schema, int &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx);
	virtual std::vector<table_exp_t *> extract_opview(table_list *Schema, std::vector<query_node *> &qnodes, opview_set &opviews, std::string rootnm, std::string silo_nm);
//		Ensure that any refs to interface params have been split away.
//			CURRENTLY not allowed by split_node_for_fta
	int count_ifp_refs(std::set<std::string> &ifpnames){return 0;}
	int resolve_if_params(ifq_t *ifdb, std::string &err){return 0;}

	// the following method is used for distributed query optimization
	double get_rate_estimate();

	qp_node* make_copy(std::string suffix){
		sgahcwcb_qpn *ret = new sgahcwcb_qpn();

		ret->param_tbl = new param_table();
		std::vector<std::string> param_names = param_tbl->get_param_names();
		int pi;
		for(pi=0;pi<param_names.size();pi++){
			data_type *dt = param_tbl->get_data_type(param_names[pi]);
			ret->param_tbl->add_param(param_names[pi],dt->duplicate(),
							param_tbl->handle_access(param_names[pi]));
		}
		ret->definitions = definitions;

		ret->node_name = node_name + suffix;

		// make shallow copy of all fields
		ret->where = where;
		ret->having = having;
		ret->select_list = select_list;
		ret->gb_tbl = gb_tbl;
		ret->aggr_tbl = aggr_tbl;
		ret->sg_tbl = sg_tbl;
		ret->states_refd = states_refd;
		ret->cleanby = cleanby;
		ret->cleanwhen = cleanwhen;

		return ret;
	};

	void create_protocol_se(vector<qp_node *> q_sources, table_list *Schema);
};


std::vector<qp_node *> create_query_nodes(query_summary_class *qs,table_list *Schema);



void untaboo(string &s);

table_def *create_attributes(string tname, vector<select_element *> &select_list);



#endif
