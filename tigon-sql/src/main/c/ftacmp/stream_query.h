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
#ifndef __STREAM_QUERY_H_
#define __STREAM_QUERY_H_

#include<vector>
#include<string>
#include<map>

#include"parse_schema.h"
#include"parse_fta.h"
#include"analyze_fta.h"
#include"query_plan.h"
#include"parse_partn.h"


class stream_query{
public:
//		query_plan contains the query nodes, which form a query tree (dag?)
//		by linking to each other via indices into this vector.
//		NOTE: there might be holes (NULLs) in this list due to shifting
//		around operators for optimization.
	std::vector<qp_node *> query_plan;
	std::vector<ospec_str *> output_specs;
	vector<qp_node *> output_operators;

	int qhead;
	std::vector<int> qtail;

	table_def *attributes;
	param_table *parameters;
	std::map<std::string, std::string> defines;
	std::string query_name;

	int n_parallel;		// If the HFTA has been parallelized, # of copies.
	int parallel_idx;	// which of the cloned hftas this one is.
						// needed for output splitting.

	int n_successors;	// # of hftas which read from this one.

	int gid;		// global id

//		For error reporting.
	int error_code;
	std::string err_str;

  stream_query(){
	error_code = 0;
	attributes = NULL;
	parameters = NULL;
  };

//		Create stream query for split-off lfta.
//		The parent stream query provides annotations.
  stream_query(qp_node *qnode, stream_query *parent);

//		Create stream query from analyzed parse tree.
  stream_query(query_summary_class *qs,table_list *Schema);

//		Make a copy.
  stream_query(stream_query &source);
//	used after making the copy
  void set_nparallel(int n, int i){n_parallel = n; parallel_idx = i;}

  qp_node *get_query_head(){return query_plan[qhead];};
  std::string get_sq_name(){return query_plan[qhead]->get_node_name();};

//		Add a parse tree to the query plan.
  stream_query *add_query(query_summary_class *qs,table_list *Schema);
  stream_query *add_query(stream_query &sc);

  bool generate_linkage();
  int generate_plan(table_list *Schema);


//		Create a map of fields to scalar expression in Protocol fields
//		for each query node in the stream.  For partitioning optimizations.
  void generate_protocol_se(map<string,stream_query *> &sq_map, table_list *Schema);


//		Checks if the operator i is compatible with interface partitioning
//		(can be pushed below the merge that combines partitioned stream)
  bool is_partn_compatible(int i, map<string, int> lfta_names, vector<string> interface_names, vector<string> machine_names, ifq_t *ifaces_db, partn_def_list_t *partn_parse_result);

//		Checks if the node i can be pushed below the merge
  bool is_pushdown_compatible(int i, map<string, int> lfta_names, vector<string> interface_names, vector<string> machine_names);

//		Push the operator below the merge that combines partitioned stream
  void pushdown_partn_operator(int i);

//		Push the operator below the merge that combines regular (non-partitioned streams)
  void pushdown_operator(int i, ext_fcn_list *Ext_fcns, table_list *Schema);

//		Splits query that combines data from multiple hosts into separate hftas
  std::vector<stream_query*> split_multihost_query();

//		Extract subtree rooted at node i into separate hfta
  stream_query* extract_subtree(int i);

//		Extract any external libraries needed by the oeprators in this hfta
	void get_external_libs(std::set<std::string> &libset);


//		Perform local FTA optimizations
  void optimize(vector<stream_query *>& hfta_list, map<string, int> lfta_names, vector<string> interface_names, vector<string> machine_names, ext_fcn_list *Ext_fcns, table_list *Schema, ifq_t *ifaces_db, partn_def_list_t *partn_parse_result);

  int get_error_code(){return error_code;};
  std::string get_error_str(){return err_str;};

  void set_gid(int i){gid=i;};
  int get_gid(){return gid;};


  bool stream_input_only(table_list *Schema);

  std::vector<tablevar_t *> get_input_tables();

  table_def *get_output_tabledef();

//		Extract lfta components of the query
  std::vector<stream_query *> split_query(ext_fcn_list *Ext_fcns, table_list *Schema, bool &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx);

//		Process and register opertor views
  std::vector<table_exp_t *> extract_opview(table_list *Schema, std::vector<query_node *> &qnodes, opview_set &opviews, std::string silo_nm);

  std::string collect_refd_ifaces();

//			The serialization function
  std::string make_schema();
  std::string make_schema(int i);

//			hfta generation.  Schema must contain the table_def's
//			of all source tables (but extra tables will not hurt).
  std::string generate_hfta(table_list *Schema, ext_fcn_list *Ext_fcns, opview_set &opviews, bool distributed_mode);

//		De-silo an hfta by replacing refs to lfta inputs with a
//		merge over all silo'ed lftas.
//		TO BE REMOVED
void desilo_lftas(std::map<std::string, int> &lfta_names,std::vector<std::string> &ifq_names,table_list *Schema);

void add_output_operator(ospec_str *);

private:
//		Helper function for generate_hfta
void compute_node_format(int q, std::vector<int> &nfmt, std::map<std::string, int> &op_idx);



};

///////////////////////////////////////////////////////////////////////
////		Related functions
void get_common_lfta_filter(std::vector<stream_query *> lfta_list,table_list *Schema,ext_fcn_list *Ext_fcns, std::vector<cnf_set *> &prefilter_preds, std::set<unsigned int> &pred_ids);
void get_prefilter_temporal_cids(std::vector<stream_query *> lfta_list, col_id_set &temp_cids);



#endif
