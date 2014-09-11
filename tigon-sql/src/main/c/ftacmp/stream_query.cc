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
#include"stream_query.h"
#include"generate_utils.h"
#include"analyze_fta.h"
#include<algorithm>
#include<utility>
#include <list>

static char tmpstr[500];

using namespace std;


//		Create a query plan from a query node and an existing
//		query plan.  Use for lfta queries, the parent query plan provides
//		the annotations.
stream_query::stream_query(qp_node *qnode, stream_query *parent){
	query_plan.push_back(qnode);
	qhead = 0;
	qtail.push_back(0);
	attributes = qnode->get_fields();
	parameters = qnode->get_param_tbl();
	defines = parent->defines;
	query_name = qnode->get_node_name();
}

//		Copy the query plan.
stream_query::stream_query(stream_query &src){
	query_plan = src.query_plan;
	qhead = src.qhead;
	qtail = src.qtail;
	attributes = src.attributes;
	parameters = src.parameters;
	defines = src.defines;
	query_name = src.query_name;
	gid = src.gid;
}


//		Create a query plan from an analyzed parse tree.
//		Perform analyses to find the output node, input nodes, etc.

stream_query::stream_query(query_summary_class *qs,table_list *Schema){
//		Generate the query plan nodes from the analyzed parse tree.
//		There is only one for now, so just assign the return value
//		of create_query_nodes to query_plan
	error_code = 0;
	query_plan = create_query_nodes(qs,Schema);
    int i;
if(query_plan.size() == 0){
  fprintf(stderr,"INTERNAL ERROR, zero-size query plan in stream_query::stream_query\n");
  exit(1);
}
    for(i=0;i<query_plan.size();++i){
		if(query_plan[i]){
			if(query_plan[i]->get_error_code() != 0){
				error_code = query_plan[i]->get_error_code();
				err_str +=  query_plan[i]->get_error_str();
			}
		}
	}
	qhead = query_plan.size()-1;
	gid = -1;

}


stream_query * stream_query::add_query(query_summary_class *qs,table_list *Schema){
//		Add another query block to the query plan
	error_code = 0;
	vector<qp_node *> new_nodes = create_query_nodes(qs, Schema);
	query_plan.insert(query_plan.end(),new_nodes.begin(), new_nodes.end());
	return this;
}

stream_query * stream_query::add_query(stream_query &src){
//		Add another query block to the query plan
	error_code = 0;
	query_plan.insert(query_plan.end(),src.query_plan.begin(), src.query_plan.end());
	return this;
}


void stream_query::generate_protocol_se(map<string,stream_query *> &sq_map, table_list *Schema){
	int i,n;

//		Mapping fields to protocol fields requires schema binding.
//      First ensure all nodes are in the schema.
    for(n=0;n<query_plan.size();++n){
        if(query_plan[n] != NULL){
            Schema->add_table(query_plan[n]->get_fields());
        }
    }
//		Now do schema binding
	for(n=0;n<query_plan.size();++n){
		if(query_plan[n] != NULL){
			query_plan[n]->bind_to_schema(Schema);
		}
	}

//			create name-to-index map
	map<string, int> name_to_node;
	for(n=0;n<query_plan.size();++n){
		if(query_plan[n]){
			name_to_node[query_plan[n]->get_node_name()] = n;
		}
	}

//		Create a list of the nodes to process, in order.
//		Search from the root down.
//			ASSUME tree plan.

	list<int> search_q;
	list<int> work_list;

	search_q.push_back(qhead);
	while(! search_q.empty()){
		int the_q = search_q.front();
		search_q.pop_front(); work_list.push_front(the_q);
		vector<int> the_pred = query_plan[the_q]->get_predecessors();
		for(i=0;i<the_pred.size();i++)
			search_q.push_back(the_pred[i]);
	}

//		Scan through the work list, from the front,
//		gather the qp_node's ref'd, and call the
//		protocol se generator.  Pass NULL for
//		sources not found - presumably user-defined operator

	while(! work_list.empty()){
		int the_q = work_list.front();
		work_list.pop_front();
		vector<qp_node *> q_sources;
		vector<tablevar_t *> q_input_tbls = query_plan[the_q]->get_input_tbls();
		for(i=0;i<q_input_tbls.size();++i){
			 string itbl_nm = q_input_tbls[i]->get_schema_name();
			 if(name_to_node.count(itbl_nm)>0){
				 q_sources.push_back(query_plan[name_to_node[itbl_nm]]);
			 }else if(sq_map.count(itbl_nm)>0){
				 q_sources.push_back(sq_map[itbl_nm]->get_query_head());
			 }else{
				 q_sources.push_back(NULL);
			 }
		}
		query_plan[the_q]->create_protocol_se(q_sources, Schema);
	}

//////////////////////////////////////////////////////////
//			trust but verify


/*
	for(i=0;i<query_plan.size();++i){
		if(query_plan[i]){
			printf("query node %s, type=%s:\n",query_plan[i]->get_node_name().c_str(),
					query_plan[i]->node_type().c_str());
			map<std::string, scalarexp_t *> *pse_map = query_plan[i]->get_protocol_se();
			map<std::string, scalarexp_t *>::iterator mssi;
			for(mssi=pse_map->begin();mssi!=pse_map->end();++mssi){
				if((*mssi).second)
					printf("\t%s : %s\n",(*mssi).first.c_str(), (*mssi).second->to_string().c_str());
				else
					printf("\t%s : NULL\n",(*mssi).first.c_str());
			}
			if(query_plan[i]->node_type() == "filter_join" || query_plan[i]->node_type() == "join_eq_hash_qpn"){
				vector<scalarexp_t *> pse_l;
				vector<scalarexp_t *> pse_r;
				if(query_plan[i]->node_type() == "filter_join"){
					pse_l = ((filter_join_qpn *)query_plan[i])->get_hash_l();
					pse_r = ((filter_join_qpn *)query_plan[i])->get_hash_r();
				}
				if(query_plan[i]->node_type() == "join_eq_hash_qpn"){
					pse_l = ((join_eq_hash_qpn *)query_plan[i])->get_hash_l();
					pse_r = ((join_eq_hash_qpn *)query_plan[i])->get_hash_r();
				}
				int p;
				for(p=0;p<pse_l.size();++p){
					if(pse_l[p] != NULL)
						printf("\t\t%s = ",pse_l[p]->to_string().c_str());
					else
						printf("\t\tNULL = ");
					if(pse_r[p] != NULL)
						printf("%s\n",pse_r[p]->to_string().c_str());
					else
						printf("NULL\n");
				}
			}
			if(query_plan[i]->node_type() == "sgah_qpn" || query_plan[i]->node_type() == "rsgah_qpn" || query_plan[i]->node_type() == "sgahcwcb_qpn"){
				vector<scalarexp_t *> pseg;
                if(query_plan[i]->node_type() == "sgah_qpn")
					pseg = ((sgah_qpn *)query_plan[i])->get_gb_sources();
                if(query_plan[i]->node_type() == "rsgah_qpn")
					pseg = ((rsgah_qpn *)query_plan[i])->get_gb_sources();
                if(query_plan[i]->node_type() == "sgahcwcb_qpn")
					pseg = ((sgahcwcb_qpn *)query_plan[i])->get_gb_sources();
				int g;
				for(g=0;g<pseg.size();g++){
					if(pseg[g] != NULL)
						printf("\t\tgb %d = %s\n",g,pseg[g]->to_string().c_str());
					else
						printf("\t\tgb %d = NULL\n",g);
				}
			}
		}
	}
*/

}

bool stream_query::generate_linkage(){
	bool create_failed = false;
	int n, f,s;

//			Clear any leftover linkages
	for(n=0;n<query_plan.size();++n){
		if(query_plan[n]){
			query_plan[n]->clear_predecessors();
			query_plan[n]->clear_successors();
		}
	}
	qtail.clear();

//			create name-to-index map
	map<string, int> name_to_node;
	for(n=0;n<query_plan.size();++n){
		if(query_plan[n]){
			name_to_node[query_plan[n]->get_node_name()] = n;
		}
	}

//		Do the 2-way linkage.
	for(n=0;n<query_plan.size();++n){
		if(query_plan[n]){
		  vector<tablevar_t *> fm  = query_plan[n]->get_input_tbls();
		  for(f=0;f<fm.size();++f){
			string src_tbl = fm[f]->get_schema_name();
			if(name_to_node.count(src_tbl)>0){
				int s = name_to_node[src_tbl];
				query_plan[n]->add_predecessor(s);
				query_plan[s]->add_successor(n);
			}
		  }
		}
	}

//		Find the head (no successors) and the tails (at least one
//		predecessor is external).
//		Verify that there is only one head,
//		and that all other nodes have one successor (because
//		 right now I can only handle trees).

	qhead = -1;		// no head yet found.
	for(n=0;n<query_plan.size();++n){
		if(query_plan[n]){
		  vector<int> succ = query_plan[n]->get_successors();
/*
		  if(succ.size() > 1){
			fprintf(stderr,"ERROR, query node %s supplies data to multiple nodes, but currently only tree query plans are supported:\n",query_plan[n]->get_node_name().c_str());
			for(s=0;s<succ.size();++s){
				fprintf(stderr,"%s ",query_plan[succ[s]]->get_node_name().c_str());
			}
			fprintf(stderr,"\n");
			create_failed = true;
		  }
*/
		  if(succ.size() == 0){
			if(qhead >= 0){
				fprintf(stderr,"ERROR, query nodes %s and %s both have zero successors.\n",query_plan[n]->get_node_name().c_str(), query_plan[qhead]->get_node_name().c_str());
				create_failed = true;
			}else{
				qhead = n;
			}
		  }
		  if(query_plan[n]->n_predecessors() < query_plan[n]->get_input_tbls().size()){
			qtail.push_back(n);
		  }
		}
	}

	return create_failed;
}

//		After the collection of query plan nodes is generated,
//		analyze their structure to link them up into a tree (or dag?).
//		Verify that the structure is acceptable.
//		Do some other analysis and verification tasks (?)
//		then gather summar information.
int stream_query::generate_plan(table_list *Schema){

//		The first thing to do is verify that the query plan
//		nodes were successfully created.
	bool create_failed = false;
	int n,f,s;
	for(n=0;n<query_plan.size();++n){
		if(query_plan[n]!=NULL && query_plan[n]->get_error_code()){
			fprintf(stderr,"%s",query_plan[n]->get_error_str().c_str());
			create_failed = true;
		}
	}
/*
for(n=0;n<query_plan.size();++n){
if(query_plan[n] != NULL){
string nstr = query_plan[n]->get_node_name();
printf("In generate_plan, node %d is %s, reads from:\n\t",n,nstr.c_str());
vector<tablevar_t *> inv = query_plan[n]->get_input_tbls();
int nn;
for(nn=0;nn<inv.size();nn++){
printf("%s (%d) ",inv[nn]->to_string().c_str(),inv[nn]->get_schema_ref());
}
printf("\n");
}
}
*/

	if(create_failed) return(1);

//		Here, link up the query nodes, then verify that the
//		structure is acceptable (single root, no cycles, no stranded
//		nodes, etc.)
	create_failed = generate_linkage();
	if(create_failed) return -1;


//		Here, do optimizations such as predicate pushing,
//		join rearranging, etc.
//			Nothing to do yet.

/*
for(n=0;n<query_plan.size();++n){
if(query_plan[n] != NULL){
string nstr = query_plan[n]->get_node_name();
printf("B generate_plan, node %d is %s, reads from:\n\t",n,nstr.c_str());
vector<tablevar_t *> inv = query_plan[n]->get_input_tbls();
int nn;
for(nn=0;nn<inv.size();nn++){
printf("%s (%d) ",inv[nn]->to_string().c_str(),inv[nn]->get_schema_ref());
}
printf("\n");
}
}
printf("qhead=%d, qtail = ",qhead);
int nn;
for(nn=0;nn<qtail.size();++nn)
printf("%d ",qtail[nn]);
printf("\n");
*/

//		Collect query summaries.  The query is reperesented by its head node.
	query_name = query_plan[qhead]->get_node_name();
	attributes = query_plan[qhead]->get_fields();
//		TODO: The params and defines require a lot more thought.
	parameters = query_plan[qhead]->get_param_tbl();
	defines = query_plan[qhead]->get_definitions();

	return(0);
  };

void stream_query::add_output_operator(ospec_str *o){
	output_specs.push_back(o);
}


void stream_query::get_external_libs(set<string> &libset){

	int qn,i;
	for(qn=0;qn<query_plan.size();++qn){
		if(query_plan[qn] != NULL){
			vector<string> op_libs = query_plan[qn]->external_libs();
			for(i=0;i<op_libs.size();++i){
				libset.insert(op_libs[i]);
			}
		}
	}

	for(qn=0;qn<output_operators.size();++qn){
		if(output_operators[qn] != NULL){
			vector<string> op_libs = output_operators[qn]->external_libs();
			for(i=0;i<op_libs.size();++i){
				libset.insert(op_libs[i]);
			}
		}
	}
}



//	Split into LFTA, HFTA components.
//		Split this query into LFTA and HFTA queries.
//		Four possible outcomes:
//		1) the query reads from a protocol, but does not need to
//			split (can be evaluated as an LFTA).
//			The lfta query is the only element in the return vector,
//			and hfta_returned is false.
//		2) the query reads from no protocol, and therefore cannot be split.
//			THe hfta query is the only element in the return vector,
//			and hfta_returned is true.
//		3) reads from at least one protocol, but cannot be split : failure.
//			return vector is empty, the error conditions are written
//			in err_str and error_code
//		4) The query splits into an hfta query and one or more LFTA queries.
//			the return vector has two or more elements, and hfta_returned
//			is true.  The last element is the HFTA.

vector<stream_query *> stream_query::split_query(ext_fcn_list *Ext_fcns, table_list *Schema, bool &hfta_returned, ifq_t *ifdb, int n_virtual_ifaces, int hfta_parallelism, int hfta_idx){
   	vector<stream_query *> queries;
   	int l,q,s;
	int qp_hfta;

	hfta_returned = false;	// assume until proven otherwise

	for(l=0;l<qtail.size();++l){
		int leaf = qtail[l];


		vector<qp_node *> qnodes = query_plan[leaf]->split_node_for_fta(Ext_fcns, Schema, qp_hfta, ifdb, n_virtual_ifaces, hfta_parallelism, hfta_idx);


		if(qnodes.size() == 0 || query_plan[leaf]->get_error_code()){	// error
//printf("error\n");
			error_code = query_plan[leaf]->get_error_code();
			err_str = query_plan[leaf]->get_error_str();
   			vector<stream_query *> null_result;
			return(null_result);
		}
		if(qnodes.size() == 1 && qp_hfta){  // nothing happened
//printf("no change\n");
			query_plan[leaf] = qnodes[0];
		}
		if(qnodes.size() == 1 && !qp_hfta){	// push to lfta
//printf("lfta only\n");
			queries.push_back(new stream_query(qnodes[0], this));
			vector<int> succ = query_plan[leaf]->get_successors();
			for(s=0;s<succ.size();++s){
				query_plan[succ[s]]->remove_predecessor(leaf);
			}
			query_plan[leaf] = NULL;	// delete it?
		}
		if(qnodes.size() > 1){	// actual splitting occurred.
			if(!qp_hfta){		// internal consistency check.
				error_code = 1;
				err_str = "INTERNAL ERROR: mulitple nodes returned by split_node_for_fta, but none are hfta nodes.\n";
   				vector<stream_query *> null_result;
				return(null_result);
			}

			for(q=0;q<qnodes.size()-qp_hfta;++q){  // process lfta nodes
//printf("creating lfta %d (%s)\n",q,qnodes[q]->get_node_name().c_str());
				queries.push_back(new stream_query(qnodes[q], this));
			}
//					Use new hfta node
			query_plan[leaf] = qnodes[qnodes.size()-1];
//					Add in any extra hfta nodes
			for(q=qnodes.size()-qp_hfta;q<qnodes.size()-1;++q)
				query_plan.push_back(qnodes[q]);
		}
	}

    int n_ifprefs = 0;
	set<string> ifps;
	for(q=0;q<query_plan.size();++q){
		if(query_plan[q] != NULL){
			 n_ifprefs += query_plan[q]->count_ifp_refs(ifps);
			 hfta_returned = true;
		}
	}

	if(n_ifprefs){
		set<string>::iterator ssi;
		err_str += "ERROR, unresolved interface parameters in HFTA:\n";
		for(ssi=ifps.begin();ssi!=ifps.end();++ssi){
			err_str += (*ssi)+" ";
		}
		err_str += "\n";
		error_code = 3;
		vector<stream_query *> null_result;
		return(null_result);
	}


	if(hfta_returned){
		if(generate_linkage()){
			fprintf(stderr,"INTERNAL ERROR, generate_linkage failed in split_query.\n");
			exit(1);
		}
		queries.push_back(this);
	}

	return(queries);
}



vector<table_exp_t *> stream_query::extract_opview(table_list *Schema, vector<query_node *> &qnodes, opview_set &opviews, string silo_nm){
   	vector<table_exp_t *> subqueries;
   	int l,q;

	string root_name = this->get_output_tabledef()->get_tbl_name();


	for(l=0;l<qtail.size();++l){
		int leaf = qtail[l];
		vector<table_exp_t *> new_qnodes = query_plan[leaf]->extract_opview(Schema, qnodes, opviews, root_name, silo_nm);

		for(q=0;q<new_qnodes.size();++q){  // process lfta nodes
			subqueries.push_back( new_qnodes[q]);
		}
	}

	return(subqueries);
}




string stream_query::make_schema(){
	return make_schema(qhead);
}

string stream_query::make_schema(int q){
	string ret="FTA{\n\n";

	ret += query_plan[q]->get_fields()->to_string();

	ret += "DEFINE{\n";
	ret += "\tquery_name '"+query_plan[q]->get_node_name()+"';\n";

	map<string, string> defs = query_plan[q]->get_definitions();
	map<string, string>::iterator dfi;
	for(dfi=defs.begin(); dfi!=defs.end(); ++dfi){
		ret += "\t"+ (*dfi).first + " '" + (*dfi).second + "';\n";
	}
	ret += "}\n\n";

	ret += "PARAM{\n";
	param_table *params = query_plan[q]->get_param_tbl();
	vector<string> param_names = params->get_param_names();
	int p;
	for(p=0;p<param_names.size();p++){
		data_type *dt = params->get_data_type( param_names[p] );
		ret += "\t" + param_names[p] + " '" + dt->get_type_str() + "';\n";
	}
	ret += "}\n";

	ret += query_plan[q]->to_query_string();
	ret += "\n}\n\n";

	return(ret);

}

string stream_query::collect_refd_ifaces(){
	string ret="";
	int q;
	for(q=0;q<query_plan.size();++q){
		if(query_plan[q]){
			map<string, string> defs = query_plan[q]->get_definitions();
			if(defs.count("_referenced_ifaces")){
				if(ret != "") ret += ",";
				ret += defs["_referenced_ifaces"];
			}
		}
	}

	return ret;
}


bool stream_query::stream_input_only(table_list *Schema){
	vector<tablevar_t *> input_tbls = this->get_input_tables();
	int i;
	for(i=0;i<input_tbls.size();++i){
		int t = Schema->get_table_ref(input_tbls[i]->get_schema_name());
		if(Schema->get_schema_type(t) == PROTOCOL_SCHEMA) return(false);
	}
	return(true);
}

//		Return input tables.  No duplicate removal performed.
vector<tablevar_t *> stream_query::get_input_tables(){
	vector<tablevar_t *> retval;

//			create name-to-index map
	int n;
	map<string, int> name_to_node;
	for(n=0;n<query_plan.size();++n){
	  if(query_plan[n]){
		name_to_node[query_plan[n]->get_node_name()] = n;
	  }
	}

	int l;
	for(l=0;l<qtail.size();++l){
		int leaf = qtail[l];
		vector<tablevar_t *> tmp_v = query_plan[leaf]->get_input_tbls();
		int i;
		for(i=0;i<tmp_v.size();++i){
			if(name_to_node.count(tmp_v[i]->get_schema_name()) == 0)
				retval.push_back(tmp_v[i]);
		}
	}
	return(retval);
}


void stream_query::compute_node_format(int q, vector<int> &nfmt, map<string, int> &op_idx){
	int netcnt = 0, hostcnt = 0;
	int i;

	vector<tablevar_t *> itbls = query_plan[q]->get_input_tbls();
	for(i=0;i<itbls.size();++i){
		string tname = itbls[i]->get_schema_name();
		if(op_idx.count(tname)){
			int o = op_idx[tname];
			if(nfmt[o] == UNKNOWNFORMAT)
				compute_node_format(o,nfmt,op_idx);
			if(nfmt[o] == NETFORMAT) netcnt++;
			else	hostcnt++;
		}else{
			netcnt++;
		}
	}
	if(query_plan[q]->makes_transform()){
		nfmt[q] = HOSTFORMAT;
	}else{
		if(hostcnt>0){
			nfmt[q] = HOSTFORMAT;
		}else{
			nfmt[q] = NETFORMAT;
		}
	}
//printf("query plan %d (%s) is ",q,query_plan[q]->get_node_name().c_str());
//if(nfmt[q] == HOSTFORMAT) printf(" host format.\n");
//else printf("net format\n");
}


string stream_query::generate_hfta(table_list *Schema, ext_fcn_list *Ext_fcns, opview_set &opviews, bool distributed_mode){
	int schref, ov_ix, i, q, param_sz;
	bool dag_graph = false;


//		Bind the SEs in all query plan nodes to this schema, and
//			Add all tables used by this query to the schema.
//			Question: Will I be polluting the global schema by adding all
//			query node schemas?

//		First ensure all nodes are in the schema.
	int qn;
	for(qn=0;qn<query_plan.size();++qn){
		if(query_plan[qn] != NULL){
			Schema->add_table(query_plan[qn]->get_fields());
		}
	}
//		Now do binding.
	for(qn=0;qn<query_plan.size();++qn){
		if(query_plan[qn] != NULL){
			query_plan[qn]->bind_to_schema(Schema);
		}
	}

//		Is it a DAG plan?
	set<string> qsources;
	int n;
	for(n=0;n<query_plan.size();++n){
	  if(query_plan[n]){
		vector<tablevar_t *> tmp_v = query_plan[n]->get_input_tbls();
		int i;
		for(i=0;i<tmp_v.size();++i){
			if(qsources.count(tmp_v[i]->get_schema_name()) > 0)
				dag_graph = true;
			qsources.insert(tmp_v[i]->get_schema_name());
		}
	  }
	}



//		Collect set of tables ref'd in this HFTA
	set<int> tbl_set;
	for(qn=0;qn<query_plan.size();++qn){
	  if(query_plan[qn]){
//			get names of the tables
		vector<tablevar_t *> input_tbls = query_plan[qn]->get_input_tbls();
		vector<tablevar_t *> output_tbls = query_plan[qn]->get_output_tbls();
//			Convert to tblrefs, add to set of ref'd tables
		int i;
		for(i=0;i<input_tbls.size();i++){
//			int t = Schema->get_table_ref(input_tbls[i]->get_schema_name());
			int t = input_tbls[i]->get_schema_ref();
			if(t < 0){
				fprintf(stderr,"INTERNAL ERROR in generate_hfta. "
								"query plan node %s references input table %s, which is not in schema.\n",
								query_name.c_str(), input_tbls[i]->get_schema_name().c_str());
				exit(1);
			}
			tbl_set.insert(t);
		}

		for(i=0;i<output_tbls.size();i++){
			int t = Schema->get_table_ref(output_tbls[i]->get_schema_name());
			if(t < 0){
				fprintf(stderr,"INTERNAL ERROR in generate_hfta."
								"query plan node %s references output table %s, which is not in schema.\n",
								query_name.c_str(), output_tbls[i]->get_schema_name().c_str());
				exit(1);
			}
			tbl_set.insert(t);
		}
	  }
	}

//			Collect map of lftas, query nodes
	map<string, int> op_idx;
	for(q=0;q<query_plan.size();q++){
	  if(query_plan[q]){
		op_idx[query_plan[q]->get_node_name()] = q;
	  }
	}

//		map of input tables must include query id and input
//		source (0,1) becuase several queries might reference the same source
	vector<tablevar_t *> input_tbls = this->get_input_tables();
	vector<bool> input_tbl_free;
	for(i=0;i<input_tbls.size();++i){
		input_tbl_free.push_back(true);
	}
	map<string, int> lfta_idx;
//fprintf(stderr,"%d input tables, %d query nodes\n",input_tbls.size(), query_plan.size());
	for(q=0;q<query_plan.size();q++){
	  if(query_plan[q]){
		vector<tablevar_t *> itbls = query_plan[q]->get_input_tbls();
		int it;
		for(it=0;it<itbls.size();it++){
			string tname = itbls[it]->get_schema_name()+"-"+int_to_string(q)+"-"+int_to_string(it);
			string src_tblname = itbls[it]->get_schema_name();
			bool src_is_external = false;
			for(i=0;i<input_tbls.size();++i){
				if(src_tblname == input_tbls[i]->get_schema_name()){
				  src_is_external = true;
				  if(input_tbl_free[i]){
					lfta_idx[tname] = i;
					input_tbl_free[i] = false;
//fprintf(stderr,"Adding %s (src_tblname=%s, q=%d, it=%d) to %d.\n",tname.c_str(), src_tblname.c_str(), q, it, i);
					break;
				  }
				}
			}
			if(i==input_tbls.size() && src_is_external){
				fprintf(stderr,"INTERNAL ERROR in stream_query::generate_hfta, can't find free entry in input_tbls for query %d, intput %d (%s)\n",q,it,src_tblname.c_str());
				exit(1);
			}
		}
	  }
	}
/*
	for(i=0;i<input_tbls.size();++i){
		string src_tblname = input_tbls[i]->get_schema_name();
		lfta_idx[src_tblname] = i;
	}
*/

//		Compute the output formats of the operators.
	vector<int> node_fmt(query_plan.size(),UNKNOWNFORMAT);
	compute_node_format(qhead, node_fmt, op_idx);


//		Generate the schema strings for the outputs.
	string schema_str;
	for(i=0;i<query_plan.size();++i){
		if(i != qhead && query_plan[i]){
			string schema_tmpstr = this->make_schema(i);
			schema_str += "gs_csp_t node"+int_to_string(i)+"_schema = "+make_C_embedded_string(schema_tmpstr)+";\n";
		}
	}

	attributes = query_plan[qhead]->get_fields();


	string schema_tmpstr = this->make_schema();
	schema_str += "gs_csp_t "+generate_schema_string_name(query_name)+" = "+make_C_embedded_string(schema_tmpstr)+";\n";

//		Generate the collection of tuple defs.

	string tuple_defs = "\n/*\tDefine tuple structures \t*/\n\n";
	set<int>::iterator si;
	for(si=tbl_set.begin(); si!=tbl_set.end(); ++si){
		tuple_defs += generate_host_tuple_struct( Schema->get_table( (*si) ));
		tuple_defs += "\n\n";
	}

//		generate the finalize tuple function
	string finalize_str = generate_hfta_finalize_tuple(attributes);

//		Analyze and make the output operators
	bool eat_input = false;
	string src_op = query_name;
	string pred_op = src_op;
	int last_op = -1;
	if(output_specs.size()>0)
		eat_input = true;
	if(n_successors>0)
		eat_input = false;	// must create stream output for successor HFTAs.
	int n_filestreams = 0;
	for(i=0;i<output_specs.size();++i){
		if(output_specs[i]->operator_type == "stream" ){
			eat_input = false;
		}
		if(output_specs[i]->operator_type == "file" || output_specs[i]->operator_type == "zfile" ){
			last_op = i;
			n_filestreams++;
		}
	}
	int filestream_id = 0;
	for(i=0;i<output_specs.size();++i){
		if(output_specs[i]->operator_type == "file" || output_specs[i]->operator_type == "zfile"){

			int n_fstreams = output_specs[i]->n_partitions / n_parallel;
			if(n_fstreams * n_parallel < output_specs[i]->n_partitions){
				n_fstreams++;
				if(n_parallel == 1 || query_name.find("__copy1") != string::npos){
					fprintf(stderr,"WARNING, in query %s, %d streams requested for %s output, but it must be a multiple of the hfta parallelism (%d), increasing number of output streams to %d.\n",query_name.c_str(), output_specs[i]->n_partitions,  output_specs[i]->operator_type.c_str(), n_parallel, n_fstreams*n_parallel);
				}
			}
//			output_file_qpn *new_ofq = new output_file_qpn();
			string filestream_tag = "";
			if(n_filestreams>1){
				filestream_tag = "_fileoutput"+int_to_string(filestream_id);
				filestream_id++;
			}
			output_file_qpn *new_ofq = new output_file_qpn(pred_op, src_op, filestream_tag, query_plan[qhead]->get_fields(), output_specs[i], (i==last_op ? eat_input : false) );
//			if(n_fstreams > 1){
			if(n_fstreams > 0){
				string err_str;
				bool err_ret = new_ofq->set_splitting_params(n_parallel,parallel_idx,n_fstreams,output_specs[i]->partitioning_flds,err_str);
				if(err_ret){
					fprintf(stderr,"%s",err_str.c_str());
					exit(1);
				}
			}
			output_operators.push_back(new_ofq );
			pred_op = output_operators.back()->get_node_name();
		}else if(! (output_specs[i]->operator_type == "stream" || output_specs[i]->operator_type == "Stream" || output_specs[i]->operator_type == "STREAM") ){
			fprintf(stderr,"WARNING, output operator type %s (on query %s) is not supported, ignoring\n",output_specs[i]->operator_type.c_str(),query_name.c_str() );
		}
	}



//		Generate functors for the query nodes.

	string functor_defs = "\n/*\tFunctor definitions\t*/\n\n";
	for(qn=0;qn<query_plan.size();++qn){
		if(query_plan[qn]!=NULL){
//				Compute whether the input needs a ntoh xform.
			vector<bool> needs_xform;
			vector<tablevar_t *> itbls = query_plan[qn]->get_input_tbls();
			for(i=0;i<itbls.size();++i){
				string tname = itbls[i]->get_schema_name();
//				if(query_plan[qn]->makes_transform()){
					if(op_idx.count(tname)>0){
						if(node_fmt[ op_idx[tname] ] == NETFORMAT){
							needs_xform.push_back(true);
						}else{
							needs_xform.push_back(false);
						}
					}else{
						needs_xform.push_back(true);
					}
//				}else{
//					if(op_idx.count(tname)>0){
//						if(node_fmt[qn] != node_fmt[ op_idx[tname] ]){
//							needs_xform.push_back(true);
//						}else{
//							needs_xform.push_back(false);
//						}
//					}else{
//						if(node_fmt[qn] == HOSTFORMAT){
//							needs_xform.push_back(true);
//						}else{
//							needs_xform.push_back(false);
//						}
//					}
//				}
			}

			functor_defs += query_plan[qn]->generate_functor(Schema, Ext_fcns, needs_xform);
		}
	}

//			Generate output operator functors

	vector<bool> needs_xform;
	for(i=0;i<output_operators.size();++i)
		functor_defs += output_operators[i]->generate_functor(Schema, Ext_fcns, needs_xform);

	string ret =
"extern \"C\" {\n"
"#include <lapp.h>\n"
"#include <fta.h>\n"
"#include <gshub.h>\n"
"#include <stdlib.h>\n"
"#include <stdio.h>\n"
"#include <limits.h>\n"
"}\n"
;
	if(dag_graph)
		ret +=
"#define PLAN_DAG\n"
;
	ret +=
"#include <schemaparser.h>\n"
"#include<hfta_runtime_library.h>\n"
"\n"
"#include <host_tuple.h>\n"
"#include <hfta.h>\n"
"#include <hfta_udaf.h>\n"
"#include <hfta_sfun.h>\n"
"\n"
//"#define MAXSCHEMASZ 16384\n"
"#include <stdio.h>\n\n"
;

//		Get include file for each of the operators.
//		avoid duplicate inserts.
	set<string> include_fls;
	for(qn=0;qn<query_plan.size();++qn){
		if(query_plan[qn] != NULL)
			include_fls.insert(query_plan[qn]->get_include_file());
	}
	for(i=0;i<output_operators.size();++i)
			include_fls.insert(output_operators[i]->get_include_file());
	set<string>::iterator ssi;
	for(ssi=include_fls.begin();ssi!=include_fls.end();++ssi)
		ret += (*ssi);

//		Add defines for hash functions
	ret +=
"\n"
"#define hfta_BOOL_to_hash(x) (x)\n"
"#define hfta_USHORT_to_hash(x) (x)\n"
"#define hfta_UINT_to_hash(x) (x)\n"
"#define hfta_IP_to_hash(x) (x)\n"
"#define hfta_IPV6_to_hash(x) ( (x.v[0]) ^ (x.v[1]) ^ (x.v[2]) ^ (x.v[3]))\n"
"#define hfta_INT_to_hash(x) (gs_uint32_t)(x)\n"
"#define hfta_ULLONG_to_hash(x) ( (( (x) >>32)&0xffffffff) ^ ((x)&0xffffffff) )\n"
"#define hfta_LLONG_to_hash(x) ( (( (x) >>32)&0xffffffff) ^ ((x)&0xffffffff) )\n"
"#define hfta_FLOAT_to_hash(x) ( (( ((gs_uint64_t)(x)) >>32)&0xffffffff) ^ (((gs_uint64_t)(x))&0xffffffff) )\n"
"\n"
;

//	ret += "#define SERIOUS_LFTA \""+input_tbls[0]->get_schema_name()+"\"\n";
	ret += "#define OUTPUT_HFTA \""+query_name+"\"\n\n";

//		HACK ALERT: I know for now that all query plans are
//		single operator plans, but while SPX and SGAH can use the
//		UNOP template, the merge operator must use the MULTOP template.
//		WORSE HACK ALERT : merge does not translate its input,
//		so don't apply finalize to the output.
//		TODO: clean this up.

//	string node_type = query_plan[0]->node_type();

	ret += schema_str;
	ret += tuple_defs;

//			Need to work on the input, output xform logic.
//			For now, just add it in.
//	ret += finalize_str;

	if(node_fmt[qhead] == NETFORMAT){
		ret +=
"void finalize_tuple(host_tuple &tup){\n"
"return;\n"
"}\n"
"\n";
	}else{
		ret += finalize_str;
	}

	ret += functor_defs;

//			Parameter block management
//			The proper parameter block must be transmitted to each
//			external stream source.
//			There is a 1-1 mapping between the param blocks returned
//			by this list and the registered data sources ...
//			TODO: make this more manageable, but for now
//			there is no parameter block manipulation so I just
//			need to have the same number.

	ret +=
"int get_lfta_params(gs_int32_t sz, void * value,list<param_block>& lst){\n"
"		// for now every  lfta receive the full copy of hfta parameters\n"
"        struct param_block pb;\n";

	set<string> lfta_seen;
	for(i=0;i<input_tbls.size();++i){
		string src_tblname = input_tbls[i]->get_schema_name();
		if(lfta_seen.count(src_tblname) == 0){
		  lfta_seen.insert(src_tblname);
		  schref = input_tbls[i]->get_schema_ref();
		  if(Schema->get_schema_type(schref) == OPERATOR_VIEW_SCHEMA){
			ov_ix = input_tbls[i]->get_opview_idx();
			opview_entry *opv = opviews.get_entry(ov_ix);
			string op_param = "SUBQ:";
			int q;
			for(q=0;q<opv->subq_names.size();++q){
				if(q>0) op_param+=",";
				op_param+=opv->subq_names[q];
			}
			op_param+="\\n";
			param_sz = op_param.size()-1;

			sprintf(tmpstr,"\t\tpb.block_length = %d;\n",param_sz); ret+=tmpstr;
			ret+=
"        pb.data = malloc(pb.block_length);\n";
			ret+="\t\tmemcpy(pb.data,\""+op_param+"\",pb.block_length);\n"
"        lst.push_back(pb);\n\n";
		  }else{
			ret+=
"        pb.block_length = sz;\n"
"        pb.data = malloc(pb.block_length);\n"
"        memcpy(pb.data, value, pb.block_length);\n"
"        lst.push_back(pb);\n\n";
		  }
		}
	}

	ret +=
"        return 0;\n"
"}\n"
"\n";

		ret+=
"struct FTA* alloc_hfta (struct FTAID ftaid, gs_uint32_t reusable, gs_int32_t command, gs_int32_t sz, void * value ) {\n"
"\n"
"	// find the lftas\n"
"	list<lfta_info*> *lfta_list = new list<lfta_info*>;\n"
"\n"
"	FTAID f;\n"
"	char schemabuf[MAXSCHEMASZ];\n"
"	gs_schemahandle_t schema_handle;\n"
"\n";

//		Register the input data sources.
//		Register a source only once.

//	vector<string> ext_reg_txt;
	map<string, int> input_tbl_srcid;
	for(i=0;i<input_tbls.size();++i){
		string src_tblname = input_tbls[i]->get_schema_name();
//			Use UDOP alias when in distributed mode.
//			the cluster manager will make the translation
//			using infr from qtree.xml
		if(distributed_mode && input_tbls[i]->get_udop_alias() != "")
			src_tblname = input_tbls[i]->get_udop_alias();
	if(input_tbl_srcid.count(src_tblname) == 0){
		int srcid = input_tbl_srcid.size();
		input_tbl_srcid[src_tblname] = srcid;
		string tmp_s=
"\n	// find "+src_tblname+"\n"
"	if (fta_find(\""+src_tblname+"\",1,&f,schemabuf,MAXSCHEMASZ)!=0) {\n"
"			    fprintf(stderr,\"HFTA::error:could not find LFTA \\n\");\n"
"			    return 0;\n"
"	}\n"
"	//fprintf(stderr,\"HFTA::FTA found at %u[%u]\\n\",ftamsgid,ftaindex);\n"
"\n"
"	// parse the schema and get the schema handle\n"
"	schema_handle = ftaschema_parse_string(schemabuf);\n"
"	lfta_info* inf"+int_to_string(srcid)+" = new lfta_info();\n"
"	inf"+int_to_string(srcid)+"->f = f;\n"
"	inf"+int_to_string(srcid)+"->fta_name = strdup(\""+src_tblname+"\");\n"
"	inf"+int_to_string(srcid)+"->schema = strdup(schemabuf);\n"
"	inf"+int_to_string(srcid)+"->schema_handle = schema_handle;\n"
"	lfta_list->push_back(inf"+int_to_string(srcid)+");\n\n";
//		ext_reg_txt.push_back(tmp_s);
		ret += tmp_s;
	  }
	}

	ret+="\n";
	ret += "\tgs_schemahandle_t root_schema_handle = ftaschema_parse_string("+generate_schema_string_name(query_name)+");\n";
	for(i=0;i<query_plan.size();++i){
		if(i != qhead && query_plan[i]){
			ret += "\tgs_schemahandle_t op"+int_to_string(i)+"_schema_handle = ftaschema_parse_string(node"+int_to_string(i)+"_schema);\n";
		}
	}
	ret+="\n";

//		Create the operators.
	int n_basic_ops;
	for(q=0;q<query_plan.size();q++){
	  if(query_plan[q]){
		ret+=
"\n"
"	// create an instance of operator "+int_to_string(q)+" ("+query_plan[q]->get_node_name()+")	\n";

//			Create parameters for operator construction.
		string op_params;
		vector<tablevar_t *> itbls = query_plan[q]->get_input_tbls();
		string tname = itbls[0]->get_schema_name();
//		string li_tname = tname +"-"+int_to_string(q)+"-0";
//		if(lfta_idx.count(li_tname)>0)
		if(input_tbl_srcid.count(tname)>0){
//			ret += ext_reg_txt[lfta_idx[li_tname]];
//			op_params += "inf"+int_to_string( lfta_idx[li_tname] )+"->schema_handle";
			op_params += "inf"+int_to_string( input_tbl_srcid[tname] )+"->schema_handle";
		}else if(op_idx.count(tname)>0){
			op_params += "op"+int_to_string( op_idx[tname] )+"_schema_handle";
		}else{
			fprintf(stderr,"INTERNAL ERROR, can't identify input table %s when creating operator (3) %d (%s)\n",tname.c_str(),q,query_plan[q]->get_node_name().c_str());
			exit(1);
		}
		if(itbls.size()>1){
			string tname = itbls[1]->get_schema_name();
//			string li_tname = tname +"-"+int_to_string(q)+"-1";
//			if(lfta_idx.count(li_tname)>0)
			if(input_tbl_srcid.count(tname)>0){
//				ret += ext_reg_txt[lfta_idx[li_tname]];
//				op_params += ",inf"+int_to_string( lfta_idx[li_tname] )+"->schema_handle";
				op_params += ",inf"+int_to_string( input_tbl_srcid[tname] )+"->schema_handle";
			}else if(op_idx.count(tname)>0){
				op_params += ",op"+int_to_string( op_idx[tname] )+"_schema_handle";
			}else{
				fprintf(stderr,"INTERNAL ERROR, can't identify input table %s when creating operator (4) %d (%s)\n",tname.c_str(),q,query_plan[q]->get_node_name().c_str());
				exit(1);
			}
		}
		ret += query_plan[q]->generate_operator(q,op_params);
		ret +=
"	operator_node* node"+int_to_string(q)+" = new operator_node(op"+int_to_string(q)+");\n";

		n_basic_ops = q;
	  }
	}
	n_basic_ops++;
//			Next for the output operators if any
	for(i=0;i<output_operators.size();++i){
		ret += output_operators[i]->generate_operator(n_basic_ops+i,"root_schema_handle");
		ret +=
"	operator_node* node"+int_to_string(n_basic_ops+i)+" = new operator_node(op"+int_to_string(n_basic_ops+i)+");\n";
	}


//		Link up operators.
	for(q=0;q<query_plan.size();++q){
	  if(query_plan[q]){
//			NOTE: this code assume that the operator has at most
//			two inputs.  But the template code also makes
//			this assumption.  Both will need to be changed.
		vector<tablevar_t *> itbls = query_plan[q]->get_input_tbls();
		string tname = itbls[0]->get_schema_name();
//		string li_tname = tname +"-"+int_to_string(q)+"-0";
//		if(lfta_idx.count(li_tname)>0)
		if(input_tbl_srcid.count(tname)>0){
//			ret += "\tnode"+int_to_string(q)+"->set_left_lfta(inf"+int_to_string( lfta_idx[li_tname] )+");\n";
			ret += "\tnode"+int_to_string(q)+"->set_left_lfta(inf"+int_to_string( input_tbl_srcid[tname] )+");\n";
		}else if(op_idx.count(tname)>0){
			ret += "\tnode"+int_to_string(q)+"->set_left_child_node(node"+int_to_string( op_idx[tname] )+");\n";
		}else{
			fprintf(stderr,"INTERNAL ERROR, can't identify input table %s when linking operator (1) %d (%s)\n",tname.c_str(),q,query_plan[q]->get_node_name().c_str());
			exit(1);
		}
		if(itbls.size()>1){
			string tname = itbls[1]->get_schema_name();
//			string li_tname = tname +"-"+int_to_string(q)+"-1";
//			if(lfta_idx.count(li_tname)>0)
			if(input_tbl_srcid.count(tname)>0){
//				ret += "\tnode"+int_to_string(q)+"->set_right_lfta(inf"+int_to_string( lfta_idx[li_tname] )+");\n";
				ret += "\tnode"+int_to_string(q)+"->set_right_lfta(inf"+int_to_string( input_tbl_srcid[tname] )+");\n";
			}else if(op_idx.count(tname)>0){
				ret += "\tnode"+int_to_string(q)+"->set_right_child_node(node"+int_to_string( op_idx[tname] )+");\n";
			}else{
				fprintf(stderr,"INTERNAL ERROR, can't identify input table %s (%s) when linking operator (2) %d (%s)\n",tname.c_str(), tname.c_str(),q,query_plan[q]->get_node_name().c_str());
				exit(1);
			}
		}
	  }
	}
	for(i=0;i<output_operators.size();++i){
		if(i==0)
			ret += "\tnode"+int_to_string(n_basic_ops)+"->set_left_child_node(node"+int_to_string( qhead )+");\n";
		else
			ret += "\tnode"+int_to_string(n_basic_ops+i)+"->set_left_child_node(node"+int_to_string( n_basic_ops+i-1 )+");\n";
	}

	// FTA is reusable if reusable option is set explicitly or it doesn't have any parameters

	bool fta_reusable = false;
	if (query_plan[qhead]->get_val_of_def("reusable") == "yes" ||
		query_plan[qhead]->get_param_tbl()->size() == 0) {
		fta_reusable = 1;
	}

	int root_node = qhead;
	if(output_operators.size()>0)
		root_node = n_basic_ops+i-1;

	ret+=
"\n"
"\n"
"	MULTOP_HFTA* ftap = new MULTOP_HFTA(ftaid, OUTPUT_HFTA, command, sz, value, root_schema_handle, node"+int_to_string(root_node)+", lfta_list, " + (fta_reusable ?"true":"false") + ", reusable);\n"
"	if(ftap->init_failed()){ delete ftap; return 0;}\n"
"	return (FTA*)ftap;\n"
"}\n"
"\n"
"\n";


	string comm_bufsize = "16*1024*1024";
	if(defines.count("hfta_comm_buf")>0){
		comm_bufsize = defines["hfta_comm_buf"];
	}

	ret+=
"\n"
"int main(int argc, char * argv[]) {\n"
"\n"
"\n"
"   /* parse the arguments */\n"
"\n"
"    gs_int32_t tip1,tip2,tip3,tip4;\n"
"    endpoint gshub;\n"
"    gs_sp_t instance_name;\n"
"    if (argc<3) {\n"
"                gslog(LOG_EMERG,\"Wrong arguments at startup\");\n"
"        exit(1);\n"
"    }\n"
"\n"
"    if ((sscanf(argv[1],\"%u.%u.%u.%u:%hu\",&tip1,&tip2,&tip3,&tip4,&(gshub.port)) != 5)) {\n"
"        gslog(LOG_EMERG,\"HUB IP NOT DEFINED\");\n"
"        exit(1);\n"
"    }\n"
"    gshub.ip=htonl(tip1<<24|tip2<<16|tip3<<8|tip4);\n"
"    gshub.port=htons(gshub.port);\n"
"    instance_name=strdup(argv[2]);\n"
"    if (set_hub(gshub)!=0) {\n"
"        gslog(LOG_EMERG,\"Could not set hub\");\n"
"        exit(1);\n"
"    }\n"
"    if (set_instance_name(instance_name)!=0) {\n"
"        gslog(LOG_EMERG,\"Could not set instance name\");\n"
"        exit(1);\n"
"    }\n"
"\n"
"\n"
"    /* initialize host library  */\n"
"\n"
//"    fprintf(stderr,\"Initializing gscp\\n\");\n"
"    gsopenlog(argv[0]);\n"
"\n"
"    if (hostlib_init(HFTA, "+comm_bufsize+", DEFAULTDEV, 0, 0)!=0) {\n"
"        fprintf(stderr,\"%s::error:could not initialize gscp\\n\",\n"
"		argv[0]);\n"
"        exit(1);\n"
"    }\n"
"\n"
"\n"
"\n"
"    FTAID ret = fta_register(OUTPUT_HFTA, " + (fta_reusable?"1":"0") + ", DEFAULTDEV, alloc_hfta, "+generate_schema_string_name(query_name)+", -1, 0ull);\n"
"    fta_start_service(-1);\n"
"\n"
"   return 0;\n"
"\n"
"}\n"
"\n";
////////////////////
	return(ret);
}


//		Checks if the node i is compatible with interface partitioning
//		(can be pushed below the merge that combines partitioned stream)
//		i - index of the query node in a query plan
bool stream_query::is_partn_compatible(int index, map<string, int> lfta_names, vector<string> interface_names, vector<string> machine_names, ifq_t *ifaces_db, partn_def_list_t *partn_parse_result) {
	int i;
	qp_node* node = query_plan[index];
	qp_node* child_node = NULL;

	if (node->predecessors.empty())
		return false;

	// all the node predecessors must be partition merges with the same partition definition
	partn_def_t* partn_def = NULL;
	for (i = 0; i < node->predecessors.size(); ++i) {
		child_node = query_plan[node->predecessors[i]];
		if (child_node->node_type() != "mrg_qpn")
			return false;

		// merge must have only one parent for this optimization to work
		// check that all its successors are the same
		for (int j = 1; j < child_node->successors.size(); ++j) {
			if (child_node->successors[j] != child_node->successors[0])
				return false;
		}

		partn_def_t* new_partn_def = ((mrg_qpn*)child_node)->get_partn_definition(lfta_names, interface_names, machine_names, ifaces_db, partn_parse_result);
		if (!new_partn_def)
			return false;
		if (!i)
			partn_def = new_partn_def;
		else if (new_partn_def != partn_def)
			return false;

	}

	if (node->node_type() == "spx_qpn")	// spx nodes are always partition compatible
		return true;
	else if (node->node_type() == "sgah_qpn") {
		gb_table gb_tbl = ((sgah_qpn*)node)->gb_tbl;
		return true; //partn_def->is_compatible(&gb_tbl);
	}
	else if (node->node_type() == "rsgah_qpn") {
		gb_table gb_tbl = ((rsgah_qpn*)node)->gb_tbl;
		return partn_def->is_compatible(&gb_tbl);
	}
	else if (node->node_type() == "sgahcwcb_qpn") {
		gb_table gb_tbl = ((sgahcwcb_qpn*)node)->gb_tbl;
		return partn_def->is_compatible(&gb_tbl);
	}
	else if (node->node_type() == "join_eq_hash_qpn") {
		return true;
	}
	else
		return false;
}

//		Push the operator below the merge that combines
void stream_query::pushdown_partn_operator(int index) {
	qp_node* node = query_plan[index];
	int i;
	char node_index[128];

//	fprintf(stderr, "Partn pushdown, query %s of type %s\n", node->get_node_name().c_str(), node->node_type().c_str());


	// HACK ALERT: When reordering merges we screw up slack computation
	// since slack should no longer be used, it is not an issue


	// we can safely reorder nodes that have one and only one temporal atribute in select list
	table_def* table_layout = node->get_fields();
	vector<field_entry*> fields = table_layout->get_fields();
	int merge_fieldpos = -1;

	data_type* dt;
	for (i = 0; i < fields.size(); ++i) {
		data_type dt(fields[i]->get_type(),fields[i]->get_modifier_list());
		if(dt.is_temporal()) {
			if (merge_fieldpos != -1)	// more that one temporal field found
				return;
			merge_fieldpos = i;
		}
	}

	if (merge_fieldpos == -1)	// if no temporal fieldf found
		return;

	std::vector<colref_t *> mvars;			// the merge-by columns.

	// reodring procedure is different for unary operators and joins
	if (node->node_type() == "join_eq_hash_qpn") {
		vector<qp_node*> new_nodes;

		tablevar_t *left_table_name;
		tablevar_t *right_table_name;
		mrg_qpn* left_mrg = (mrg_qpn*)query_plan[node->predecessors[0]];
		mrg_qpn* right_mrg = (mrg_qpn*)query_plan[node->predecessors[1]];

		// for now we will only consider plans where both child merges
		// merge the same set of streams

		if (left_mrg->fm.size() != right_mrg->fm.size())
			return;

		// maping of interface names to table definitions
		map<string, tablevar_t*> iface_map;
		for (i = 0; i < left_mrg->fm.size(); i++) {
			left_table_name = left_mrg->fm[i];
			iface_map[left_table_name->get_machine() + left_table_name->get_interface()] = left_table_name;
		}

		for (i = 0; i < right_mrg->fm.size(); i++) {
			right_table_name = right_mrg->fm[i];

			// find corresponding left tabke
			if (!iface_map.count(right_table_name->get_machine() + right_table_name->get_interface()))
				return;

			left_table_name = iface_map[right_table_name->get_machine() + right_table_name->get_interface()];

			// create new join nodes
			sprintf(node_index, "_%d", i);
			join_eq_hash_qpn* new_node = (join_eq_hash_qpn*)node->make_copy(node_index);

			// make a copy of right_table_name
			right_table_name = right_table_name->duplicate();
			left_table_name->set_range_var(((join_eq_hash_qpn*)node)->from[0]->get_var_name());
			right_table_name->set_range_var(((join_eq_hash_qpn*)node)->from[1]->get_var_name());
			new_node->from[0] = left_table_name;
			new_node->from[1] = right_table_name;
			new_nodes.push_back(new_node);
		}

		// make right_mrg a new root
		right_mrg->set_node_name(node->get_node_name());
		right_mrg->table_layout = table_layout;
		right_mrg->merge_fieldpos = merge_fieldpos;

		for (i = 0; i < right_mrg->fm.size(); i++) {
			// make newly create joins children of merge
			sprintf(node_index, "_%d", i);
			right_mrg->fm[i] = new tablevar_t(right_mrg->fm[i]->get_machine().c_str(), right_mrg->fm[i]->get_interface().c_str(), (node->get_node_name() + string(node_index)).c_str());
			sprintf(node_index, "_m%d", i);
			right_mrg->fm[i]->set_range_var(node_index);
			right_mrg->mvars[i]->set_field(table_layout->get_field_name(merge_fieldpos));

		}

		if (left_mrg != right_mrg)
			query_plan[node->predecessors[0]] = NULL;	// remove left merge from the plan

		query_plan.insert(query_plan.end(), new_nodes.begin(), new_nodes.end());

	} else {	// unary operator
		// get the child merge node
		mrg_qpn* child_mrg = (mrg_qpn*)query_plan[node->predecessors[0]];

		child_mrg->set_node_name(node->get_node_name());
		child_mrg->table_layout = table_layout;
		child_mrg->merge_fieldpos = merge_fieldpos;

		// create new nodes for every source stream
		for (i = 0; i < child_mrg->fm.size(); i++) {
			tablevar_t *table_name = child_mrg->fm[i];
			sprintf(node_index, "_%d", i);
			qp_node* new_node = node->make_copy(node_index);

			if (node->node_type() == "spx_qpn")
				((spx_qpn*)new_node)->table_name = table_name;
			else if (node->node_type() == "sgah_qpn")
				((sgah_qpn*)new_node)->table_name = table_name;
			else if (node->node_type() == "rsgah_qpn")
				((rsgah_qpn*)new_node)->table_name = table_name;
			else if (node->node_type() == "sgahcwcb_qpn")
				((sgahcwcb_qpn*)new_node)->table_name = table_name;
			table_name->set_range_var("_t0");

			child_mrg->fm[i] = new tablevar_t(table_name->get_machine().c_str(), table_name->get_interface().c_str(), new_node->get_node_name().c_str());
			sprintf(node_index, "_m%d", i);
			child_mrg->fm[i]->set_range_var(node_index);
			child_mrg->mvars[i]->set_field(table_layout->get_field_name(merge_fieldpos));

			// add new node to query plan
			query_plan.push_back(new_node);
		}
	}
	query_plan[index] = NULL;
	generate_linkage();
}


//		Checks if the node i can be pushed below the merge
  bool stream_query::is_pushdown_compatible(int index, map<string, int> lfta_names, vector<string> interface_names, vector<string> machine_names) {

	int i;
	qp_node* node = query_plan[index];
	qp_node* child_node = NULL;

	if (node->predecessors.size() != 1)
		return false;

	// node predecessor must be merge that combine streams from multiple hosts
	child_node = query_plan[node->predecessors[0]];
	if (child_node->node_type() != "mrg_qpn")
		return false;

	if (!((mrg_qpn*)child_node)->is_multihost_merge())
		return false;

	// merge must have only one parent for this optimization to work
	// check that all its successors are the same
	for (int j = 1; j < child_node->successors.size(); ++j) {
		if (child_node->successors[j] != child_node->successors[0])
			return false;
	}

	// selections can always be pushed down, aggregations can always be split into selection/aggr or aggr/aggr pair
	// and pushed down
	if (node->node_type() == "spx_qpn")
		return true;
	else if (node->node_type() == "sgah_qpn")
		return true;
	else
		return false;

  }

//		Push the operator below the merge
  void stream_query::pushdown_operator(int index, ext_fcn_list *Ext_fcns, table_list *Schema) {
	qp_node* node = query_plan[index];
	int i;
	char node_suffix[128];

	// we can only safely push down queries that have one and only one temporal atribute in select list
	table_def* table_layout = node->get_fields();
	vector<field_entry*> fields = table_layout->get_fields();
	int merge_fieldpos = -1;

	data_type* dt;
	for (i = 0; i < fields.size(); ++i) {
		data_type dt(fields[i]->get_type(),fields[i]->get_modifier_list());
		if(dt.is_temporal()) {
			if (merge_fieldpos != -1)	// more that one temporal field found
				return;
			merge_fieldpos = i;
		}
	}

	if (merge_fieldpos == -1)	// if no temporal field found
		return;

	std::vector<colref_t *> mvars;			// the merge-by columns.

	fprintf(stderr, "Regular pushdown, query %s of type %s\n", node->get_node_name().c_str(), node->node_type().c_str());

	// get the child merge node
	mrg_qpn* child_mrg = (mrg_qpn*)query_plan[node->predecessors[0]];

	tablevar_t *table_name = NULL;


	if (node->node_type() == "spx_qpn") {
		// get the child merge node

		// create new nodes for every source stream
		for (i = 0; i < child_mrg->fm.size(); i++) {
			table_name = child_mrg->fm[i];
			sprintf(node_suffix, "_%d", i);
			qp_node* new_node = node->make_copy(node_suffix);

			((spx_qpn*)new_node)->table_name = table_name;
			table_name->set_range_var("_t0");

			child_mrg->fm[i] = new tablevar_t(table_name->get_machine().c_str(), table_name->get_interface().c_str(), new_node->get_node_name().c_str());
			sprintf(node_suffix, "_m%d", i);
			child_mrg->fm[i]->set_range_var(node_suffix);
			child_mrg->mvars[i]->set_field(table_layout->get_field_name(merge_fieldpos));

			// add new node to query plan
			query_plan.push_back(new_node);
		}
		child_mrg->table_layout = table_layout;
		child_mrg->merge_fieldpos = merge_fieldpos;

	} else {		// aggregation node

		vector<qp_node*> new_nodes;

		// split aggregations into high and low-level part
		vector<qp_node *> split_nodes = ((sgah_qpn*)node)->split_node_for_hfta(Ext_fcns, Schema);
		if (split_nodes.size() != 2)
			return;

		sgah_qpn* super_aggr = (sgah_qpn*)split_nodes[1];
		super_aggr->table_name = ((sgah_qpn*)node)->table_name;

		// group all the sources by host
		map<string, vector<int> > host_map;
		for (i = 0; i < child_mrg->fm.size(); i++) {
			tablevar_t *table_name = child_mrg->fm[i];
			if (host_map.count(table_name->get_machine()))
				host_map[table_name->get_machine()].push_back(i);
			else {
				vector<int> tables;
				tables.push_back(i);
				host_map[table_name->get_machine()] = tables;
			}
		}

		// create a new merge and low-level aggregation for each host
		map<string, vector<int> >::iterator iter;
		for (iter = host_map.begin(); iter != host_map.end(); iter++) {
			string host_name = (*iter).first;
			vector<int> tables = (*iter).second;

			sprintf(node_suffix, "_%s", host_name.c_str());
			string suffix(node_suffix);
			untaboo(suffix);
			mrg_qpn *new_mrg = (mrg_qpn *)child_mrg->make_copy(suffix);
			for (i = 0; i < tables.size(); ++i) {
				sprintf(node_suffix, "_m%d", i);
				new_mrg->fm.push_back(child_mrg->fm[tables[i]]);
				new_mrg->mvars.push_back(child_mrg->mvars[i]);
				new_mrg->fm[i]->set_range_var(node_suffix);
			}
			qp_node* new_node = split_nodes[0]->make_copy(suffix);

			if (new_node->node_type() == "spx_qpn")  {
				((spx_qpn*)new_node)->table_name = new tablevar_t(host_name.c_str(), "IFACE", new_mrg->get_node_name().c_str());
				((spx_qpn*)new_node)->table_name->set_range_var("_t0");
			} else {
				((sgah_qpn*)new_node)->table_name = new tablevar_t(host_name.c_str(), "IFACE", new_mrg->get_node_name().c_str());
				((sgah_qpn*)new_node)->table_name->set_range_var("_t0");
			}
			query_plan.push_back(new_mrg);
			new_nodes.push_back(new_node);
		}

		child_mrg->merge_fieldpos = merge_fieldpos;
		if (split_nodes[0]->node_type() == "spx_qpn")
			child_mrg->table_layout = create_attributes(child_mrg->get_node_name(), ((spx_qpn*)split_nodes[0])->select_list);
		else
			child_mrg->table_layout = create_attributes(child_mrg->get_node_name(), ((sgah_qpn*)split_nodes[0])->select_list);


		// connect newly created nodes with parent multihost merge
		for (i = 0; i < new_nodes.size(); ++i) {
			if (new_nodes[i]->node_type() == "spx_qpn")
				child_mrg->fm[i] = new tablevar_t(((spx_qpn*)new_nodes[i])->table_name->get_machine().c_str(), "IFACE", new_nodes[i]->get_node_name().c_str());
			else {
				child_mrg->fm[i] = new tablevar_t(((sgah_qpn*)new_nodes[i])->table_name->get_machine().c_str(), "IFACE", new_nodes[i]->get_node_name().c_str());

			}
			child_mrg->mvars[i]->set_field(child_mrg->table_layout->get_field_name(merge_fieldpos));

			sprintf(node_suffix, "_m%d", i);
			child_mrg->fm[i]->set_range_var(node_suffix);
			query_plan.push_back(new_nodes[i]);
		}
		child_mrg->fm.resize(new_nodes.size());
		child_mrg->mvars.resize(new_nodes.size());

		// push the new high-level aggregation
		query_plan.push_back(super_aggr);

	}
	query_plan[index] = NULL;
	generate_linkage();

  }

//		Extract subtree rooted at node i into separate hfta
stream_query* stream_query::extract_subtree(int index) {

	vector<int> nodes;
	stream_query* new_query = new stream_query(query_plan[index], this);

	nodes.push_back(index);
	for (int i = 0; i < nodes.size(); ++i) {
		qp_node* node = query_plan[nodes[i]];
		if (!node)
			continue;

		// add all children to nodes list
		for (int j = 0; j < node->predecessors.size(); ++j)
			nodes.push_back(node->predecessors[j]);
		if (i)
			new_query->query_plan.push_back(node);

		query_plan[nodes[i]] = NULL;
	}

	return new_query;
}

//		Splits query that combines data from multiple hosts into separate hftas.
vector<stream_query*> stream_query::split_multihost_query()  {

	vector<stream_query*> ret;
	char node_suffix[128];
	int i;

	// find merges combining multiple hosts into per-host groups
	int plan_size = query_plan.size();
	vector<mrg_qpn*> new_nodes;

	for (i = 0; i < plan_size; ++i) {
		qp_node* node = query_plan[i];
		if (node && node->node_type() == "mrg_qpn") {
			mrg_qpn* mrg = (mrg_qpn*)node;
			if (mrg->is_multihost_merge()) {

				// group all the sources by host
				map<string, vector<int> > host_map;
				for (int j = 0; j < mrg->fm.size(); j++) {
					tablevar_t *table_name = mrg->fm[j];
					if (host_map.count(table_name->get_machine()))
						host_map[table_name->get_machine()].push_back(j);
					else {
						vector<int> tables;
						tables.push_back(j);
						host_map[table_name->get_machine()] = tables;
					}
				}

				// create a new merge for each host
				map<string, vector<int> >::iterator iter;
				for (iter = host_map.begin(); iter != host_map.end(); iter++) {
					string host_name = (*iter).first;
					vector<int> tables = (*iter).second;

					if (tables.size() == 1)
						continue;

					sprintf(node_suffix, "_%s", host_name.c_str());
					string suffix(node_suffix);
					untaboo(suffix);
					mrg_qpn *new_mrg = (mrg_qpn *)mrg->make_copy(suffix);
					for (int j = 0; j < tables.size(); ++j) {
						new_mrg->fm.push_back(mrg->fm[tables[j]]);
						new_mrg->mvars.push_back(mrg->mvars[j]);
						sprintf(node_suffix, "m_%d", j);
						new_mrg->fm[j]->set_range_var(node_suffix);
					}
					new_nodes.push_back(new_mrg);
				}

				if (!new_nodes.empty()) {
					// connect newly created merge nodes with parent multihost merge
					for (int j = 0; j < new_nodes.size(); ++j) {
						mrg->fm[j] = new tablevar_t(new_nodes[j]->fm[0]->get_machine().c_str(), "IFACE", new_nodes[j]->get_node_name().c_str());
						query_plan.push_back(new_nodes[j]);
					}
					mrg->fm.resize(new_nodes.size());
					mrg->mvars.resize(new_nodes.size());
					generate_linkage();
				}


				// now externalize the sources
				for (int j = 0; j < node->predecessors.size(); ++j) {
					//		Extract subtree rooted at node i into separate hfta
					stream_query* q = extract_subtree(node->predecessors[j]);
					if (q) {
						q->generate_linkage();
						ret.push_back(q);
					}

				}
				generate_linkage();
			}
		}
	}

	return ret;
}


//		Perform local FTA optimizations
void stream_query::optimize(vector<stream_query *>& hfta_list, map<string, int> lfta_names, vector<string> interface_names, vector<string> machine_names, ext_fcn_list *Ext_fcns, table_list *Schema, ifq_t *ifaces_db, partn_def_list_t *partn_parse_result){

	// Topologically sort the nodes in query plan (leaf-first)
	int i = 0, j = 0;
	vector<int> sorted_nodes;

	int num_nodes = query_plan.size();
	bool* leaf_flags = new bool[num_nodes];
	memset(leaf_flags, 0, num_nodes * sizeof(bool));

	// run topological sort
	bool done = false;

	// add all leafs to sorted_nodes
	while (!done) {
		done = true;
		for (i = 0; i < num_nodes; ++i) {
			if (!query_plan[i])
				continue;

			if (!leaf_flags[i] && query_plan[i]->predecessors.empty()) {
				leaf_flags[i] = true;
				sorted_nodes.push_back(i);
				done = false;

				// remove the node from its parents predecessor lists
				// since we only care about number of predecessors, it is sufficient just to remove
				// one element from the parent's predecessors list
				for (int j = query_plan[i]->successors.size() - 1; j >= 0; --j)
					query_plan[query_plan[i]->successors[j]]->predecessors.pop_back();
			}
		}
	}
	delete[] leaf_flags;
	num_nodes = sorted_nodes.size();
	generate_linkage();		// rebuild the recently destroyed predecessor lists.

	// collect the information about interfaces nodes read from
	for (i = 0; i < num_nodes; ++i) {
		qp_node* node = query_plan[sorted_nodes[i]];
		vector<tablevar_t *> input_tables = node->get_input_tbls();
		for (j = 0; j <  input_tables.size(); ++j) {
			tablevar_t * table = input_tables[j];
			if (lfta_names.count(table->schema_name)) {
				int index = lfta_names[table->schema_name];
				table->set_machine(machine_names[index]);
				table->set_interface(interface_names[index]);
			}
		}
	}

	/*

	// push eligible operators down in the query plan
	for (i = 0; i < num_nodes; ++i) {
		if (partn_parse_result && is_partn_compatible(sorted_nodes[i], lfta_names, interface_names, machine_names, ifaces_db, partn_parse_result)) {
			pushdown_partn_operator(sorted_nodes[i]);
		} else if (is_pushdown_compatible(sorted_nodes[i], lfta_names, interface_names, machine_names)) {
			pushdown_operator(sorted_nodes[i], Ext_fcns, Schema);
		}
	}

	// split the query into multiple hftas if it combines the data from multiple hosts
	vector<stream_query*> hftas = split_multihost_query();
	hfta_list.insert(hfta_list.end(), hftas.begin(), hftas.end());
	*/

	num_nodes = query_plan.size();
	// also split multi-way merges into two-way merges
	for (i = 0; i < num_nodes; ++i) {
		qp_node* node = query_plan[i];
		if (node && node->node_type() == "mrg_qpn") {
			vector<mrg_qpn *> split_merge = ((mrg_qpn *)node)->split_sources();

			query_plan.insert(query_plan.end(), split_merge.begin(), split_merge.end());
			// delete query_plan[sorted_nodes[i]];
			query_plan[i] = NULL;
		}
	}

	generate_linkage();
}



table_def *stream_query::get_output_tabledef(){
	return( query_plan[qhead]->get_fields() );
}



//////////////////////////////////////////////////////////
////		De-siloing.  TO BE REMOVED

void stream_query::desilo_lftas(map<string, int> &lfta_names,vector<string> &silo_names,table_list *Schema){
	int i,t,s;

	int suffix_len = silo_names.back().size();

	for(i=0;i<qtail.size();++i){
		vector<tablevar_t *> itbls = query_plan[qtail[i]]->get_input_tbls();
		for(t=0;t<itbls.size();++t){
			string itbl_name = itbls[t]->get_schema_name();
			if(lfta_names.count(itbl_name)>0){
//printf("Query %s input %d references lfta input %s\n",query_plan[qtail[i]]->get_node_name().c_str(),t,itbl_name.c_str());
				vector<string> src_names;
				string lfta_base = itbl_name.substr(0,itbl_name.size()-suffix_len);
				for(s=0;s<silo_names.size();++s){
					string lfta_subsilo = lfta_base + silo_names[s];
//printf("\t%s\n",lfta_subsilo.c_str());
					src_names.push_back(lfta_subsilo);
				}
				string merge_node_name = "desilo_"+query_plan[qtail[i]]->get_node_name()+
					"_input_"+int_to_string(t);
				mrg_qpn *merge_node = new mrg_qpn(merge_node_name,src_names,Schema);
				int m_op_pos = Schema->add_table(merge_node->table_layout);
				itbls[t]->set_schema(merge_node_name);
				itbls[t]->set_schema_ref(m_op_pos);
				query_plan.push_back(merge_node);
			}
		}
	}

	generate_linkage();
}
////////////////////////////////////////
///		End de-siloing



//			Given a collection of LFTA stream queries,
//			extract their WHERE predicates
//			and pass them to an analysis routine which will extract
//			common elements
//
void get_common_lfta_filter(std::vector<stream_query *> lfta_list,table_list *Schema,ext_fcn_list *Ext_fcns, vector<cnf_set *> &prefilter_preds, set<unsigned int> &pred_ids){
	int s;
	std::vector< std::vector<cnf_elem *> > where_list;

//		still safe to assume that LFTA queries have a single
//		query node, which is at position 0.
	for(s=0;s<lfta_list.size();++s){
		vector<cnf_elem *> cnf_list = lfta_list[s]->query_plan[0]->get_filter_clause();
		if(lfta_list[s]->query_plan[0]->node_type() == "sgah_qpn"){
			gb_table *gtbl = ((sgah_qpn *)(lfta_list[s]->query_plan[0]))->get_gb_tbl();
			int c;
			for(c=0;c<cnf_list.size();++c){
				insert_gb_def_pr(cnf_list[c]->pr,gtbl);
			}
		}
		where_list.push_back(lfta_list[s]->query_plan[0]->get_filter_clause());
	}

	find_common_filter(where_list,Schema,Ext_fcns,prefilter_preds, pred_ids);
}





//			Given a collection of LFTA stream queries,
//			extract the union of all temporal attributes referenced in select clauses
//			those attributes will need to be unpacked in prefilter
//
void get_prefilter_temporal_cids(std::vector<stream_query *> lfta_list, col_id_set &temp_cids){
	int s, sl;
	vector<scalarexp_t *> sl_list;
	gb_table *gb_tbl = NULL;


//		still safe to assume that LFTA queries have a single
//		query node, which is at position 0.
	for(s=0;s<lfta_list.size();++s){

		if(lfta_list[s]->query_plan[0]->node_type() == "spx_qpn"){
			spx_qpn *spx_node = (spx_qpn *)lfta_list[s]->query_plan[0];
			sl_list = spx_node->get_select_se_list();
		}
		if(lfta_list[s]->query_plan[0]->node_type() == "sgah_qpn"){
			sgah_qpn *sgah_node = (sgah_qpn *)lfta_list[s]->query_plan[0];
			sl_list = sgah_node->get_select_se_list();
			gb_tbl = sgah_node->get_gb_tbl();
		}

		for(sl=0;sl<sl_list.size();sl++){
			data_type *sdt = sl_list[sl]->get_data_type();
			if (sdt->is_temporal()) {
				gather_se_col_ids(sl_list[sl],temp_cids, gb_tbl);
			}
		}
	}
}


