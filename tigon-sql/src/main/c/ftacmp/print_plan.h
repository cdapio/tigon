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

#ifndef __PRINT_PLAN_H
#define __PRINT_PLAN_H


#include <stdio.h>
#include <vector>
#include <set>
#include <stdlib.h>
using namespace std;


int print_qp_node(FILE* fd, int i, stream_query* query, map<string, int>& lfta_names, map<string, int>& fta_names) {

	int j;

	qp_node* node = query->query_plan[i];

	string ntype = node->node_type();

	fprintf(fd, "\tNODE %s, type %s\n", node->node_name.c_str(), ntype.c_str());

	if (!node->predecessors.empty()) {
		for (j = 0; j < node->predecessors.size(); ++j)
			fprintf(fd, "\t\tCHILD_NODE %s\n", query->query_plan[query->query_plan[i]->predecessors[j]]->node_name.c_str());
	}



	// print input tables
	vector<tablevar_t *> input_tables = node->get_input_tbls();

	for (j = 0; j <  input_tables.size(); ++j) {
		tablevar_t * table = input_tables[j];

		if (lfta_names.count(table->schema_name))
			fprintf(fd, "\t\tSOURCE_LFTA %s\n", table->schema_name.c_str());
		else if (fta_names.count(table->schema_name))
			fprintf(fd, "\t\tSOURCE_HFTA %s\n", table->schema_name.c_str());
	}

	fprintf(fd, "\n");



	return 0;

}

int print_query(FILE* fd, stream_query* query, map<string, int>& lfta_names) {

	fprintf(fd, "HFTA %s\n", query->query_name.c_str());


	vector<tablevar_t *> tmp_tv = query->get_input_tables();
	int itv;
	map<string, int> fta_names;

	for(itv=0;itv<tmp_tv.size();++itv) {
		fta_names[tmp_tv[itv]->get_schema_name()] = 0;
	}

	for (int i = 0; i < query->query_plan.size(); ++i) {
		if (query->query_plan[i])
			print_qp_node(fd, i, query, lfta_names, fta_names);
	}

	return 0;

}

int print_hftas(const char* fname, vector<stream_query *>& hfta_list, map<string, int>& lfta_names, vector<string>& query_names, vector<string>& interface_names) {

	FILE* f = fopen(fname, "w");
	for (int i = 0; i < hfta_list.size(); ++i) {
		print_query(f, hfta_list[i], lfta_names);
	}


	for (int mi = 0; mi < query_names.size(); ++mi) {
		fprintf(f,"LFTA %s\n",query_names[mi].c_str());
		fprintf(f,"\tINTERFACE %s\n",interface_names[mi].c_str());

	}

	fclose(f);


	return 0;

}

#endif		// __PRINT_PLAN_H
