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

#ifndef __CLUSTER_H
#define __CLUSTER_H

#include <string>
using namespace std;

struct host_info {
        string host_name;
        bool online;    //indicates whether node is up
        string restart_command;
};


#include "cluster_manager.h"
#include "xml_parser.h"

class cluster_parser;


class cluster_node_t {
public:
	int lineno;
	std::map<std::string, std::vector<std::string> > vals;

	cluster_node_t(int l){
		lineno = l;
	}

	void add_property(const char *name, const char *att);
	void add_property(const char *name, const char **atts);
	void add_property(const char *name, std::vector<std::string> &val_vec);
	std::vector<std::string> get_property(std::string p){
		std::vector<std::string> ret;
		if(vals.count(p)) return vals[p];
		else return ret;
	}
	int finalize(std::string &errs);

	std::string to_string();

};


class cluster_db{
private:
	std::vector<host_info*> node_list;
	cluster_node_t* curr_node;
	bool failure;
	cluster_parser* clust_parser;
public:
	int load_cluster(std::string fname, std::string &err);

	int finalize_cluster_nodes(string &errs);

	std::vector<host_info*> get_cluster_nodes();
};

/////////////////////////
//	for resource parser

class cluster_parser : public xml_parser {

public:
	std::vector<std::string> level;
	bool in_Cluster, in_ClusterNode;
	cluster_node_t *curr_node;
	bool failure;
	std::vector<cluster_node_t *> nodes;

	void new_node(int l){
		curr_node = new cluster_node_t(l);
	};
	void add_property(const char *name, const char *att){
		curr_node->add_property(name, att);
	};
	void add_property(const char *name, const char **atts){
		curr_node->add_property(name, atts);
	};
	void add_property(const char *name, std::vector<std::string> &val_vec){
		curr_node->add_property(name, val_vec);
	};


	std::vector<std::string> get_property(int i, std::string property){
		return nodes[i]->get_property(property);
	};
	int finalize_node(std::string &errs);
	bool failed(){return failure;};
	std::string to_string();

public:
	cluster_parser() : xml_parser() {
		in_Cluster = false;
		in_ClusterNode = false;
		curr_node = NULL;
		failure = false;
	}
	virtual void startElement(const char *name, std::vector<std::string> &attr_vec, std::vector<std::string> &val_vec);
	virtual void endElement(const char *name) ;
};

#endif // __CLUSTER_H
