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

#ifndef __QTREE_H
#define __QTREE_H

#include "xml_parser.h"
#include <map>
#include <string>
#include <vector>
using namespace std;

struct qnode {
	enum qnode_type {
		QNODE_LFTA,
		QNODE_HFTA,
		QNODE_UDOP
	};
	string fta_name;
	qnode_type node_type;
	double rate;
	unsigned liveness_timeout;
	unsigned ip;	// ip address recommended by optimizer

	string host;		// only set in lftas
	string iface;		// only set in lftas
	string alias;		// only set in udops
	string filename;

	vector<qnode*> children;
	qnode* parent;	// parent node (NULL for top level ftas)

	qnode() { parent = NULL; }
};

extern vector<qnode*> top_fta_list;	// top level ftas
extern vector<qnode*> lfta_list;	// top level ftas
extern map<string, qnode*> qnode_map;


class qtree_parser;

class qnode_t {
public:
	int lineno;
	std::map<std::string, std::vector<std::string> > vals;

	qnode_t(int l){
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


class qtree_t {
private:
	std::vector<qnode_t*> qnode_list;
	qnode_t* curr_qnode;
	bool failure;
	qtree_parser* q_parser;
public:
	int load_qtree(std::string fname, std::string &err);

	int finalize_qnode(std::string &errs);

	std::vector<qnode*> get_qnodes();
};

/////////////////////////
//	for resource parser

class qtree_parser : public xml_parser {

public:
	std::vector<std::string> level;
	bool in_QueryNodes, in_Fta;
	qnode_t *curr_qnode;
	bool failure;
	std::vector<qnode_t *> qnodes;

	void new_qnode(int l){
		curr_qnode = new qnode_t(l);
	};
	void add_property(const char *name, const char *att){
		curr_qnode->add_property(name, att);
	};
	void add_property(const char *name, const char **atts){
		curr_qnode->add_property(name, atts);
	};
	void add_property(const char *name, std::vector<std::string> &val_vec){
		curr_qnode->add_property(name, val_vec);
	};


	std::vector<std::string> get_property(int i, std::string property){
		return qnodes[i]->get_property(property);
	};
	int finalize_qnode(std::string &errs);
	bool failed(){return failure;};
	std::string to_string();

public:
	qtree_parser() : xml_parser() {
		in_QueryNodes = false;
		in_Fta = false;
		curr_qnode = NULL;
		failure = false;
	}
	virtual void startElement(const char *name, std::vector<std::string> &attr_vec, std::vector<std::string> &val_vec);
	virtual void endElement(const char *name) ;
};

#endif // __QTREE_H
