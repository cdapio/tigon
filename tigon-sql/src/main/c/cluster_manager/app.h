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

#ifndef __APP_H
#define __APP_H

#include "cluster_manager.h"
#include "xml_parser.h"
#include <netinet/in.h>

class app_parser;

class app_t {
public:
	int lineno;
	std::map<std::string, std::vector<std::string> > vals;

	app_t(int l){
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


class app_db{
private:
	std::vector<app_t*> app_list;
	app_t* curr_app;
	bool failure;
	app_parser* appl_parser;
public:
	int load_apps(std::string fname, std::string &err);

	int finalize_app(string &errs);

	std::vector<app_node*> get_apps();
};

/////////////////////////
//	for resource parser

class app_parser : public xml_parser {

public:
	std::vector<std::string> level;
	bool in_Applications, in_App;
	app_t *curr_app;
	bool failure;
	std::vector<app_t *> apps;

	void new_app(int l){
		curr_app = new app_t(l);
	};
	void add_property(const char *name, const char *att){
		curr_app->add_property(name, att);
	};
	void add_property(const char *name, const char **atts){
		curr_app->add_property(name, atts);
	};
	void add_property(const char *name, std::vector<std::string> &val_vec){
		curr_app->add_property(name, val_vec);
	};


	std::vector<std::string> get_property(int i, std::string property){
		return apps[i]->get_property(property);
	};
	int finalize_app(std::string &errs);
	bool failed(){return failure;};
	std::string to_string();

public:
	app_parser() : xml_parser() {
		in_Applications = false;
		in_App = false;
		curr_app = NULL;
		failure = false;
	}
	virtual void startElement(const char *name, std::vector<std::string> &attr_vec, std::vector<std::string> &val_vec);
	virtual void endElement(const char *name) ;
};

#endif // __APP_H
