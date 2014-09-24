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
#ifndef __IFACE_Q_DEFINED_
#define __IFACE_Q_DEFINED_

#include <stdio.h>
//#include "xmlparse.h"
#include<vector>
#include<string>
#include<map>
#include<set>
#include<algorithm>

#include"parse_fta.h"


//			Represent an interface.
class iface_t{
public:
	int lineno;
	std::map<std::string, std::vector<std::string> > vals;

	iface_t(int l){
		lineno = l;
	}

	void add_property(const char *name, const char *att);
	void add_property(const char *name, const char **atts);
	void add_property(const char *name, std::vector<std::string> &val_vec);

	std::vector<std::string> get_property(std::string p){
		std::vector<std::string> ret;

	/* convert property name to lcase */
	std::transform(p.begin(), p.end(), p.begin(), (int(*)(int))std::tolower);

/*
printf("get_property %s :\n",p.c_str());
std::map<std::string, std::vector<std::string> >::iterator msvsi;
for(msvsi=vals.begin(); msvsi!=vals.end();++msvsi){
  printf("\t%s : ",(*msvsi).first.c_str());
  std::vector<std::string>  vs = (*msvsi).second;
  int i;
  for(i=0;i<vs.size();++i){
    printf(" %s",vs[i].c_str());
  }
  printf("\n");
}
*/
		if(vals.count(p)) return vals[p];
		else return ret;
	}

	std::vector<std::string> get_properties(){
		std::vector<std::string> ret;

		std::map<std::string, std::vector<std::string> >::iterator iter;
		for (iter = vals.begin(); iter != vals.end(); iter++) {
			ret.push_back((*iter).first);
		}
		return ret;
	}

	int finalize(std::string &errs);
	std::string to_string();
	std::string get_name();
	std::string get_host();
	bool eval_Contains(std::string prop, std::string val);
	bool eval_Equals(std::string prop, std::string val);
	bool eval_Exists(std::string prop);
};

//			Represent an interface.
class gs_host_t{
public:
	int lineno;
	std::map<std::string, std::vector<std::string> > vals;

	gs_host_t(int l){
		lineno = l;
	}

	void add_property(const char *name, const char *att);
	void add_property(const char *name, std::vector<std::string> &val_vec);
	std::vector<std::string> get_property(std::string p){
		std::vector<std::string> ret;
		if(vals.count(p)) return vals[p];
		else return ret;
	}
	int finalize(std::string &errs);

	void add_interface(iface_t* it) {ifaces.push_back(it);}

	void propagate_name() {
		for(int i=0;i<ifaces.size();i++)
			ifaces[i]->add_property("host", vals["name"]);
	}

	std::string to_string();
	std::string get_name();

	std::vector<iface_t *> ifaces;
};



//		A collection of resources (interfaces)

class resparse_data{
public:
	std::vector<std::string> level;
	bool in_Resources, in_Iface, in_Host;
	iface_t *curr_iface;
	gs_host_t *curr_host;
	bool failure;
    std::vector<iface_t *> ifaces;
	std::vector<gs_host_t *> hosts;
	bool use_live_hosts_file;
	std::set<std::string> *live_hosts;
	bool distributed_mode;

//	XML_Parser parser;

//	resparse_data(XML_Parser p){
//		parser = p;
	resparse_data(bool use_live_hosts_file, std::set<std::string> *live_hosts, bool distributed_mode){
		in_Resources = false;
		in_Iface = false;
		curr_iface = NULL;
		failure = false;
		this->use_live_hosts_file = use_live_hosts_file;
		this->live_hosts = live_hosts;
		this->distributed_mode = distributed_mode;
	};

	void new_iface(int l){
		curr_iface = new iface_t(l);
	};
	void new_host(int l){
		curr_host = new gs_host_t(l);
	};
	void add_property(const char *name, const char *att){
		curr_iface->add_property(name, att);
	};
	void add_property(const char *name, const char **atts){
		curr_iface->add_property(name, atts);
	};
	void add_property(const char *name, std::vector<std::string> &val_vec){
		curr_iface->add_property(name, val_vec);
	};


	std::vector<std::string> get_property(int i, std::string property){
		return ifaces[i]->get_property(property);
	};
	std::vector<std::string> get_properties(int i){
		return ifaces[i]->get_properties();
	};

	int finalize_iface(std::string &errs);
	int finalize_host(std::string &errs);
	bool failed(){return failure;};
	std::string to_string();
	std::vector<std::pair<std::string,std::string> > find_ifaces(predicate_t *pr);
	std::vector<int> get_ifaces_by_Name(std::string host_name, std::string if_name);
private:
	bool eval_pred(int i, predicate_t *pr);
};


class ifq_t{
public:
	resparse_data *rpd;
	std::map<std::string, predicate_t *> ifq_map;

	ifq_t(){ rpd = NULL;};

	int load_ifaces(std::string fname, bool use_live_hosts_file, bool distributed_mode, std::string &err);
	int load_ifqs(std::string fname, std::string &err);

	std::vector<std::pair<std::string,std::string> > eval(std::string qname, int &errnbr);
	std::vector<std::string> get_iface_vals(std::string host_name, std::string if_name, std::string property, int &errnbr, std::string &err_str);
	std::vector<std::string> get_iface_properties(std::string host_name, std::string basic_if_name, int &err_no, std::string &err_str);
};


/////////////////////////
//	for resource parser

void startElement(void *userData, const char *name, std::vector<std::string> &attr_vec, std::vector<std::string> &val_vec);
void endElement(void *userData, const char *name) ;




#endif
