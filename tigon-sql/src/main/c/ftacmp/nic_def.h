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
#ifndef _FTA_NIC_DEF_H_INCLUDED__
#define _FTA_NIC_DEF_H_INCLUDED__

#include<vector>
#include<string>
#include<map>

class nic_property{
public:
	std::vector<std::string> opta;	// option args
	std::vector<std::string> optv;	// val of option args
	std::vector<std::string> typea;	// legal types 
	std::vector<std::string> opba;  // legal binary ops
	std::vector<std::string> opua;  // legal unary ops
	std::vector<std::string> fieldsa; // illegal fields
	std::vector<std::string> funca;  // legal functions

	nic_property(){};

	bool legal_type(std::string t);
	bool legal_unary_op(std::string t);
	bool legal_binary_op(std::string t);
	bool illegal_field(std::string t);
	bool option_exists(std::string t);
	std::string option_value(std::string t);
};


nic_property *load_nic_property(std::string dir, std::string name);


class nic_prop_db{
public:
	std::string dir;
	std::map<std::string, nic_property *> ndb;

	nic_prop_db(std::string d){dir=d;};

	
	nic_property *get_nic_property(std::string nics, int &err_no){
		if(ndb.count(nics))
			return ndb[nics];
		nic_property *ret = load_nic_property(dir, nics);
		if(ret)
			ndb[nics] = ret;
		return ret;
	};


	std::string get_nic_prop_val(std::string nics, std::string prop,int &err_no){
		nic_property *nicp = this->get_nic_property(nics, err_no);
		if(!nicp){
			fprintf(stderr,"ERROR cannot load nic properties %s for nic type %s in get_nic_prop_val\n",nics.c_str(), prop.c_str());
				exit(1);
		}
		if(nicp->option_exists(prop)){
			return nicp->option_value("Return");
		}else{
			return "";
		}

	}
};


void NicParser_setfileinput(FILE *f);



#endif
