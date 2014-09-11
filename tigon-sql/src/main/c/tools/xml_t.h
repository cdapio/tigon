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

#ifndef __XML_T_DEFINED_
#define __XML_T_DEFINED_

#include <stdio.h>
#include<vector>
#include<string>
#include<map>
#include<set>
#include<algorithm>


void xmlParser_setfileinput(FILE *f);
void xmlParser_setstringinput(char *s);



struct name_val_pair_t{
	std::string name;
	std::string val;

	name_val_pair_t(const char *n, const char *v){
		name = n;
		val = v;
	}
};

struct name_val_list_t{
	std::vector<name_val_pair_t *> nvp_list;

	name_val_list_t(){
	}

	name_val_list_t(name_val_pair_t *nvp){
		nvp_list.push_back(nvp);
	}

	name_val_list_t *append(name_val_pair_t *nvp){
		nvp_list.push_back(nvp);
		return this;
	}
};

struct tag_t{
	std::string name;
	std::vector<name_val_pair_t *> nvp_list;

	tag_t(const char *n, name_val_list_t *nvl){
		name=n;
		nvp_list = nvl->nvp_list;
	}
};


struct xml_list_t;

struct xml_t{
	std::string name;
	std::string end_tag_name;
	std::vector<name_val_pair_t *> attribs;

	std::vector<xml_t *> subtrees;

	xml_t(){}

	xml_t(tag_t *s, xml_list_t *xl,const char *e);

	xml_t(tag_t *s, char *e){
		name=s->name;
		attribs = s->nvp_list;
		end_tag_name = e;
	}

	xml_t(const char *n, name_val_list_t *nvl){
		name=n;
		attribs=nvl->nvp_list;
	}

	std::string to_string(){
		return to_string("");
	}
	std::string to_string(std::string indent){
		std::string ret;
		std::string whitespace;
		int i;

		ret += indent+"<"+name;
		for(i=0;i<attribs.size();++i)
			ret += " "+attribs[i]->name+"="+attribs[i]->val;

		if(subtrees.size()>0){
			ret+=">\n";
			for(i=0;i<subtrees.size();++i){
				ret += subtrees[i]->to_string(indent+"  ");
			}
			ret += indent + "</"+name+">\n";
		}else{
			ret+=">\n";
		}
		return ret;
	}

	void get_roots(std::string type, std::vector<xml_t *> &ret);
	void get_roots(std::set<std::string> type, std::vector<xml_t *> &ret);

	bool get_attrib_val(std::string name, std::string &val){
		val="";
		int i;
		for(i=0;i<attribs.size();++i){
			if(attribs[i]->name == name){
				val = attribs[i]->val;
				return true;
			}
		}
		return false;
	}

};

struct xml_list_t{
	std::vector<xml_t *> xlist;

	xml_list_t(xml_t *x){
		xlist.push_back(x);
	}

	xml_list_t *append(xml_t *x){
		xlist.push_back(x);
		return this;
	}
};

#endif

