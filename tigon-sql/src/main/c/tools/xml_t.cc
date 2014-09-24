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

#include "xml_t.h"

using namespace std;

xml_t::xml_t(tag_t *s, xml_list_t *xl,const char *e){
	name=s->name;
	attribs = s->nvp_list;
	subtrees = xl->xlist;
	end_tag_name = e;
}

void xml_t::get_roots(string type, vector<xml_t *> &ret){
	if(name==type){
		ret.push_back(this);
	}else{
		int i;
		for(i=0;i<subtrees.size();++i){
			subtrees[i]->get_roots(type,ret);
		}
	}
}

void xml_t::get_roots(set<string> type, vector<xml_t *> &ret){
	if(type.count(name)>0){
		ret.push_back(this);
	}else{
		int i;
		for(i=0;i<subtrees.size();++i){
			subtrees[i]->get_roots(type,ret);
		}
	}
}
