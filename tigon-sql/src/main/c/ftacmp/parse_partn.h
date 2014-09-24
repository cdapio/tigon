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
#ifndef _PARTN_PARSE_H_INCLUDED__
#define _PARTN_PARSE_H_INCLUDED__

#include<stdio.h>
#include"parse_fta.h"
#include"analyze_fta.h"

/*		Interface to FTA Parser		*/
void PartnParser_setfileinput(FILE *f);
void PartnParser_setstringinput(char *s);

class partn_def_t{
public:
	std::string name;
	std::string protocol;
	std::vector<scalarexp_t *> se_list;
	std::vector<std::string> field_list;

	partn_def_t(const char *n, const char *p, se_list_t *sl){
		name=n;
		protocol=p;
		se_list = sl->se_list;
	}

	int verify(table_list *schema, std::string &err_msg);
	bool is_compatible(gb_table *gb_tbl);
};

class partn_def_list_t{
public:
	std::map<std::string, partn_def_t *> partn_map;

	partn_def_list_t(partn_def_t *pd){
		partn_map[pd->name] = pd;
	}
	partn_def_list_t *append(partn_def_t *pd){
		if(partn_map.count(pd->name)){
			fprintf(stderr,"Warning, duplicate entry named %s in partition definitions, ignored.\n",pd->name.c_str());
		}else{
			partn_map[pd->name] = pd;
		}
		return this;
	}

	partn_def_t *get_partn_def(std::string n){
		if(partn_map.count(n))
			return partn_map[n];
		return NULL;
	}

	int verify(table_list *schema, std::string &err_msg){
	  int retval=0;
	  std::map<std::string, partn_def_t *>::iterator mspi;
		for(mspi=partn_map.begin(); mspi!=partn_map.end();++mspi){
			if((*mspi).second->verify(schema, err_msg))
				retval=1;
		}
		return retval;
	}


};

#endif
