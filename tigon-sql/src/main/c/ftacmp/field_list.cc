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
#include<stdio.h>
#include<stdlib.h>

#include<vector>
#include<string>
#include<map>

#include"xml_t.h"
#include"field_list.h"

using namespace std;

struct type_map_t{
	std::string from;
	std::string to;
};

static struct type_map_t type_map[] = {
	"BOOL", 			"BOOLEAN",
	"Bool", 			"BOOLEAN",
	"bool", 			"BOOLEAN",
	"BOOLEAN", 			"BOOLEAN",
	"INT",				"INT",
	"Int",				"INT",
	"int",				"INT",
	"INTEGER",			"INT",
	"INT(_huge_)",		"INT(_huge_)",
	"LLONG",			"INT(_huge_)",
	"Llong",			"INT(_huge_)",
	"llong",			"INT(_huge_)",
	"INTEGER(_huge_)",	"INT(_huge_)",
	"UINT",				"UINT",
	"Uint",				"UINT",
	"uint",				"UINT",
	"USHORT",			"UINT",
	"Ushort",			"UINT",
	"ushort",			"UINT",
	"UINT(_huge_)",		"UINT(_huge_)",
	"ULLONG",			"UINT(_huge_)",
	"Ullong",			"UINT(_huge_)",
	"ullong",			"UINT(_huge_)",
	"STR",				"STRING",
	"STRING",			"STRING",
	"FLT",				"FLOAT",
	"FLOAT",			"FLOAT",
	"Float",			"FLOAT",
	"float",			"FLOAT",
	"DATE",				"DATE",
	"CLOCK",			"CLOCK",
	"DATE_CLOCK",		"DATE_CLOCK",
	"TIME",				"TIME",
	"IP",				"IP2",
	"IP2",				"IP2",
	"STRING",			"STRING",
	"STR",				"STRING",
	"String",			"STRING",
	"string",			"STRING",
	"V_STR",			"STRING",
	"V_str",			"STRING",
	"v_str",			"STRING",
	"IPV6",				"IP6z",
	"IP6",				"IP6z",
	"IP6z",				"IP6z",
	"",					""
};


	
field_list::field_list(xml_t *x){
	int i,j,pos;
	for(i=0;type_map[i].from != ""; i++){
		type_verifier[type_map[i].from] = type_map[i].to;
	}
	type_verifier[""] = "";

	for(i=0;i<x->leaves.size();i++){
		if(x->leaves[i]->name != "field"){
			fprintf(stderr,"WARNING, unrecognized leaf element %s in field list file, ignoring.\n",x->leaves[i]->name.c_str());
			continue;
		}

		std::string fname, ftype;
		for(j=0;j<x->leaves[i]->attrs.size();j++){
			if(x->leaves[i]->attrs[j] == "name"){
				if(fname != ""){
					fprintf(stderr,"WARNING, found duplicate name property for a field (#%d, %s) in the field list, ignoring second name %s.\n",i,fname.c_str(), x->leaves[i]->vals[j].c_str());
				}else{
					fname = x->leaves[i]->vals[j];
				}
			}
		}
		if(fname == ""){
			fprintf(stderr,"WARNING, field %d has an empty name, ignoring\n",i);
			continue;
		}

		for(j=0;j<x->leaves[i]->attrs.size();j++){
			if(x->leaves[i]->attrs[j] == "type"){
				if(ftype != ""){
					fprintf(stderr,"WARNING, found duplicate type property for field %s in the field list, ignoring.\n",fname.c_str());
				}else{
					ftype = x->leaves[i]->vals[j];
				}
			}
		}

		if(fields.count(fname)){
			fprintf(stderr,"WARNING, duplicate field list name %s, ignoring repeat entry.\n",fname.c_str());
			continue;
		}
		if(type_verifier.count(ftype) == 0){
			fprintf(stderr,"WARNING, field %s in the field list has unrecognized type %s, ignoring the type.\n",fname.c_str(),ftype.c_str());
			ftype = "";
		}

		string::size_type ix = fname.rfind("/");
		if(ix != string::npos){
			fname = fname.substr(ix+1);
		}

		fields[fname] = ftype;
	}
}


