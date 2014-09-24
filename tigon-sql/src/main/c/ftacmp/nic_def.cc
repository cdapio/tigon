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

#include"nic_def.h"

extern int NicParserparse(void);
extern FILE *NicParserin;
extern int NicParserdebug;


nic_property *nicp;
std::vector<std::string> nic_attr_vec;
std::vector<std::string> nic_val_vec;
std::string nic_a, nic_v;

using namespace std;

nic_property *load_nic_property(std::string dir, std::string name){

	string fname;
	if(dir != "")
		fname = dir + "/nic_property_"+name+".def";
	else
		fname = "nic_property_"+name+".def";

	FILE *nicf = fopen(fname.c_str(),"r");
	if(! nicf){
		fprintf(stderr,"ERROR, can't open nic property file %s\n",fname.c_str());
		return NULL;
	}

NicParserdebug = 0;
	nicp = new nic_property();
	NicParser_setfileinput(nicf);
	if(NicParserparse()){
		fprintf(stderr,"could not parse nic property file %s\n",fname.c_str());
		return NULL;
	}
	
	return nicp;
}

bool nic_property::legal_type(string t){
int i;
	for(i=0;i<typea.size();++i){
		if(t==typea[i])
			return true;
	}
	return false;
}


bool nic_property::legal_unary_op(string t){
int i;
	for(i=0;i<opua.size();++i){
		if(t==opua[i])
			return true;
	}
	return false;
}



bool nic_property::legal_binary_op(string t){
int i;
	for(i=0;i<opba.size();++i){
		if(t==opba[i])
			return true;
	}
	return false;
}


bool nic_property::illegal_field(string t){
int i;
	for(i=0;i<fieldsa.size();++i){
		if(t==fieldsa[i])
			return true;
	}
	return false;
}



bool nic_property::option_exists(string t){
int i;
	for(i=0;i<opta.size();++i){
		if(t==opta[i])
			return true;
	}
	return false;
}


string nic_property::option_value(string t){
int i;
	for(i=0;i<opta.size();++i){
		if(t==opta[i])
			return optv[i];
	}
	return "";
}












