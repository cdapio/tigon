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

#ifndef __FIELD_LIST_H_
#define __FIELD_LIST_H_

#include<string>
#include<map>
#include"xml_t.h"


class field_list{
public:
	std::map<std::string, std::string> type_verifier;
	std::map<std::string, std::string> fields;

	field_list(xml_t *x);

	int verify_field(std::string name, std::string type, std::string &err){
		if(fields.count(name) == 0){
			err += "\tCould not find field "+name+" in the field list\n";
			return 1;
		}
		if(fields[name] == "")
			return 0;
		if(type_verifier[fields[name]] != type_verifier[type]){
			err += "\tfield "+name+" has type "+type+", which isn't compatible with field list type "+fields[name]+"\n";
			return 2;
		}
		return 0;
	}

}; 


#endif

