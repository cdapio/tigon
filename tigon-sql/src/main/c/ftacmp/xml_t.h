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


//			Represent leaf-level xml nodes
class xml_leaf_t{
public:
	std::string name;
	std::vector<std::string> attrs;
	std::vector<std::string> vals;

	xml_leaf_t(const char *n, std::vector<std::string> &a, std::vector<std::string> &v){
		name = n;
		attrs = a;
		vals = v;
	}
};

class xml_t{
public:
	std::vector<xml_leaf_t *> leaves;

	xml_t(){}

	void add_leaf(xml_leaf_t *l){
		leaves.push_back(l);
	}
};

#endif

