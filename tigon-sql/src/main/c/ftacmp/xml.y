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

/*
	MUST COMPILE WITH
	bison --verbose -d -p xmlParser  -o xml.tab.cc xml.y

	 (or equivalent).
*/

%{


#include <stdio.h>

/*		Some addn'l includes, necessary but not included by the
		bison generated code.
*/

#include <stdlib.h>

/*		prototypes for the parser callbacks.
*/

#include "xml_t.h"


extern std::vector<std::string> xml_attr_vec;
extern std::vector<std::string> xml_val_vec;
extern std::string xml_a, xml_v;
extern xml_t *xml_leaves;

extern int xmlParserdebug;
extern void xmlParsererror(char *s);
extern int xmlParserlex();


#define YYDEBUG 1

%}


	/* symbolic tokens */

%union {
	int intval;
	double floatval;
	char *strval;
	int subtok;

	/*			for FTA definition.	*/

}

%token <strval> NAME
%token <strval> STRING_TOKEN




%%

parse_result:	resource
	;

resource:
	start_tag xml_list end_tag
	| start_tag end_tag
	| '<' NAME opt_val_list '/' '>'
		{xml_leaves->add_leaf(new xml_leaf_t($2, xml_attr_vec, xml_val_vec));
		}
	;

start_tag:
	'<' NAME opt_val_list '>'	
	;

end_tag:
	'<' '/' NAME '>'	
	;


xml_list:
	resource
	| xml_list resource
	;

opt_val_list:
	val_list
	|		{xml_attr_vec.clear(); xml_val_vec.clear();}
	;

val_list:
	val		{xml_attr_vec.clear();  xml_attr_vec.push_back(xml_a);
			 xml_val_vec.clear(); xml_val_vec.push_back(xml_v); }
	| val_list val {xml_attr_vec.push_back(xml_a);
					xml_val_vec.push_back(xml_v); }
	;

val:
	NAME '=' STRING_TOKEN	{xml_a = $1; xml_v = $3;}
	;
	




%%


