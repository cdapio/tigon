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
		bison --verbose --defines=res.tab.cc.h -p ResParser  -o res.tab.cc res.y

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

#include "xml_parser.h"
#include "app.h"
#include "iface_q.h"
#include "qtree.h"

std::vector<std::string> res_attr_vec;
std::vector<std::string> res_val_vec;
std::string res_a, res_v;

extern xml_parser* parser;



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
	start_tag res_list end_tag
	| start_tag end_tag
	| '<' NAME opt_val_list '/' '>'
		{parser->startElement($2, res_attr_vec, res_val_vec);
		parser->endElement($2);
		}
	;

start_tag:
	'<' NAME opt_val_list '>'	{parser->startElement($2, res_attr_vec, res_val_vec);}
	;

end_tag:
	'<' '/' NAME '>'	{parser->endElement($3);}
	;


res_list:
	resource
	| res_list resource
	;

opt_val_list:
	val_list
	|		{res_attr_vec.clear(); res_val_vec.clear();}
	;

val_list:
	val		{res_attr_vec.clear();  res_attr_vec.push_back(res_a);
			 res_val_vec.clear(); res_val_vec.push_back(res_v); }
	| val_list val {res_attr_vec.push_back(res_a);
					res_val_vec.push_back(res_v); }
	;

val:
	NAME '=' STRING_TOKEN	{res_a = $1; res_v = $3;}
	;
	




%%

