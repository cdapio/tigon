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
  bison --verbose --defines=nic.tab.cc.h -p NicParser  -o nic.tab.cc nic.y

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

#include "nic_def.h"

extern int NicParserlex();
void NicParsererror(char *s);


extern nic_property *nicp;
extern std::vector<std::string> nic_attr_vec;
extern std::vector<std::string> nic_val_vec;
extern std::string nic_a, nic_v;



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
%token FUNC TYPES UNARY_OPS BINARY_OPS FIELDS OPTIONS




%%

parse_result:
	opts types unary_ops binary_ops fields fcns 
	;

opts:
	OPTIONS opt_val_list ';' {nicp->opta=nic_attr_vec; nicp->optv=nic_val_vec;}
	;

types:
	TYPES  opt_attr_list ';' {nicp->typea=nic_attr_vec;}
	;

unary_ops:
	UNARY_OPS  opt_attr_list ';' {nicp->opua=nic_attr_vec;}
	;

binary_ops:
	BINARY_OPS  opt_attr_list ';' {nicp->opba=nic_attr_vec;}
	;

fields:
	FIELDS  opt_attr_list ';' {nicp->fieldsa=nic_attr_vec;}
	;

fcns:
	FUNC  opt_attr_list ';' {nicp->funca=nic_attr_vec;}
	;

opt_attr_list:
	attr_list
	|		{nic_attr_vec.clear(); }
	;

attr_list:
	NAME		{nic_attr_vec.clear();  nic_attr_vec.push_back($1); }
	| STRING_TOKEN	{nic_attr_vec.clear();  nic_attr_vec.push_back($1); }
	| attr_list ',' NAME {nic_attr_vec.push_back($3);}
	| attr_list ',' STRING_TOKEN {nic_attr_vec.push_back($3);}
	;

opt_val_list:
	val_list
	|		{nic_attr_vec.clear(); nic_val_vec.clear();}
	;

val_list:
	val		{nic_attr_vec.clear();  nic_attr_vec.push_back(nic_a);
			 nic_val_vec.clear(); nic_val_vec.push_back(nic_v); }
	| val_list ',' val {nic_attr_vec.push_back(nic_a);
					nic_val_vec.push_back(nic_v); }
	;

val:
	NAME '=' STRING_TOKEN 	{nic_a = $1; nic_v = $3;}
	| NAME '=' NAME 	{nic_a = $1; nic_v = $3;}
	;
	




%%

