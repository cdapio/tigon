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


extern xml_t *xml_result;

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
	struct name_val_pair_t *nvpt;
	struct name_val_list_t *nvpl;
	struct tag_t *tg_t;
	struct xml_list_t *xlist_t;
	struct xml_t *x_t;
}

%token <strval> NAME
%token <strval> STRING_TOKEN

%type <nvpt> val
%type <nvpl> val_list
%type <nvpl> opt_val_list
%type <strval> end_tag
%type <tg_t> start_tag
%type <xlist_t> xml_list
%type<x_t> resource


%%

parse_result:	resource	{xml_result = $1;}
	;

resource:
	start_tag xml_list end_tag {$$ = new xml_t($1,$2,$3);}
	| start_tag end_tag	{$$=new xml_t($1,$2);}
	| '<' NAME opt_val_list '/' '>'	{$$=new xml_t($2,$3);}
	;

start_tag:
	'<' NAME opt_val_list '>'	{$$ = new tag_t($2, $3);}
	;

end_tag:
	'<' '/' NAME '>'	{$$=$3;}
	;


xml_list:
	resource	{$$=new xml_list_t($1);}
	| xml_list resource	{$$=$1->append($2);}
	;

opt_val_list:
	val_list	{$$=$1;}
	|		{$$=new name_val_list_t();}
	;

val_list:
	val		{$$ = new name_val_list_t($1);}
	| val_list val {$$ = $1->append($2);}
	;

val:
	NAME '=' STRING_TOKEN	{$$ = new name_val_pair_t($1,$3);}
	;
	




%%


