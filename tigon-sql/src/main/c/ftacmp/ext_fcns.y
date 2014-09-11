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
	bison --verbose --defines=ext_fcns.tab.cc.h -p Ext_fcnsParser -o ext_fcns.tab.cc ext_fcns.y
	 
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

#include "parse_ext_fcns.h"

/*		Interface to ext_fcns parser	*/
	int yyparse();
	void yyerror(char *s);
	int yylex();
extern int flex_ext_fcns_lineno, flex_ext_fcns_ch;

/*		Return value	*/
extern ext_fcn_list *Ext_fcns;

#define YYDEBUG 1

%}


	/* symbolic tokens */

%union {
	char* strval;
	ext_fcn_list *ext_fcn_list_t;
	ext_fcn_def *ext_fcn_t;
	ext_fcn_param_list *plist_t;
	param_list *tplist_t;
	ext_fcn_param *param_t;
	ext_fcn_modifier_list *modif_t;
	}

	
%token <strval> NAME
%token <strval> STRING_TOKEN
%token <strval> INTNUM


%type <ext_fcn_t> fcn_def 
%type <ext_fcn_list_t> fcn_list
%type <plist_t> param_commalist
%type <plist_t> opt_param_commalist
%type <tplist_t> type_param_commalist
%type <tplist_t> opt_type_param_commalist
%type <param_t> param
%type <modif_t> modifier_list
	/* operators */


	/* literal keyword tokens */

%token SEMICOLON HANDLE CONST CLASS FUN PRED
%token EXTR UDAF STATE SFUN


%%

result:	fcn_list	{Ext_fcns = $1;}
	;

fcn_list:	fcn_def	{$$ = new ext_fcn_list($1);}
	|	fcn_list fcn_def	{$$ = $1->append_ext_fcn_def($2);}
	;

fcn_def:	NAME opt_type_param_commalist FUN '[' modifier_list ']' NAME '(' opt_param_commalist ')' SEMICOLON  {
					$$=new ext_fcn_def($1,$2,$5,$7,$9); delete $5; delete $9; }
	|	NAME opt_type_param_commalist FUN NAME '(' opt_param_commalist ')' SEMICOLON  {
					$$=new ext_fcn_def($1,$2,NULL,$4,$6); delete $6; }
	|	PRED '[' modifier_list ']' NAME '[' opt_param_commalist ']' SEMICOLON  {
					$$=new ext_fcn_def($3,$5,$7); delete $3; delete $7; }
	|	PRED NAME '[' opt_param_commalist ']' SEMICOLON  {
					$$=new ext_fcn_def(NULL,$2,$4); delete $4; }
	|	NAME opt_type_param_commalist UDAF '[' modifier_list ']' NAME NAME '(' opt_param_commalist ')' SEMICOLON  {
					$$=new ext_fcn_def($1,$2,$5,$7,$8,$10); delete $5; delete $10; }
	|	NAME opt_type_param_commalist UDAF NAME NAME '(' opt_param_commalist ')' SEMICOLON  {
					$$=new ext_fcn_def($1,$2,NULL,$4,$5, $7); delete $7; }
	|	NAME opt_type_param_commalist EXTR '[' modifier_list ']' NAME NAME NAME '(' opt_param_commalist ')' SEMICOLON  {
					$$=new ext_fcn_def($1,$2,$5,$7,$8, $9, $11); delete $5; delete $11; }
	|	NAME opt_type_param_commalist EXTR NAME NAME NAME '(' opt_param_commalist ')' SEMICOLON  {
					$$=new ext_fcn_def($1,$2,NULL,$4,$5, $6, $8); delete $8; }
	|	NAME opt_type_param_commalist SFUN '[' modifier_list ']' NAME NAME '(' opt_param_commalist ')' SEMICOLON  {
					$$=ext_fcn_def::make_sfun_def($1,$2,$5,$7,$8,$10); delete $10; }
	|	NAME opt_type_param_commalist SFUN NAME NAME '(' opt_param_commalist ')' SEMICOLON  {
					$$=ext_fcn_def::make_sfun_def($1,$2,NULL,$4,$5,$7); delete $7; }
	|	NAME STATE NAME SEMICOLON  {
					$$=ext_fcn_def::make_state_def($1,$3); }

	;

opt_param_commalist:
		/* empty */    {$$ = NULL;}
	|	param_commalist {$$ = $1;}
	;
	
modifier_list:
		NAME	{$$ = new ext_fcn_modifier_list($1);}
	|	NAME NAME	{$$ = new ext_fcn_modifier_list($1, $2);}
	|	modifier_list ',' NAME	{$$ = $1->append($3);	}	
	|	modifier_list ',' NAME NAME	{$$ = $1->append($3, $4);	}	
	;
	
param_commalist:
		param	{$$ = new ext_fcn_param_list($1);}
	|	param_commalist ',' param {$$ = $1->append($3);}
	;

param:
		NAME opt_type_param_commalist HANDLE	{ $$ = new ext_fcn_param($1,$2,1,0,0);}
	|	NAME opt_type_param_commalist CONST	{ $$ = new ext_fcn_param($1,$2,0,1,0);}
	|	NAME opt_type_param_commalist CLASS	{ $$ = new ext_fcn_param($1,$2,0,0,1);}
	|	NAME opt_type_param_commalist HANDLE CLASS	{ $$ = new ext_fcn_param($1,$2,1,0,1);}
	|	NAME opt_type_param_commalist CONST CLASS	{ $$ = new ext_fcn_param($1,$2,0,1,1);}
	|	NAME opt_type_param_commalist
				{ $$ = new ext_fcn_param($1,$2,0,0,0);}
	;

opt_type_param_commalist:
		/* empty */    {$$ = NULL;}
	|	'(' type_param_commalist ')' {$$ = $2;}
	;

type_param_commalist:
		NAME	{$$ = new param_list($1);}
	|	NAME NAME	{$$ = new param_list($1,$2);}
	|	NAME STRING_TOKEN	{$$ = new param_list($1,$2);}
	|	NAME INTNUM	{$$ = new param_list($1,$2);}
	|	type_param_commalist ',' NAME {$$ = $1->append($3);}
	|	type_param_commalist ',' NAME NAME {$$ = $1->append($3,$4);}
	|	type_param_commalist ',' NAME STRING_TOKEN {$$ = $1->append($3,$4);}
	|	type_param_commalist ',' NAME INTNUM {$$ = $1->append($3,$4);}
	;



%%
 
