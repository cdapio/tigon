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
   bison --verbose --defines=partn.tab.cc.h -p PartnParser  -o partn.tab.cc partn.y

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

#include "parse_partn.h"

extern partn_def_list_t *partn_parse_result;


#define YYDEBUG 1

%}


	/* symbolic tokens */

%union {
	int intval;
	double floatval;
	char *strval;
	int subtok;

/*			for FTA definition.	*/
	literal_t *litval;
	scalarexp_t *scalarval;
	se_list_t *se_listval;
	colref_t *colref;
	partn_def_list_t *pdl;
	partn_def_t *pd;
}

%token <strval> NAME
%token <strval> STRING_TOKEN
%token <strval> INTNUM
%token <strval> LONGINTNUM

/*			for FTA definition.	*/
%type <litval> literal
%type <colref> column_ref
%type <scalarval> scalar_exp
%type <se_listval> scalar_exp_commalist
%type <pdl> partn_def_list
%type <pd> partn_def

%left '|'
%left '&'
%left '+' '-'
%left '*' '/' 
%nonassoc UMINUS

	/* literal keyword tokens */
%token HEX_L LHEX_L IP_L


%%

/*			Union of possible results	*/
parse_result:	partn_def_list	{
		partn_parse_result = $1;
	}
	;


partn_def_list:
	partn_def	{$$ = new partn_def_list_t($1);}
	| partn_def_list partn_def	{$$=$1->append($2);}
	;

partn_def:
	NAME '[' NAME ']' ':' '(' scalar_exp_commalist ')'
		{$$ = new partn_def_t($1,$3,$7);}
	;

scalar_exp:
		scalar_exp '+' scalar_exp	{ $$=new scalarexp_t("+",$1,$3);}
	|	scalar_exp '-' scalar_exp	{ $$=new scalarexp_t("-",$1,$3);}
	|	scalar_exp '|' scalar_exp	{ $$=new scalarexp_t("|",$1,$3);}
	|	scalar_exp '*' scalar_exp	{ $$=new scalarexp_t("*",$1,$3);}
	|	scalar_exp '/' scalar_exp	{ $$=new scalarexp_t("/",$1,$3);}
	|	scalar_exp '&' scalar_exp	{ $$=new scalarexp_t("&",$1,$3);}
	|	'+' scalar_exp %prec UMINUS	{ $$ = new scalarexp_t("+",$2);}
	|	'-' scalar_exp %prec UMINUS	{ $$ = new scalarexp_t("-",$2);}
	|	literal	{ $$= new scalarexp_t($1);}
	|	column_ref { $$ = new scalarexp_t($1);}
	|	'(' scalar_exp ')'	{$$ = $2;}
	;

scalar_exp_commalist:
		scalar_exp	{	$$= new se_list_t($1); }
	|	scalar_exp_commalist ',' scalar_exp	{ $$=$1->append($3); }
	;

literal:
		INTNUM		{$$ = new literal_t($1,LITERAL_INT);}
	|	LONGINTNUM		{$$ = new literal_t($1,LITERAL_LONGINT);}
	|	HEX_L  STRING_TOKEN	{$$ = new literal_t($2,LITERAL_HEX);}
	|	LHEX_L  STRING_TOKEN	{$$ = new literal_t($2,LITERAL_LONGHEX);}
	|	IP_L  STRING_TOKEN	{$$ = new literal_t($2,LITERAL_IP);}
	;


column_ref:
		NAME	{$$ = new colref_t($1); }
	;


%%

