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
		bison --verbose --defines=ifq.tab.cc.h -p IfqParser  -o ifq.tab.cc ifq.y

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

#include "parse_fta.h"


extern fta_parse_t *ifq_parse_result;


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
	select_list_t *select_listval;
	table_exp_t *tblp;
	predicate_t *predp;
	literal_list_t *lit_l;
	tablevar_t *table;
	tablevar_list_t *tbl_list;
	colref_t *colref;
	colref_list_t *clist;
	var_defs_t *var_defs;
	var_pair_t *var_pair;
	gb_t *gb_val;
	gb_list_t *gb_list;

	query_list_t *q_list;


}

%token <strval> NAME
%token <strval> PRED
%token <strval> STRING_TOKEN
%token <strval> INTNUM
%token <strval> LONGINTNUM
%token <strval> APPROXNUM

/*			for FTA definition.	*/
%type <q_list> query_list
%type <tblp> gsql
%type <tblp> select_statement
%type <tblp> table_exp
%type <predp> where_clause
%type <predp> search_condition
%type <predp> predicate
%type <litval> literal
%type <scalarval> scalar_exp
%type <se_listval> scalar_exp_commalist





	/* operators */

%left OR
%left AND
%left NOT

	/* literal keyword tokens */
%token  SEMICOLON LEFTBRACE RIGHTBRACE
/*		For query definition	*/
%token BY AS
%token <strval> AGGR
%token FROM INNER_JOIN OUTER_JOIN LEFT_OUTER_JOIN RIGHT_OUTER_JOIN
%token GROUP HAVING IN
%token SELECT
%token WHERE
%token SUCH THAT

%token TRUE_V FALSE_V
%token TIMEVAL_L HEX_L LHEX_L IP_L
%token	MERGE SLACK

%token DEFINE_SEC PARAM_SEC

/*		For table definition	*/
%token RIGHTBRACE PROTOCOL TABLE STREAM FTA
%token OPERATOR OPERATOR_VIEW FIELDS SUBQUERIES SELECTION_PUSHDOWN


%%

/*			Union of possible results	*/
parse_result:	query_list	{
		ifq_parse_result->parse_tree_list = $1;
		ifq_parse_result->tables = NULL;
		ifq_parse_result->parse_type = QUERY_PARSE;
	}
	;



/*		Query definition.
		WARNING: there might be some relics.
*/

gsql:	NAME ':' select_statement {
				$$ = $3; $$->nmap["name"] = $1;
			}
	;

query_list:	gsql	{$$ = new query_list_t($1);}
	|	query_list SEMICOLON gsql	{$$ = $1->append($3);}
	;


select_statement:
		table_exp	{$$ = $1;}
	;


	/* query expressions */



table_exp:
		where_clause {$$=new table_exp_t(NULL,$1,NULL,NULL,NULL,NULL,NULL,NULL);}
	;



where_clause:
		search_condition	{$$ = $1;}
	;



	/* search conditions */

search_condition:
 		search_condition OR search_condition  {$$=new predicate_t("OR",$1,$3);}
	|	search_condition AND search_condition {$$=new predicate_t("AND",$1,$3);}
	|	NOT search_condition	{$$ = new predicate_t("NOT", $2 );}
	|	'(' search_condition ')' {$$ = $2;}
	|	predicate {$$ = $1;}
	;

predicate:
		PRED '[' scalar_exp_commalist ']' {$$ = new predicate_t($1, $3->get_se_list()); }
	;


	/* scalar expressions */

scalar_exp:
		literal	{ $$= scalarexp_t::make_param_reference($1->to_string().c_str());}
	|	NAME {$$ = scalarexp_t::make_param_reference($1);}
	;


scalar_exp_commalist:
		scalar_exp	{	$$= new se_list_t($1); }
	|	scalar_exp_commalist ',' scalar_exp	{ $$=$1->append($3); }
	;

literal:
		STRING_TOKEN	{$$ = new literal_t($1,LITERAL_STRING);}
	|	INTNUM		{$$ = new literal_t($1,LITERAL_INT);}
	|	LONGINTNUM		{$$ = new literal_t($1,LITERAL_LONGINT);}
	|	APPROXNUM	{$$ = new literal_t($1,LITERAL_FLOAT);}
	|	TRUE_V		{$$ = new literal_t("TRUE",LITERAL_BOOL);}
	|	FALSE_V		{$$ = new literal_t("FALSE",LITERAL_BOOL);}
	|	TIMEVAL_L STRING_TOKEN	{$$ = new literal_t($2,LITERAL_TIMEVAL);}
	|	HEX_L  STRING_TOKEN	{$$ = new literal_t($2,LITERAL_HEX);}
	|	LHEX_L  STRING_TOKEN	{$$ = new literal_t($2,LITERAL_LONGHEX);}
	|	IP_L  STRING_TOKEN	{$$ = new literal_t($2,LITERAL_IP);}
	;



%%

