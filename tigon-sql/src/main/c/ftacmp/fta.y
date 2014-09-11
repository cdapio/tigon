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
   bison --verbose --defines=fta.tab.cc.h -p FtaParser  -o fta.tab.cc fta.y

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
#include "parse_schema.h"


extern fta_parse_t *fta_parse_result;
extern var_defs_t *fta_parse_defines;


#define YYDEBUG 1

%}


	/* symbolic tokens */

%union {
	int intval;
	double floatval;
	char *strval;
	int subtok;

	string_t *stringval;

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
	ifpref_t *ifpref;
	colref_list_t *clist;
	var_defs_t *var_defs;
	var_pair_t *var_pair;
	gb_t *gb_val;
	gb_list_t *gb_list;
	list_of_gb_list_t *list_of_gb_list;
	extended_gb_t *extended_gb;
	extended_gb_list_t *extended_gb_list;

	query_list_t *q_list;

/*		For table definition	*/
	field_entry *field_t;
	field_entry_list *field_list_t;
	table_def *table_def_t;
	table_list *table_list_schema;
	param_list *plist_t;
	name_vec  *namevec_t;
	subquery_spec *subq_spec_t;
	subqueryspec_list *subqueryspec_list_t;
	unpack_fcn	*ufcn;
	unpack_fcn_list *ufcnl;

}

%token <strval> NAME
%token <strval> STRING_TOKEN
%token <strval> INTNUM
%token <strval> LONGINTNUM
%token <strval> APPROXNUM

/*			for FTA definition.	*/
%type <q_list> query_list
%type <tblp> gsql
%type <tblp> select_statement
%type <tblp> merge_statement
%type <predp> opt_where_clause
%type <predp> opt_having_clause
%type <select_listval> selection
%type <tblp> table_exp
%type <tbl_list> from_clause
%type <tbl_list> table_ref_commalist
%type <table> table_ref
%type <stringval> qname
%type <predp> where_clause
%type <predp> having_clause
%type <predp> search_condition
%type <predp> predicate
%type <predp> comparison_predicate
%type <predp> in_predicate
%type <predp> opt_cleaning_when_clause
%type <predp> opt_cleaning_by_clause
%type <predp> opt_closing_when_clause
%type <litval> literal
%type <colref> column_ref
%type <colref> gb_ref
%type <ifpref> ifparam
%type <clist> column_ref_list
%type <clist> gb_ref_list
%type <table> table
%type <scalarval> scalar_exp
%type <se_listval> scalar_exp_commalist
%type <select_listval> select_commalist
%type <lit_l> literal_commalist
%type <extended_gb_list> opt_group_by_clause
%type <clist> opt_supergroup_clause
%type <gb_list> gb_commalist
%type <extended_gb> extended_gb
%type <extended_gb_list> extended_gb_commalist
%type <list_of_gb_list> list_of_gb_commalist
%type <gb_val> gb
/*			for PARAM, DEFINE block	*/
%type <var_defs> variable_def
%type <var_defs> params_def
%type <var_defs> variable_list
%type <var_pair> variable_assign

/*		For table definition	*/
%type <field_t> field
%type <field_list_t> field_list
%type <table_def_t> table_def
%type <table_list_schema> table_list
%type <plist_t> param_commalist
%type <plist_t> opt_param_commalist
%type <plist_t> singleparam_commalist
%type <plist_t> opt_singleparam_commalist
%type <plist_t> opt_singleparam_commalist_bkt
%type <namevec_t> namevec_commalist
%type <subq_spec_t> subq_spec
%type <subqueryspec_list_t> subqueryspec_list
%type <ufcn> unpack_func 
%type <ufcnl> unpack_func_list 

	/* operators */

%left OR
%left AND
%left NOT
%left <strval> COMPARISON /* = <> < > <= >= */
%left '|'
%left '&'
%left <strval> SHIFT_OP /* << >> */
%left '+' '-'
%left '*' '/' '%'
%nonassoc UMINUS

	/* literal keyword tokens */
%token  SEMICOLON LEFTBRACE RIGHTBRACE
/*		For query definition	*/
%token BY AS
%token <strval> AGGR
%token FROM INNER_JOIN FILTER_JOIN OUTER_JOIN LEFT_OUTER_JOIN RIGHT_OUTER_JOIN
%token GROUP HAVING IN
%token SELECT
%token WHERE SUPERGROUP CLEANING_WHEN CLEANING_BY CLOSING_WHEN
%token SUCH THAT
%token CUBE ROLLUP GROUPING_SETS

%token TRUE_V FALSE_V
%token TIMEVAL_L HEX_L LHEX_L IP_L IPV6_L
%token	MERGE SLACK

%token DEFINE_SEC PARAM_SEC

/*		For table definition	*/
%token PROTOCOL TABLE STREAM FTA UNPACK_FCNS
%token OPERATOR OPERATOR_VIEW FIELDS SUBQUERIES SELECTION_PUSHDOWN


%%

/*			Union of possible results	*/
parse_result:	query_list	{
		fta_parse_result->parse_tree_list = $1;
		fta_parse_result->tables = NULL;
		fta_parse_result->parse_type = QUERY_PARSE;
	}
	| table_list	{
		fta_parse_result->parse_tree_list = NULL;
		fta_parse_result->fta_parse_tree = NULL;
		fta_parse_result->tables = $1;
		fta_parse_result->parse_type = TABLE_PARSE;
	}
//				table_list should contain a single STREAM_SCHEMA table.
	| FTA LEFTBRACE table_list gsql RIGHTBRACE	{
		fta_parse_result->parse_tree_list = NULL;
		fta_parse_result->fta_parse_tree = $4;
		fta_parse_result->tables = $3;
		fta_parse_result->parse_type = STREAM_PARSE;
	}
	;



/*		Query definition.
		WARNING: there might be some relics.
*/

gsql:		variable_def params_def select_statement {
				$3->add_nmap($1);			// Memory leak : plug it.
				$3->add_param_list($2);		// Memory leak : plug it.
				$$ = $3;
			}
		|	params_def variable_def select_statement {
				$3->add_nmap($2);			// Memory leak : plug it.
				$3->add_param_list($1);		// Memory leak : plug it.
				$$ = $3;
			}
		|	params_def select_statement {
				$2->add_nmap(NULL);			// Memory leak : plug it.
				$2->add_param_list($1);		// Memory leak : plug it.
				$$ = $2;
			}
		|	variable_def select_statement {
				$2->add_nmap($1);			// Memory leak : plug it.
				$$ = $2;
			}
		|	select_statement {
				$$ = $1;
			}
		|	variable_def params_def merge_statement{
				$3->add_nmap($1);			// Memory leak : plug it.
				$3->add_param_list($2);		// Memory leak : plug it.
				$$ = $3;
			}
		|	params_def variable_def merge_statement{
				$3->add_nmap($2);			// Memory leak : plug it.
				$3->add_param_list($1);		// Memory leak : plug it.
				$$ = $3;
			}
		|	params_def merge_statement{
				$2->add_param_list($1);		// Memory leak : plug it.
				$$ = $2;
			}
		|	variable_def merge_statement{
				$2->add_nmap($1);			// Memory leak : plug it.
				$$ = $2;
			}
		|	merge_statement{
				$$ = $1;
			}
	;

query_list:	gsql	{$$ = new query_list_t($1);}
	|	query_list SEMICOLON gsql	{$$ = $1->append($3);}
	;

params_def:
	 PARAM_SEC LEFTBRACE variable_list RIGHTBRACE	{$$=$3;}
	| PARAM_SEC LEFTBRACE  RIGHTBRACE	{$$=NULL;}
	;

variable_def:
	 DEFINE_SEC LEFTBRACE variable_list RIGHTBRACE {$$=$3;fta_parse_defines=$3;}
	| DEFINE_SEC LEFTBRACE RIGHTBRACE	{$$=NULL;fta_parse_defines = NULL;}	
	;

variable_list:
	variable_assign			{$$ = new var_defs_t($1);}
	| variable_list variable_assign	{$$ = $1->add_var_pair($2);}
	;

variable_assign:
	NAME STRING_TOKEN SEMICOLON	{$$ = new var_pair_t($1,$2);}
	| NAME NAME SEMICOLON	{$$ = new var_pair_t($1,$2);}
	;

select_statement:
		SELECT  selection table_exp	{$$ = $3->add_selection($2);}
	;

merge_statement:
		MERGE column_ref_list
		from_clause		{$$ = new table_exp_t($2,$3);}
	|	MERGE column_ref_list SLACK  scalar_exp
		from_clause		{$$ = new table_exp_t($2,$4,$5);}
	;

	/* query expressions */


selection:
		select_commalist	{ $$ = $1;}
	;

table_exp:
		from_clause
		opt_where_clause
		opt_group_by_clause
		opt_supergroup_clause
		opt_having_clause	
		opt_cleaning_when_clause	
		opt_cleaning_by_clause	
		opt_closing_when_clause	
			{$$=new table_exp_t($1,$2,$3,$4,$5,$6,$7, $8);}
	;

from_clause:
		FROM table_ref_commalist	{$$ = $2; $$->set_properties(-1);}
	|	INNER_JOIN FROM table_ref_commalist	{$$ = $3; $$->set_properties(INNER_JOIN_PROPERTY);}
	|	OUTER_JOIN FROM table_ref_commalist	{$$ = $3; $$->set_properties(OUTER_JOIN_PROPERTY);}
	|	RIGHT_OUTER_JOIN FROM table_ref_commalist	{$$ = $3; $$->set_properties(RIGHT_OUTER_JOIN_PROPERTY);}
	|	LEFT_OUTER_JOIN FROM table_ref_commalist	{$$ = $3; $$->set_properties(LEFT_OUTER_JOIN_PROPERTY);}
	|	FILTER_JOIN '(' column_ref ',' INTNUM ')' FROM table_ref_commalist	{$$ = $8; $$->set_properties(FILTER_JOIN_PROPERTY); $$->set_colref($3); $$->set_temporal_range($5);}
	;

table_ref_commalist:
		table_ref	{$$ = new tablevar_list_t($1);}
	|	table_ref_commalist ',' table_ref	{$$= $1->append_table($3);}
	;

table_ref:
		table { $$ = $1;}
	|	table NAME { $$= $1->set_range_var($2);}
	;

table:
		qname	{$$ = new tablevar_t($1->c_str());}
	|	NAME '.' qname	{$$ = new tablevar_t($1,$3->c_str(),0);}
	|	NAME '.' NAME '.' qname	{$$ = new tablevar_t($1,$3,$5->c_str());}
	|	STRING_TOKEN '.' NAME '.' qname	{$$ = new tablevar_t($1,$3,$5->c_str());}
	|	'[' NAME ']' '.' qname	{$$ = new tablevar_t($2,$5->c_str(),1);}
	;

qname:
		NAME    {$$ = new string_t($1);}
	|	qname '/' NAME  {$$ = $$->append("/",$3);}
	;


opt_where_clause:
		/* empty */	{$$=NULL;}
	|	where_clause	{$$=$1;}
	;

where_clause:
		WHERE search_condition	{$$ = $2;}
	;

opt_cleaning_when_clause:
		/* empty */	{$$=NULL;}
	|	CLEANING_WHEN search_condition {$$=$2; }
	;

opt_cleaning_by_clause:
		/* empty */	{$$=NULL;}
	|	CLEANING_BY search_condition {$$=$2; }
	;

opt_closing_when_clause:
		/* empty */	{$$=NULL;}
	|	CLOSING_WHEN search_condition {$$=$2; }
	;

opt_having_clause:
		/* empty */	{$$=NULL;}
	|	having_clause	{$$=$1;}
	;

having_clause:
		HAVING search_condition	{$$ = $2;}
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
		comparison_predicate { $$=$1;}
	|	in_predicate	{ $$ = $1;}
	|	NAME '[' ']' {$$ = predicate_t::make_paramless_fcn_predicate($1); }	
	|	NAME '[' scalar_exp_commalist ']' {$$ = new predicate_t($1, $3->get_se_list()); }
	;

comparison_predicate:
		scalar_exp COMPARISON scalar_exp	{$$ = new predicate_t($1,$2,$3);}
	;

in_predicate:
		scalar_exp IN '[' literal_commalist ']'	{ $$ = new predicate_t($1,$4); }
	;

literal_commalist:
		literal	{$$ = new literal_list_t($1);}
	|	literal_commalist ',' literal	{$$ = $1->append_literal($3);}
	;

	/* scalar expressions */

scalar_exp:
		scalar_exp '+' scalar_exp	{ $$=new scalarexp_t("+",$1,$3);}
	|	scalar_exp '-' scalar_exp	{ $$=new scalarexp_t("-",$1,$3);}
	|	scalar_exp '|' scalar_exp	{ $$=new scalarexp_t("|",$1,$3);}
	|	scalar_exp '*' scalar_exp	{ $$=new scalarexp_t("*",$1,$3);}
	|	scalar_exp '/' scalar_exp	{ $$=new scalarexp_t("/",$1,$3);}
	|	scalar_exp '&' scalar_exp	{ $$=new scalarexp_t("&",$1,$3);}
	|	scalar_exp '%' scalar_exp	{ $$=new scalarexp_t("%",$1,$3);}
	|	scalar_exp SHIFT_OP scalar_exp	{ $$=new scalarexp_t($2,$1,$3);}
	|	'+' scalar_exp %prec UMINUS	{ $$ = new scalarexp_t("+",$2);}
	|	'-' scalar_exp %prec UMINUS	{ $$ = new scalarexp_t("-",$2);}
	|	'!' scalar_exp %prec UMINUS	{ $$ = new scalarexp_t("!",$2);}
	|	'~' scalar_exp %prec UMINUS	{ $$ = new scalarexp_t("~",$2);}
	|	literal	{ $$= new scalarexp_t($1);}
	|	'$' NAME {$$ = scalarexp_t::make_param_reference($2);}
	|	ifparam {$$ = scalarexp_t::make_iface_param_reference($1);}
	|	column_ref { $$ = new scalarexp_t($1);}
	|	'(' scalar_exp ')'	{$$ = $2;}
	|	AGGR '(' '*' ')'	{ $$ = scalarexp_t::make_star_aggr($1); }
	|	AGGR '(' scalar_exp ')'	{ $$ = scalarexp_t::make_se_aggr($1,$3); }
	|	NAME '(' scalar_exp_commalist ')' {$$ = new scalarexp_t($1, $3->get_se_list()); }
	|	NAME '(' ')' {$$ = scalarexp_t::make_paramless_fcn($1); }
	|	AGGR '$' '(' '*' ')'	{ $$ = scalarexp_t::make_star_aggr($1); $$->set_superaggr(true); }
	|	AGGR '$' '(' scalar_exp ')'	{ $$ = scalarexp_t::make_se_aggr($1,$4); $$->set_superaggr(true); }
	|	NAME '$' '(' scalar_exp_commalist ')' {$$ = new scalarexp_t($1, $4->get_se_list()); $$->set_superaggr(true); }
	|	NAME '$' '(' ')' {$$ = scalarexp_t::make_paramless_fcn($1); $$->set_superaggr(true); }
	;


select_commalist:
		scalar_exp	{	$$= new select_list_t($1); }
	|	scalar_exp AS NAME	{	$$= new select_list_t($1,$3); }
	|	select_commalist ',' scalar_exp	{ $$=$1->append($3); }
	|	select_commalist ',' scalar_exp AS NAME	{ $$=$1->append($3,$5); }
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
	|	IPV6_L  STRING_TOKEN	{$$ = new literal_t($2,LITERAL_IPV6);}
	|	'#' NAME	{$$ = literal_t::make_define_literal($2,fta_parse_defines);}
	;



opt_group_by_clause:
		/* empty */    {$$ = NULL;}
	|	GROUP BY extended_gb_commalist	{$$ = $3;}
	;

opt_supergroup_clause:
		/* empty */    {$$ = NULL;}
	|	SUPERGROUP gb_ref_list	{$$ = $2;}
	;

list_of_gb_commalist:
		'(' gb_commalist ')' { $$ = new list_of_gb_list_t($2);}
	| 	list_of_gb_commalist ',' '(' gb_commalist ')' {$$ = $1->append($4);}
	;

extended_gb:
		gb	{$$ = extended_gb_t::create_from_gb($1);}
	|	ROLLUP '(' gb_commalist ')' {$$ = extended_gb_t::extended_create_from_rollup($3);}
	|	CUBE '(' gb_commalist ')' {$$ = extended_gb_t::extended_create_from_cube($3);}
	|	GROUPING_SETS '(' list_of_gb_commalist ')' {$$ = extended_gb_t::extended_create_from_gsets($3);}
	;

extended_gb_commalist:
		extended_gb { $$ = new extended_gb_list_t($1);}
	|	extended_gb_commalist ',' extended_gb { $$=$1->append($3);}
    ;

gb_commalist:
		gb { $$ = new gb_list_t($1);}
	|	gb_commalist ',' gb { $$=$1->append($3);}
    ;

gb:
		NAME	{$$ = new gb_t($1); }
	|	NAME '.' NAME	{$$ = new gb_t($1,$3); }
	|	NAME '.' NAME '.' NAME	{$$ = new gb_t($1,$3,$5); }
	|	scalar_exp AS NAME {$$ = new gb_t($1,$3); }
	;

	/* miscellaneous */


/*
		I do not currently use 3 part column refs
*/

ifparam:
		'@' NAME	{$$ = new ifpref_t($2);}
	|	NAME '.' '@' NAME	{$$ = new ifpref_t($1, $4);}
	;

column_ref:
		NAME	{$$ = new colref_t($1); }
	|	NAME '.' NAME	{$$ = new colref_t($1,$3); }
	|	NAME '.' NAME '.' NAME	{$$ = new colref_t($1,$3,$5); }
	;

column_ref_list:
		column_ref ':' column_ref	{$$=new colref_list_t($1); $$->append($3);}
	|	column_ref_list ':' column_ref	{$$ = $1->append($3);}
	;

gb_ref:
	NAME	{$$ = new colref_t($1); }
	;

gb_ref_list:
		gb_ref ',' gb_ref	{$$=new colref_list_t($1); $$->append($3);}
	|	gb_ref_list ',' gb_ref	{$$ = $1->append($3);}
	;

		/* data types */


	/* the various things you can name */





/*			Table definition section		*/


table_list:	table_def	{$$ = new table_list($1);}
	|	table_list table_def	{$$ = $1->append_table($2);}
	;

table_def:	PROTOCOL NAME  opt_singleparam_commalist opt_singleparam_commalist_bkt LEFTBRACE field_list RIGHTBRACE {
					$$=new table_def($2,$3,$4, $6, PROTOCOL_SCHEMA); delete $6;}
	|		STREAM qname  opt_singleparam_commalist LEFTBRACE field_list RIGHTBRACE {
					$$=new table_def($2->c_str(),$3,NULL,$5, STREAM_SCHEMA); delete $5;}
	|		OPERATOR_VIEW NAME LEFTBRACE OPERATOR opt_param_commalist FIELDS LEFTBRACE field_list RIGHTBRACE SUBQUERIES LEFTBRACE subqueryspec_list RIGHTBRACE SELECTION_PUSHDOWN opt_param_commalist RIGHTBRACE {
				$$ = new table_def($2, $5, $8, $12, $15); }
	|		UNPACK_FCNS LEFTBRACE unpack_func_list RIGHTBRACE { $$ = new table_def($3); }
	;

unpack_func_list:
	unpack_func	{$$ = new unpack_fcn_list($1);}
	|	unpack_func_list SEMICOLON unpack_func	{$$ = $1 -> append($3);}
	;

unpack_func:
	NAME NAME INTNUM {$$ = new unpack_fcn($1,$2,$3);}
	;

subqueryspec_list:
	subq_spec	{$$ = new subqueryspec_list($1);}
	|	subqueryspec_list SEMICOLON subq_spec {$$ = $1->append($3);}
	;

subq_spec:
	NAME '(' namevec_commalist ')' {$$=new subquery_spec($1, $3); delete $3;}
	;

field_list:	field 	{$$ = new field_entry_list($1);}
	|	field_list field	{$$ = $1->append_field($2);}
	;

field:
		NAME NAME NAME opt_param_commalist opt_singleparam_commalist_bkt SEMICOLON {$$ = new field_entry($1,$2,$3,$4,$5);}
	|	NAME NAME opt_param_commalist SEMICOLON {$$ = new field_entry($1,$2,"",$3,NULL);}
	;

opt_param_commalist:
		/* empty */    {$$ = NULL;}
	|	'(' param_commalist ')' {$$ = $2;}
	;

param_commalist:
		NAME	{$$ = new param_list($1);}
	|	NAME NAME	{$$ = new param_list($1,$2);}
	|	NAME STRING_TOKEN	{$$ = new param_list($1,$2);}
	|	NAME INTNUM	{$$ = new param_list($1,$2);}
	|	param_commalist ',' NAME {$$ = $1->append($3);}
	|	param_commalist ',' NAME NAME {$$ = $1->append($3,$4);}
	|	param_commalist ',' NAME STRING_TOKEN {$$ = $1->append($3,$4);}
	|	param_commalist ',' NAME INTNUM {$$ = $1->append($3,$4);}
	;

opt_singleparam_commalist_bkt:
		/* empty */    {$$ = NULL;}
	|	'[' singleparam_commalist ']' {$$ = $2;}
	;

opt_singleparam_commalist:
		/* empty */    {$$ = NULL;}
	|	'(' singleparam_commalist ')' {$$ = $2;}
	;

singleparam_commalist:
		NAME	{$$ = new param_list($1);}
	|	singleparam_commalist ',' NAME {$$ = $1->append($3);}
	;

namevec_commalist:
		NAME NAME opt_param_commalist	{$$ = new name_vec($1,$2,$3);}
	|	namevec_commalist ',' NAME NAME opt_param_commalist {$$ = $1->append($3,$4, $5);}
	;

%%

