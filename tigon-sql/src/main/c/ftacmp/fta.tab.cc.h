/* A Bison parser, made by GNU Bison 2.3.  */

/* Skeleton interface for Bison's Yacc-like parsers in C

   Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor,
   Boston, MA 02110-1301, USA.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     NAME = 258,
     STRING_TOKEN = 259,
     INTNUM = 260,
     LONGINTNUM = 261,
     APPROXNUM = 262,
     OR = 263,
     AND = 264,
     NOT = 265,
     COMPARISON = 266,
     SHIFT_OP = 267,
     UMINUS = 268,
     SEMICOLON = 269,
     LEFTBRACE = 270,
     RIGHTBRACE = 271,
     BY = 272,
     AS = 273,
     AGGR = 274,
     FROM = 275,
     INNER_JOIN = 276,
     FILTER_JOIN = 277,
     OUTER_JOIN = 278,
     LEFT_OUTER_JOIN = 279,
     RIGHT_OUTER_JOIN = 280,
     GROUP = 281,
     HAVING = 282,
     IN = 283,
     SELECT = 284,
     WHERE = 285,
     SUPERGROUP = 286,
     CLEANING_WHEN = 287,
     CLEANING_BY = 288,
     CLOSING_WHEN = 289,
     SUCH = 290,
     THAT = 291,
     CUBE = 292,
     ROLLUP = 293,
     GROUPING_SETS = 294,
     TRUE_V = 295,
     FALSE_V = 296,
     TIMEVAL_L = 297,
     HEX_L = 298,
     LHEX_L = 299,
     IP_L = 300,
     IPV6_L = 301,
     MERGE = 302,
     SLACK = 303,
     DEFINE_SEC = 304,
     PARAM_SEC = 305,
     PROTOCOL = 306,
     TABLE = 307,
     STREAM = 308,
     FTA = 309,
     UNPACK_FCNS = 310,
     OPERATOR = 311,
     OPERATOR_VIEW = 312,
     FIELDS = 313,
     SUBQUERIES = 314,
     SELECTION_PUSHDOWN = 315
   };
#endif
/* Tokens.  */
#define NAME 258
#define STRING_TOKEN 259
#define INTNUM 260
#define LONGINTNUM 261
#define APPROXNUM 262
#define OR 263
#define AND 264
#define NOT 265
#define COMPARISON 266
#define SHIFT_OP 267
#define UMINUS 268
#define SEMICOLON 269
#define LEFTBRACE 270
#define RIGHTBRACE 271
#define BY 272
#define AS 273
#define AGGR 274
#define FROM 275
#define INNER_JOIN 276
#define FILTER_JOIN 277
#define OUTER_JOIN 278
#define LEFT_OUTER_JOIN 279
#define RIGHT_OUTER_JOIN 280
#define GROUP 281
#define HAVING 282
#define IN 283
#define SELECT 284
#define WHERE 285
#define SUPERGROUP 286
#define CLEANING_WHEN 287
#define CLEANING_BY 288
#define CLOSING_WHEN 289
#define SUCH 290
#define THAT 291
#define CUBE 292
#define ROLLUP 293
#define GROUPING_SETS 294
#define TRUE_V 295
#define FALSE_V 296
#define TIMEVAL_L 297
#define HEX_L 298
#define LHEX_L 299
#define IP_L 300
#define IPV6_L 301
#define MERGE 302
#define SLACK 303
#define DEFINE_SEC 304
#define PARAM_SEC 305
#define PROTOCOL 306
#define TABLE 307
#define STREAM 308
#define FTA 309
#define UNPACK_FCNS 310
#define OPERATOR 311
#define OPERATOR_VIEW 312
#define FIELDS 313
#define SUBQUERIES 314
#define SELECTION_PUSHDOWN 315




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
#line 37 "fta.y"
{
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
/* Line 1529 of yacc.c.  */
#line 214 "fta.tab.cc.h"
	YYSTYPE;
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif

extern YYSTYPE FtaParserlval;

